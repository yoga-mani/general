import logging
import os
import itertools

from pyspark import RDD
from pyspark.sql.types import StringType, LongType, DecimalType, DateType, FloatType, StructType, StructField, \
    IntegerType
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.functions import input_file_name, col, length, trim, lit, regexp_extract, coalesce, split
import datetime
from decimal import Decimal, InvalidOperation
from awsglue.utils import getResolvedOptions
import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import re
from dataclasses import dataclass, asdict
from typing import List
from urllib.parse import urlparse
from pyspark.sql.functions import current_timestamp

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3 = boto3.client('s3')

# Set Hadoop Configuration to avoid creating success folder
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")


# Initialize SparkSession
def get_spark_session():
    try:
        return SparkSession.builder \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .getOrCreate()
    except Exception as e:
        return SparkSession.builder \
            .appName("FixedWidthProcessing") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .getOrCreate()


logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FixedWithSpec:
    def __init__(self, metadata):
        self.cols = [col_dict['column_name'] for col_dict in metadata]
        self.col_lengths = [col_dict['column_length'] for col_dict in metadata]
        self.datatypes = [col_dict['column_datatype'] for col_dict in metadata]
        self.abs_offsets = [0] + list(itertools.accumulate(self.col_lengths))

    @staticmethod
    def get_spark_datatype(datatype):
        mapping = {
            'string': StringType(),
            'numeric': LongType(),
            'amount': DecimalType(38, 18),
            'snumeric': LongType(),
            'date': DateType(),
            'rate': FloatType()
        }
        cleaned_datatype = str(datatype).strip()  # Strip any leading/trailing whitespace
        return mapping.get(cleaned_datatype.lower(), StringType())


def convert_fixed_width_row(row, spec: FixedWithSpec, file_name, key_columns, table_name):
    transformed_row = []
    dataops_etl_dq_er = []
    key_column_values = []

    for start, end, datatype, column_name in zip(spec.abs_offsets, spec.abs_offsets[1:], spec.datatypes, spec.cols):
        col_value = row[start:end].strip()
        if column_name == "FILLERS":
            continue

        if datatype.lower() == 'date':
            if col_value == '00000000' or col_value == '':
                col_value = datetime.datetime.strptime('19010101', '%Y%m%d').date()
            else:
                try:
                    parsed_date = datetime.datetime.strptime(col_value, '%Y%m%d').date()
                    if parsed_date < datetime.date(1582, 10, 15):
                        col_value = datetime.datetime.strptime('19010101', '%Y%m%d').date()
                    else:
                        col_value = parsed_date

                except ValueError:
                    ## TODO : default value assigned only for testing purpose
                    error_desc = "Failed to parse date value as it doesn't have date value in a format YYYYMMDD"
                    dataops_etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = datetime.datetime.strptime('19010101', '%Y%m%d').date()

        elif datatype.lower() in ['numeric', 'amount', 'snumeric']:
            col_value = col_value.replace('+', '').strip()
            col_value = col_value.lstrip('0')
            if '.' in col_value:
                if col_value.index('.') == 0:
                    col_value = '0' + col_value
                if col_value.index('.') == len(col_value) - 1:
                    col_value += "00"
            if all(c == '0' for c in col_value):
                col_value = '0'

            if datatype.lower() in ['numeric', 'snumeric']:
                try:
                    col_value = int(col_value)
                except ValueError:
                    error_desc = "Failed to cast the column to long type from source data type - NUMERIC/SNUMERIC"
                    dataops_etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = 0
            if datatype.lower() == 'amount':
                try:
                    col_value = Decimal(col_value)

                except InvalidOperation:

                    ## TODO : default value assigned only for testing purpose
                    error_desc = "Failed to cast the column to decimal from source data type - AMOUNT"
                    dataops_etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = Decimal('0')

        elif datatype.lower() == 'rate':

            if "%" in col_value:
                try:
                    col_value = float(col_value.strip('%')) / 100.0
                except ValueError:
                    error_desc = ("Failed to cast the column to float from source data type - "
                                  "rate where source field is expected to be populated with %")
                    dataops_etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = 0.0
            else:  # If '%' is not present, treat as regular numeric value
                try:
                    col_value = float(col_value)
                except ValueError:
                    error_desc = "Failed to cast the column to float from source data type - rate"
                    dataops_etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = 0.0

        # Check if column is a key column and record its value
        if column_name in key_columns:
            key_column_values.append(f"{column_name}={col_value}")

        transformed_row.append(col_value)

    transformed_row.append(f"{file_name}")
    dataops_vision_region = "AU" if ".AU_" in file_name else "NZ"
    transformed_row.append(dataops_vision_region)
    # Append concatenated key column values
    key_column_str = "|".join(key_column_values)
    extracted_file_name = file_name.split('/')[-1]
    # Append key_column_str to each error message with ":" as the separator
    if len(dataops_etl_dq_er) > 0:
        dataops_etl_dq_er = [f"{extracted_file_name}:{key_column_str}:{error}" for error in dataops_etl_dq_er]
        return None, tuple(dataops_etl_dq_er)
    else:
        return tuple(transformed_row), None


@dataclass
class ColumnMetadata:
    column_name: str
    column_length: int
    column_datatype: str
    column_key: str


def get_column_metadata(metadata_file_path):
    spark = get_spark_session()
    logger.info("Reading metadata from: %s", metadata_file_path)
    metadata_df = spark.read.option("header", "false").csv(metadata_file_path)

    # Create a list to store dictionaries of ColumnMetadata
    metadata_dicts: List[dict] = []

    for row in metadata_df.collect():
        # Create and add the ColumnMetadata dictionary to the list
        column_metadata = ColumnMetadata(row[1].replace("-", "_"), int(row[3]), row[2], row[4])
        metadata_dicts.append(asdict(column_metadata))

    logger.info("metadata file is read")

    # Filter out the key column names
    key_column_names = [metadata['column_name'] for metadata in metadata_dicts if metadata['column_key'] == 'Y']

    # Calculate total length of all columns
    total_length = sum(metadata['column_length'] for metadata in metadata_dicts)
    logger.info(f"total record length: {total_length}")

    return metadata_dicts, key_column_names, total_length


@dataclass
class ProcessedData:
    transformed_rows: RDD
    invalid_width_rows_df: DataFrame
    dq_issues_rows: RDD
    stats_clean_record_file_level_df: DataFrame


def process_fixed_width_file(df, metadata, valid_rec_length, key_column_names, table_name):
    spec = FixedWithSpec(metadata)
    good_data_df = df.filter(length(col("value")) == valid_rec_length)
    invalid_width_rows_df = df.filter(length(col("value")) != valid_rec_length).select("value")
    successful_load_count = good_data_df.count()
    # Collecting file_name, count for each file after sorting based on the file name
    stats_clean_record_file_level_df = good_data_df.groupBy('dataops_file_name_path') \
        .count() \
        .orderBy('dataops_file_name_path')

    # logger.warning("good records count : " + successful_load_count)
    logger.warning(f"good records count : {str(successful_load_count)}")
    logger.warning(f"bad records count : {str(invalid_width_rows_df.count())}")

    transformed_data = good_data_df.rdd.map(
        lambda row: convert_fixed_width_row(
            row['value'],
            spec,
            row['dataops_file_name_path'],
            key_column_names,
            table_name
        )
    )
    # Separate the transformed rows and error records
    processed_good_data = transformed_data.filter(
        lambda x: x is not None and x[1] is None
    ).map(lambda x: x[0])

    data_with_dq_issues = transformed_data.flatMap(
        lambda x: x[1] if x is not None and x[1] is not None else [])

    # return processed_good_data, data_with_dq_issues, invalid_width_rows_df
    return ProcessedData(
        transformed_rows=processed_good_data,
        invalid_width_rows_df=invalid_width_rows_df,
        dq_issues_rows=data_with_dq_issues,
        stats_clean_record_file_level_df=stats_clean_record_file_level_df
    )


def create_dataframe_with_schema(processed_data, metadata):
    logger.info("Creating DataFrame with specified schema.")

    # Filter out the columns and datatypes, excluding "FILLERS"
    filtered_column_names = [col_dict['column_name'] for col_dict in metadata if col_dict['column_name'] != "FILLERS"]
    filtered_column_datatypes = [col_dict['column_datatype'] for col_dict in metadata if
                                 col_dict['column_name'] != "FILLERS"]

    # Construct the schema based on the filtered names and datatypes
    schema = StructType([
                            StructField(name, FixedWithSpec.get_spark_datatype(datatype), True)
                            for name, datatype in zip(filtered_column_names, filtered_column_datatypes)
                        ] + [
                            StructField("dataops_file_name_path", StringType(), True),
                            StructField("dataops_vision_region", StringType(), True)
                        ])

    # Convert RDD to DataFrame using the defined schema
    data_df = processed_data.toDF(schema=schema)
    return data_df


exception_schema_fields = [
    StructField("file_name", StringType(), True),
    StructField("row_business_key", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("column_value", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("row_fixed_flag", StringType(), True),
    StructField("row_fixed_timestamp", StringType(), True)
]


def create_rejected_data_df(rejected_data_rdd, schema_fields):
    # Split the string in each RDD tuple into separate columns and convert to Row
    def split_row(line):
        parts = line.split(":")
        # Ensure the number of parts matches the number of schema fields
        if len(parts) != len(schema_fields):
            raise ValueError(f"Data row has {len(parts)} parts, but schema expects {len(schema_fields)} fields.")
        return Row(*parts)

    # Convert each line into a Row object with appropriate conversions
    rejected_rows = rejected_data_rdd.map(split_row)

    # Define the schema for the DataFrame
    schema = StructType(schema_fields)

    # Create the DataFrame from the RDD
    rejected_data_df = rejected_rows.toDF(schema=schema)
    return rejected_data_df


def marker_file_creation(output_file_path, table_name):
    # After writing the Parquet file, write the marker file in the same directory.
    marker_file_name = ""

    # Extract the bucket and the path from the output_file_path.
    path_parts = output_file_path.replace("s3://", "").split("/", 1)
    bucket = path_parts[0]
    parquet_file_dir = path_parts[1] if len(path_parts) > 1 else ""

    # If parquet_file_dir is not an empty string, we process it
    if parquet_file_dir:
        # Remove trailing slash (if any) and split to get the individual directory names
        if not parquet_file_dir.endswith('/'):
            parquet_file_dir = f"{parquet_file_dir}/"

        # The last directory name; this is what you're interested in
        table_name = str(table_name).upper()
        marker_file_name = f"{table_name}_LANDED"
        # Here, we're placing it in the same directory as the Parquet files
        marker_file_s3_path = f"{parquet_file_dir}{marker_file_name}"

    else:
        marker_file_s3_path = marker_file_name

    logger.info("Writing the marker file to S3.")
    s3.put_object(Bucket=bucket, Key=marker_file_s3_path, Body=b'')

    logger.info(f"Marker file '{marker_file_name}' written to: s3://{bucket}/{marker_file_s3_path}")


def list_s3_files(raw_s3_bucket_name, raw_s3_metadata_prefix, execution_dag_date, file_prefix):
    logger.info("latest metadata file to be found from the mata s3 uri ")
    file_prefix = str(file_prefix).replace(".", "")
    s3_metadata_objs = s3.list_objects_v2(Bucket=raw_s3_bucket_name, Prefix=raw_s3_metadata_prefix)
    s3_metadata_files = [f's3://{raw_s3_bucket_name}/{obj["Key"]}' for obj in s3_metadata_objs.get('Contents', [])]

    logger.info("scan through the file names and find the file matching with the file prefix pattern")
    s3_metadata_file = [s3_uri for s3_uri in s3_metadata_files if s3_uri.split('/')[-1].startswith(file_prefix)]
    execution_date = datetime.datetime.strptime(execution_dag_date, '%Y%m%d')
    date_pattern = r'\d{8}'
    latest_metadata_s3_uri = None
    # Initialize variables
    matching_dates = []  # List to store matching dates

    # Loop through the list of matching files
    for s3_uri in s3_metadata_file:

        # Extract date part from the file name using regular expression
        match = re.search(date_pattern, s3_uri)

        if match:

            # Get the date part as a string
            date_str = match.group(0)

            # Convert the date string to a datetime object for comparison
            try:
                file_date = datetime.datetime.strptime(date_str, '%Y%m%d')

                # Check if the file date is less than or equal to execution_date
                if file_date <= execution_date:
                    matching_dates.append(file_date)

            except ValueError:
                continue

    # Check if there are matching dates
    if matching_dates:
        # Find the maximum date among the matching dates
        max_matching_date = max(matching_dates)

        # Find the file with the maximum date
        latest_metadata_s3_uri = [s3_uri for s3_uri in s3_metadata_file if s3_uri.split('/')[-1].startswith(file_prefix)
                                  and datetime.datetime.strptime(
            re.search(date_pattern, s3_uri).group(0), '%Y%m%d') == max_matching_date][0]
    logger.info("latest_metadata_s3_uri", latest_metadata_s3_uri)
    return latest_metadata_s3_uri


def list_s3_data_files_with_prefix(bucket_name, prefix, file_prefix):
    """
    List files in an S3 bucket with a given prefix and file prefix.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - prefix (str): The prefix (or directory) in the S3 bucket where the files are located.
    - file_prefix (str): The prefix of the filenames to filter.

    Returns:
    Tuple(List[str], List[str]): A tuple containing two lists - S3 URIs of files, and file names.
    """
    s3_data_objs = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    s3_data_files_all = [f's3://{bucket_name}/{obj["Key"]}' for obj in s3_data_objs.get('Contents', [])]
    s3_data_files = [s3_uri for s3_uri in s3_data_files_all if s3_uri.split('/')[-1].startswith(file_prefix)]
    s3_data_file_names = [s3_uri.split('/')[-1] for s3_uri in s3_data_files_all if
                          s3_uri.split('/')[-1].startswith(file_prefix)]

    return s3_data_files, s3_data_file_names


def split_s3_uri(s3_uri):
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI format. Must start with 's3://'.")
    parts = s3_uri[5:].split('/', 1)
    bucket_name = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket_name, prefix



def rename_csv_files_in_s3(s3_uri, base_filename, destination_prefix):
    bucket_name, source_prefix = split_s3_uri(s3_uri)
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    filename_extn = 'CSV'
    if 'Contents' in response:
        for obj in response['Contents']:
            source_key = obj['Key']
            logger.info(f"check srce key: {source_key}")
            # Check for '_summary_' in base_filename
            new_filename = f"{base_filename}.{filename_extn}"
            new_key = os.path.join(destination_prefix,str(new_filename).upper())
            logger.info(f"rename source key to new key - {source_key} - {new_key}")

            s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_key}, Key=new_key)
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
            logger.info(f"File renaming completed in the new path - {new_key}")

def s3_path_exists(s3_path):
    # Parse the S3 path to get bucket and key
    parsed_url = urlparse(s3_path, allow_fragments=False)
    if parsed_url.scheme != 's3':
        raise ValueError("URL scheme must be s3")

    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')

    # Try to get the object metadata
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except boto3.exceptions.botocore.exceptions.ClientError as e:
        # If a ClientError is raised, check if it's because the object does not exist
        # The code 404 indicates that the object does not exist.
        if int(e.response['Error']['Code']) == 404:
            return False
        else:
            # Reraise the exception if it was due to a different error
            raise

def process_source_marker_file(marker_file_path,metric_name):
    # Check if the S3 path exists
    if not s3_path_exists(marker_file_path):
        logger.info(f"S3 path does not exist:{marker_file_path}")
        return []
    # Load the text file into an RDD and zip it with an index
    rdd_with_index = spark.sparkContext.textFile(marker_file_path).zipWithIndex()

    # Convert RDD to DataFrame with columns switched to [line_number, line_content]
    source_marker_df = rdd_with_index.map(lambda x: (x[1], x[0])).toDF(["line_number", "line_content"])
    source_marker_content = source_marker_df.collect()
    logger.info(f"source marker count --> {source_marker_content}")

    # Extract numeric value from line_content and convert to integer
    source_marker_df = source_marker_df.withColumn(metric_name, regexp_extract('line_content', ':\s*(\d+)', 1).cast('integer'))

    # Order by line_number
    source_marker_df = source_marker_df.orderBy("line_number")

    # Convert DataFrame to a list of tuples for easier iteration
    source_counts = [(row['line_number'], row[metric_name]) for row in source_marker_df.collect()]
    return source_counts

def generate_summary_file(processed_file_name, order_date,dq_issue_distinct_count_per_file_df,
                          csv_output_path, parquet_output_path, number_of_source_files,
                          stats_clean_record_file_level_df, marker_file_path,file_names_in_decrypt,
                          periodic_marker_s3_key):
    # Calculate the source_record_count
    order_date_dtype = datetime.datetime.strptime(order_date, '%Y%m%d')
    order_date_formatted = order_date_dtype.strftime('%Y-%m-%d')
    # Convert source_marker_df to a list for easier iteration
    source_counts = process_source_marker_file(marker_file_path,"count")
    source_weekly_counts = process_source_marker_file(periodic_marker_s3_key,"source_distinct_count")

    job_run_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Initialize the summary_data list
    summary_data = []

    stats_clean_record_file_level_df = stats_clean_record_file_level_df.withColumn(
        "extracted_file_name",
        regexp_extract("dataops_file_name_path", ".*/([^/]+)$", 1)
    )
    # it contains only list of files from s3 decrypt location
    file_names_df = spark.createDataFrame([(name,) for name in file_names_in_decrypt], ["file_name_s3"])
    # Join clean record df with dq stats df. so that clean and dq issue stats for each file can be stored in one df
    joined_df = stats_clean_record_file_level_df.join(
        dq_issue_distinct_count_per_file_df.withColumnRenamed("count", "dq_count"),
        stats_clean_record_file_level_df["extracted_file_name"] == dq_issue_distinct_count_per_file_df["file_name"],
        "left"
    ).withColumn("dq_count", coalesce(col("dq_count"), lit(0)))
    # Joining above clean/dq df with file_names_df to ensure all the files count included.
    final_joined_df = file_names_df.join(
        joined_df,
        joined_df["extracted_file_name"] == file_names_df["file_name_s3"],
        "left"
    )

    # select and handle nulls
    stats_for_clean_dq_data_df = final_joined_df.select(
        col("file_name_s3"),
        coalesce(col("count"), lit(0)).alias("load_count"),
        coalesce(col("dq_count"), lit(0)).alias("dq_count")
    ).orderBy("file_name_s3")
    # create the list of tuples for easier processing
    stats_for_clean_dq_data = [
        (stats_row['file_name_s3'], stats_row['load_count'], stats_row['dq_count'])
        for stats_row in stats_for_clean_dq_data_df.orderBy('file_name_s3').collect()
    ]

    logger.info(f"List contains [file_name,length_validated_record_count,dq_issue_row_count] - {stats_for_clean_dq_data}")
    logger.info(f"List contains [line_number,source_daily_marker_count] - {source_counts}")
    logger.info(f"List contains [line_number,source_periodic_marker_count] - {source_weekly_counts}")

    # Ensure there's a corresponding count for each record in stats_clean_record_file_level_df
    if len(stats_for_clean_dq_data) == len(source_counts):
        summary_data = []
        for index, record in enumerate(stats_for_clean_dq_data):
            file_name_only = record[0]
            # Use index to get corresponding count from source_counts
            src_count = int(source_counts[index][1])
            source_distinct_count = int(source_weekly_counts[index][1]) if source_weekly_counts and 0 <= index < len(source_weekly_counts) else None
            successful_load_count = int(record[1])  # Access by index
            clean_record_count = successful_load_count - int(record[2])  # Access by index for dq_count
            length_rejection_count = src_count - successful_load_count

            # Append a dictionary with the summary data for this record
            summary_data.append({
                "file_name": file_name_only,
                "table_name": processed_file_name,
                "order_date": str(order_date_formatted),
                "job_run_date": str(job_run_timestamp),
                "source_file_record_count": src_count,
                "source_periodic_distinct_count": source_distinct_count,
                "record_length_rejection_count": length_rejection_count,
                "total_records_loaded_count": successful_load_count,
                "clean_record_count": clean_record_count,
                "data_quality_issue_count": int(record[2]),  # Access by index
                "number_of_source_files": int(number_of_source_files)
            })
    else:
        logger.error("Discrepancy in source daily marker file counts prevents daily reconciliation")

    # Define the schema for the DataFrame
    schema = StructType([
        StructField("file_name", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("job_run_date", StringType(), True),
        StructField("source_file_record_count", IntegerType(), True),
        StructField("source_periodic_distinct_count", IntegerType(), True),
        StructField("record_length_rejection_count", IntegerType(), True),
        StructField("total_records_loaded_count", IntegerType(), True),
        StructField("clean_record_count", IntegerType(), True),
        StructField("data_quality_issue_count", IntegerType(), True),
        StructField("number_of_source_files", IntegerType(), True)
    ])

    # Create the DataFrame
    summary_df = spark.createDataFrame(summary_data, schema)
    csv_output_data_path = os.path.join(csv_output_path, "01_detail", processed_file_name.lower())
    summary_header_part_file_path = os.path.join(csv_output_path, "header")
    summary_header_path = os.path.join(csv_output_path, "00_header")
    logger.info(f"summary spark csv file path --> {csv_output_path}")

    # Extract column names (header) from the DataFrame
    summary_header = summary_df.columns
    # Create a dictionary where each key-value pair is column_name: column_name
    header_dict = {col: col for col in summary_header}
    # Create a Row object from the dictionary
    header_row = Row(**header_dict)
    # Create a DataFrame using a list containing the Row object
    summary_header_df = spark.createDataFrame([header_row])
    logger.info(f"Header path: {summary_header_path}")

    # Write to CSV format
    summary_df.coalesce(1) \
        .write \
        .option("header", "false") \
        .format("csv") \
        .mode('overwrite') \
        .save(csv_output_data_path)

    summary_header_df.coalesce(1) \
        .write \
        .option("header", "false") \
        .format("csv") \
        .mode('Ignore') \
        .save(summary_header_part_file_path)
    base_filename = "header"
    bucket_name, summary_header_prefix = split_s3_uri(summary_header_path)
    rename_csv_files_in_s3(summary_header_part_file_path, base_filename, summary_header_prefix)

    # Write to Parquet format
    parquet_output_path = os.path.join(parquet_output_path, processed_file_name.lower())
    summary_df.coalesce(1) \
        .write \
        .format("parquet") \
        .mode('overwrite') \
        .save(parquet_output_path)

    logger.info(f"Summary files written to {csv_output_path} and {parquet_output_path}")


def delete_objects_in_ctlm_prefix(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
    logger.info(f"All objects in prefix deleted.Prefix is {prefix}")


def main(processed_file_location, raw_s3_data_prefix, raw_s3_metadata_prefix, raw_s3_bucket_name,
         processed_file_name, file_prefix, execution_dag_date, num_output_files,
         rejected_fw_misalignment_data_s3_uri, rejected_dq_data_s3_uri, exception_schema_fields,
         snowflake_exception_location, summary_data_s3_uri, snowflake_audit_log_location,
         ctlm_files_s3_uri, landing_decrypted_marker_s3_uri, marker_file_name,landing_periodic_marker_s3_uri):
    logger.info("list down the files s3 uri in a list")

    # Get the list of S3 files and their names
    s3_data_files, s3_data_file_names = list_s3_data_files_with_prefix(raw_s3_bucket_name, raw_s3_data_prefix,
                                                                       file_prefix)
    s3_bucket_ws, periodic_marker_s3_prefix = split_s3_uri(landing_periodic_marker_s3_uri)
    file_prefix_name = file_prefix.replace(".", "")
    _, periodic_marker_s3_file_name_list = list_s3_data_files_with_prefix(raw_s3_bucket_name, periodic_marker_s3_prefix, file_prefix_name)
    periodic_marker_s3_fname = ''.join(periodic_marker_s3_file_name_list)
    periodic_marker_s3_key = os.path.join("s3://",raw_s3_bucket_name,periodic_marker_s3_prefix,periodic_marker_s3_fname)
    logger.info(f"periodic marker file s3 key - {periodic_marker_s3_key}")

    # Calculate total number of files
    total_file_cnt = len(s3_data_files)
    logger.info(f"Total number of files: {total_file_cnt}")
    file_names_in_decrypt = [path.split('/')[-1] for path in s3_data_files]
    logger.info(f"s3 text file list from decrypt folder - -{file_names_in_decrypt}")


    # Fetch metadata information
    metadata_file_path = list_s3_files(raw_s3_bucket_name, raw_s3_metadata_prefix, execution_dag_date, file_prefix)
    logger.info(f"{metadata_file_path} --> metadata_dict ")
    metadata_dict, key_column_names, valid_rec_length = get_column_metadata(metadata_file_path)

    s3_uri = f"s3://{raw_s3_bucket_name}/{raw_s3_data_prefix}"
    logger.info(f"source text file path - {s3_uri}")

    # Clean Up the fw issue files from the push location if it is already existing
    bucket_name, ctlm_source_prefix = split_s3_uri(ctlm_files_s3_uri)
    delete_objects_in_ctlm_prefix(bucket_name, ctlm_source_prefix)

    output_file_path = os.path.join(processed_file_location, processed_file_name.lower())
    exception_file_path = os.path.join(snowflake_exception_location, processed_file_name.lower())
    rejected_fw_misalignment_data_file_output_path = os.path.join(rejected_fw_misalignment_data_s3_uri,
                                                                  processed_file_name.lower())
    rejected_dq_header_part_file_s3_uri = os.path.join(rejected_dq_data_s3_uri,"header")
    rejected_dq_header_file_s3_uri = os.path.join(rejected_dq_data_s3_uri,"00_header")

    rejected_dq_data_file_output_path = os.path.join(rejected_dq_data_s3_uri, "01_detail",
                                                     processed_file_name.lower())

    df = spark.read.text(f"{s3_uri}")
    df_with_file_name = df.withColumn("dataops_file_name_path", input_file_name())

    df_with_file_name.cache()

    logger.info(f"Total raw rows read from all files: {df_with_file_name.count()}")
    file_chunk_names = df_with_file_name.select('dataops_file_name_path').distinct().collect()
    numb_of_files_in_df = len(file_chunk_names)
    logger.info(f"Number of files in raw DF: {str(numb_of_files_in_df)}")
    stats_file_level_df = df_with_file_name.groupBy('dataops_file_name_path').count().collect()
    logger.info(stats_file_level_df)
    marker_file_path = os.path.join(landing_decrypted_marker_s3_uri, marker_file_name)

    processed_data = process_fixed_width_file(
            df_with_file_name, metadata_dict,
            valid_rec_length, key_column_names,
            processed_file_name
        )

    good_data_df = processed_data.transformed_rows
    rejected_data_with_dq_issues = processed_data.dq_issues_rows
    data_with_incorrect_fw_length_df = processed_data.invalid_width_rows_df
    stats_clean_record_file_level_df = processed_data.stats_clean_record_file_level_df

    data_df = create_dataframe_with_schema(good_data_df, metadata_dict)
    rejected_data_with_dq_df = create_rejected_data_df(rejected_data_with_dq_issues, exception_schema_fields)

    data_df.coalesce(int(num_output_files)).write.format("parquet").mode('overwrite').save(output_file_path)
    logger.info(f"Clean data is written into {output_file_path}")

    # Count the dq issue - records in the DataFrame
    dq_record_count = rejected_data_with_dq_df.count()

    # Extract column names (header) from the DataFrame
    dq_header = rejected_data_with_dq_df.columns
    # Create a dictionary where each key-value pair is column_name: column_name
    header_dict = {col: col for col in dq_header}
    # Create a Row object from the dictionary
    header_row = Row(**header_dict)
    # Create a DataFrame using a list containing the Row object
    dq_header_df = spark.createDataFrame([header_row])

    # Check if the record count is more than 0
    if dq_record_count > 0:
        rejected_data_with_dq_df.coalesce(10) \
            .write \
            .format("csv") \
            .option("header", "false") \
            .mode('overwrite') \
            .save(rejected_dq_data_file_output_path)
        dq_header_df.coalesce(1) \
            .write \
            .format("csv") \
            .option("header", "false") \
            .mode('ignore') \
            .save(rejected_dq_header_part_file_s3_uri)
        logger.info(f"DQ data is written into {rejected_dq_data_file_output_path} and count is {dq_record_count}")
        bucket_name, rejected_dq_header_file_s3_prefix = split_s3_uri(rejected_dq_header_file_s3_uri)
        rename_csv_files_in_s3(rejected_dq_header_part_file_s3_uri,"header", rejected_dq_header_file_s3_prefix)

    rejected_data_with_dq_df.coalesce(10).write.mode('overwrite').parquet(exception_file_path)
    logger.info(f"DQ data is written into {exception_file_path} and count is {dq_record_count}")

    # Count the invalid length - records in the DataFrame
    incorrect_fw_length_record_count = data_with_incorrect_fw_length_df.count()

    # Check if the record count is more than 0
    if incorrect_fw_length_record_count > 0:
        data_with_incorrect_fw_length_df.coalesce(10) \
            .write \
            .format("text") \
            .mode('overwrite') \
            .save(rejected_fw_misalignment_data_file_output_path)
        logger.info(
            f"Data with incorrect fixed width length written to {rejected_fw_misalignment_data_file_output_path} and "
            f"count is {incorrect_fw_length_record_count}")
    else:
        logger.info("No data with incorrect fixed width length to write.")

    marker_file_creation(processed_file_location, processed_file_name)
    logger.info("Marker file has been written to %s", processed_file_location)
    dq_issue_distinct_count_per_file = rejected_data_with_dq_df.dropDuplicates(['file_name', 'row_business_key']).groupBy("file_name").count().orderBy("file_name")

    # generate and rename audit log summary file in csv and parquet and copy the files
    generate_summary_file(processed_file_name, execution_dag_date, dq_issue_distinct_count_per_file,
                          summary_data_s3_uri, snowflake_audit_log_location, total_file_cnt, stats_clean_record_file_level_df,
                          marker_file_path,file_names_in_decrypt,periodic_marker_s3_key)


if __name__ == '__main__':
    args = getResolvedOptions(sys.argv,
                              ['raw_s3_data_prefix',
                               'raw_s3_metadata_prefix',
                               'raw_s3_bucket_name',
                               'processed_file_location',
                               'processed_file_name',
                               'file_prefix',
                               'num_output_files',
                               'rejected_fw_misalignment_data_s3_uri',
                               'rejected_dq_data_s3_uri',
                               'execution_dag_date',
                               'summary_data_s3_uri',
                               'snowflake_exception_location',
                               'snowflake_audit_log_location',
                               'ctlm_files_s3_uri', 'landing_decrypted_marker_s3_uri',
                               'marker_file_name','landing_periodic_marker_s3_uri'])
    processed_file_location = args['processed_file_location']
    raw_s3_data_prefix = args['raw_s3_data_prefix']
    raw_s3_metadata_prefix = args['raw_s3_metadata_prefix']
    raw_s3_bucket_name = args['raw_s3_bucket_name']
    processed_file_name = args['processed_file_name']
    file_prefix = args['file_prefix']
    num_output_files = args['num_output_files']
    rejected_fw_misalignment_data_s3_uri = args['rejected_fw_misalignment_data_s3_uri']
    rejected_dq_data_s3_uri = args['rejected_dq_data_s3_uri']
    execution_dag_date = args['execution_dag_date']
    snowflake_exception_location = args['snowflake_exception_location']
    summary_data_s3_uri = args['summary_data_s3_uri']
    snowflake_audit_log_location = args['snowflake_audit_log_location']
    ctlm_files_s3_uri = args['ctlm_files_s3_uri']
    landing_decrypted_marker_s3_uri = args['landing_decrypted_marker_s3_uri']
    marker_file_name = args['marker_file_name']
    landing_periodic_marker_s3_uri = args['landing_periodic_marker_s3_uri']
    main(processed_file_location, raw_s3_data_prefix, raw_s3_metadata_prefix, raw_s3_bucket_name,
         processed_file_name,
         file_prefix, execution_dag_date, num_output_files, rejected_fw_misalignment_data_s3_uri,
         rejected_dq_data_s3_uri, exception_schema_fields, snowflake_exception_location, summary_data_s3_uri,
         snowflake_audit_log_location, ctlm_files_s3_uri, landing_decrypted_marker_s3_uri, marker_file_name,landing_periodic_marker_s3_uri)