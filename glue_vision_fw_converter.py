import logging
import os
import itertools

from pyspark import RDD
from pyspark.sql.types import StringType, LongType, DecimalType, DateType, FloatType, StructType, StructField, \
    IntegerType
from pyspark.sql import SparkSession, Row, DataFrame, Window
from pyspark.sql.functions import input_file_name, col, length, trim, lit, regexp_extract, coalesce, split, \
    monotonically_increasing_id, row_number
import datetime
from decimal import Decimal, InvalidOperation
from awsglue.utils import getResolvedOptions
import sys
import argparse
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
import re
from dataclasses import dataclass, asdict
from typing import List, Optional
from urllib.parse import urlparse

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
s3 = boto3.client('s3')

# Set Hadoop Configuration to avoid creating success folder
sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ProcessedData:
    transformed_rows: RDD
    invalid_width_rows_df: DataFrame
    dq_issues_rows: RDD
    stats_clean_record_file_level_df: DataFrame


@dataclass
class ColumnMetadata:
    column_name: str
    column_length: int
    column_datatype: str
    column_key: str


def convert_fixed_width_row(row, file_name, key_columns, table_name, cols, datatypes, abs_offsets,
                            additional_col_flg):
    transformed_row = []
    etl_dq_er = []
    key_column_values = []

    for start, end, datatype, column_name in zip(abs_offsets, abs_offsets[1:], datatypes, cols):
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
                    error_desc = "Failed to parse date value as it doesn't have date value in a format YYYYMMDD"
                    etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
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
                    etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = 0
            if datatype.lower() == 'amount':
                try:
                    col_value = Decimal(col_value)

                except InvalidOperation:
                    error_desc = "Failed to cast the column to decimal from source data type - AMOUNT"
                    etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = Decimal('0')

        elif datatype.lower() == 'rate':

            if "%" in col_value:
                try:
                    col_value = float(col_value.strip('%')) / 100.0
                except ValueError:
                    error_desc = ("Failed to cast the column to float from source data type - "
                                  "rate where source field is expected to be populated with %")
                    etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = 0.0
            else:
                try:
                    col_value = float(col_value)
                except ValueError:
                    error_desc = "Failed to cast the column to float from source data type - rate"
                    etl_dq_er.append(f"{table_name}:{column_name}:{col_value}:{error_desc}:N:")
                    col_value = 0.0

        if column_name in key_columns:
            key_column_values.append(f"{column_name}={col_value}")

        transformed_row.append(col_value)
    if additional_col_flg == 'Y':
        transformed_row.append(f"{file_name}")
        project_region = {
            f"{project}_vision_region": "AU" if ".AU_" in file_name else ("NZ" if ".NZ_" in file_name else "")
        }
        region = project_region[f"{project}_vision_region"]
        transformed_row.append(region)
    key_column_str = "|".join(key_column_values)
    extracted_file_name = file_name.split('/')[-1]
    if len(etl_dq_er) > 0:
        etl_dq_er = [f"{extracted_file_name}:{key_column_str}:{error}" for error in etl_dq_er]
        return tuple(transformed_row), tuple(etl_dq_er)
    else:
        return tuple(transformed_row), None


@dataclass
class FixedWidthDataProcess:

    def __init__(self, metadata_dicts, key_column_names, valid_length, exception_schema_fields):
        self.exception_schema_fields = self.get_additional_exception_columns_schema(exception_schema_fields,
                                                                                    excp_flg='Y')
        self.metadata_dicts = metadata_dicts
        self.key_column_names = key_column_names
        self.valid_length = valid_length
        self.cols = [col_dict['column_name'] for col_dict in self.metadata_dicts]
        self.col_lengths = [col_dict['column_length'] for col_dict in self.metadata_dicts]
        self.datatypes = [col_dict['column_datatype'] for col_dict in self.metadata_dicts]
        self.abs_offsets = [0] + list(itertools.accumulate(self.col_lengths))

    @classmethod
    def get_column_metadata(cls, metadata_file_path):
        logger.info("Reading metadata from: %s", metadata_file_path)
        metadata_df = spark.read.option("header", "false").csv(metadata_file_path)
        metadata_dicts: List[dict] = []

        for row in metadata_df.collect():
            column_metadata = ColumnMetadata(row[1].replace("-", "_"), int(row[3]), row[2], row[4])
            metadata_dicts.append(asdict(column_metadata))

        logger.info("metadata file is read")
        key_column_names = [metadata['column_name'] for metadata in metadata_dicts if metadata['column_key'] == 'Y']
        valid_length = sum(metadata['column_length'] for metadata in metadata_dicts)
        logger.info(f"total record length: {valid_length}")

        return metadata_dicts, key_column_names, valid_length

    @classmethod
    def read_and_process_s3_metadata(cls, raw_s3_bucket_name, raw_s3_metadata_prefix, execution_dag_date, file_prefix,
                                     exception_fields):

        metadata_file_path = cls.list_s3_files(raw_s3_bucket_name, raw_s3_metadata_prefix, execution_dag_date,
                                               file_prefix)
        logger.info(f"{metadata_file_path} --> metadata_dict ")
        metadata_dicts, key_column_names, valid_length = cls.get_column_metadata(metadata_file_path)
        logger.info(f"metadata_dict - {metadata_dicts}")
        return cls(metadata_dicts, key_column_names, valid_length, exception_fields)

    def get_additional_exception_columns_schema(self, fields_list, excp_flg):
        logger.info(f"parse arg output -- {fields_list}")
        if excp_flg == 'Y' and len(fields_list) == 8:
            schema_list = [StructField(field, StringType(), True) for field in fields_list if field]
            logger.info(f"schema_list of exception columns - {schema_list}")
            return schema_list
        elif excp_flg == 'N':
            schema_list = [StructField(field, StringType(), True) for field in fields_list if field]
            logger.info(f"schema_list of additional columns - {schema_list}")
            return schema_list
        return []

    @classmethod
    def list_s3_files(cls, raw_s3_bucket_name, raw_s3_metadata_prefix, execution_dag_date, file_prefix):
        logger.info("latest metadata file to be found from the mata s3 uri ")
        file_prefix = str(file_prefix).replace(".", "")
        s3_metadata_objs = s3.list_objects_v2(Bucket=raw_s3_bucket_name, Prefix=raw_s3_metadata_prefix)
        s3_metadata_files = [f's3://{raw_s3_bucket_name}/{obj["Key"]}' for obj in s3_metadata_objs.get('Contents', [])]

        logger.info("scan through the file names and find the file matching with the file prefix pattern")
        s3_metadata_file = [s3_uri for s3_uri in s3_metadata_files if s3_uri.split('/')[-1].startswith(file_prefix)]
        execution_date = datetime.datetime.strptime(execution_dag_date, '%Y%m%d')
        date_pattern = r'\d{8}'
        latest_metadata_s3_uri = None
        matching_dates = []
        for s3_uri in s3_metadata_file:
            match = re.search(date_pattern, s3_uri)
            if match:
                date_str = match.group(0)
                try:
                    file_date = datetime.datetime.strptime(date_str, '%Y%m%d')
                    if file_date <= execution_date:
                        matching_dates.append(file_date)
                except ValueError:
                    logger.error(f"Unable to parse metadata date {date_str}")
                    raise
        if matching_dates:
            max_matching_date = max(matching_dates)
            latest_metadata_s3_uri = \
                [s3_uri for s3_uri in s3_metadata_file if s3_uri.split('/')[-1].startswith(file_prefix)
                 and datetime.datetime.strptime(
                    re.search(date_pattern, s3_uri).group(0), '%Y%m%d') == max_matching_date][0]
        logger.info("latest_metadata_s3_uri", latest_metadata_s3_uri)
        return latest_metadata_s3_uri

    def get_spark_datatype(self, datatype):
        mapping = {
            'string': StringType(),
            'numeric': LongType(),
            'amount': DecimalType(38, 18),
            'snumeric': LongType(),
            'date': DateType(),
            'rate': FloatType()
        }
        cleaned_datatype = str(datatype).strip()
        return mapping.get(cleaned_datatype.lower(), StringType())

    def process_fixed_width_file(self, df, table_name, file_name_path_column, additional_output_columns):
        good_data_df = df.filter(length(col("value")) == self.valid_length)
        invalid_width_rows_df = df.filter(length(col("value")) != self.valid_length).select("value")
        successful_load_count = good_data_df.count()

        stats_clean_record_file_level_df = good_data_df.groupBy(file_name_path_column) \
            .count() \
            .orderBy(file_name_path_column)
        logger.info(f"good records count : {str(successful_load_count)}")
        logger.warning(f"bad records count : {str(invalid_width_rows_df.count())}")
        key_column_names = self.key_column_names
        cols = self.cols
        datatypes = self.datatypes
        abs_offsets = self.abs_offsets
        additional_col_flg = 'Y' if additional_output_columns else 'N'

        transformed_data = good_data_df.rdd.map(
            lambda row: convert_fixed_width_row(
                row['value'],
                row[file_name_path_column],
                key_column_names,
                table_name, cols, datatypes, abs_offsets, additional_col_flg
            )
        )
        logger.info(f"transformed_data is ready")
        processed_good_data = transformed_data.filter(
            lambda x: x is not None and x[0] is not None
        ).map(lambda x: x[0])

        data_with_dq_issues = transformed_data.flatMap(
            lambda x: x[1] if x is not None and x[1] is not None else [])

        return ProcessedData(
            transformed_rows=processed_good_data,
            invalid_width_rows_df=invalid_width_rows_df,
            dq_issues_rows=data_with_dq_issues,
            stats_clean_record_file_level_df=stats_clean_record_file_level_df
        )

    def create_dataframe_with_schema(self, processed_data, metadata,
                                     optional_fields: Optional[List[StructField]] = None) -> DataFrame:
        logger.info("Creating DataFrame with specified schema.")

        filtered_columns = [
            (col_dict['column_name'], col_dict['column_datatype'])
            for col_dict in metadata
            if col_dict['column_name'] != "FILLERS"
        ]

        logger.info("prepared filtered column name and datatypes.")
        name_struct = [StructField(name, self.get_spark_datatype(datatype), True)
                       for name, datatype in filtered_columns]
        logger.info(f"name struct -{name_struct}")
        logger.info(f"optional_fields struct -{optional_fields}")
        schema = StructType(name_struct + optional_fields)
        logger.info(f"prepared schema for valid data df.- {schema}")

        data_df = processed_data.toDF(schema=schema)
        logger.info("Finished - Creating DataFrame with specified schema.")
        return data_df

    def create_rejected_data_df(self, rejected_data_rdd, schema_fields):
        def split_row(line):
            parts = line.split(":")
            if len(parts) != len(schema_fields):
                raise ValueError(f"Data row has {len(parts)} parts, but schema expects {len(schema_fields)} fields.")
            return Row(*parts)

        rejected_rows = rejected_data_rdd.map(split_row)

        schema = StructType(schema_fields)

        rejected_data_df = rejected_rows.toDF(schema=schema)
        return rejected_data_df

    def marker_file_creation(self, output_file_path, table_name):

        marker_file_name = ""
        bucket, parquet_file_dir = self.split_s3_uri(output_file_path)
        if parquet_file_dir:
            if not parquet_file_dir.endswith('/'):
                parquet_file_dir = f"{parquet_file_dir}/"

            table_name = str(table_name).upper()
            marker_file_name = f"{table_name}_LANDED"
            marker_file_s3_path = f"{parquet_file_dir}{marker_file_name}"

        else:
            marker_file_s3_path = marker_file_name

        logger.info("Writing the marker file to S3.")
        s3.put_object(Bucket=bucket, Key=marker_file_s3_path, Body=b'')

        logger.info(f"Marker file '{marker_file_name}' written to: s3://{bucket}/{marker_file_s3_path}")

    def list_s3_data_files_with_prefix(self, bucket_name, prefix, file_prefix):
        s3_data_objs = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        s3_data_files_all = [f's3://{bucket_name}/{obj["Key"]}' for obj in s3_data_objs.get('Contents', [])]
        s3_data_files = [s3_uri for s3_uri in s3_data_files_all if s3_uri.split('/')[-1].startswith(file_prefix)]
        s3_data_file_names = [s3_uri.split('/')[-1] for s3_uri in s3_data_files_all if
                              s3_uri.split('/')[-1].startswith(file_prefix)]

        return s3_data_files, s3_data_file_names

    def split_s3_uri(self, s3_uri):
        if not s3_uri.startswith("s3://"):
            raise ValueError("Invalid S3 URI format. Must start with 's3://'.")
        parts = s3_uri[5:].split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket_name, prefix

    def rename_csv_files_in_s3(self, s3_uri, base_filename, destination_prefix):
        bucket_name, source_prefix = self.split_s3_uri(s3_uri)
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
        filename_extn = 'CSV'
        if 'Contents' in response:
            for obj in response['Contents']:
                source_key = obj['Key']
                logger.info(f"check srce key: {source_key}")

                new_filename = f"{base_filename}.{filename_extn}"
                new_key = os.path.join(destination_prefix, str(new_filename).upper())
                logger.info(f"rename source key to new key - {source_key} - {new_key}")

                s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_key}, Key=new_key)
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                logger.info(f"File renaming completed in the new path - {new_key}")

    def process_source_marker_file(self, marker_file_path, metric_name):

        # rdd_with_index = spark.sparkContext.textFile(marker_file_path).zipWithIndex()
        #
        # source_marker_df = rdd_with_index.map(lambda x: (x[1], x[0])).toDF(["line_number", "line_content"])
        # source_marker_content = source_marker_df.collect()

        df = spark.read.text(marker_file_path)
        window_spec = Window.orderBy(monotonically_increasing_id())
        df_with_line_number = df.withColumn("line_number", row_number().over(window_spec))
        source_marker_df = df_with_line_number.selectExpr("line_number", "value as line_content")

        source_marker_content = source_marker_df.collect()

        logger.info(f"source marker count --> {source_marker_content}")

        source_marker_df = source_marker_df.withColumn(metric_name,
                                                       regexp_extract('line_content', ':\s*(\d+)', 1).cast('integer'))

        source_marker_df = source_marker_df.orderBy("line_number")

        source_counts = [(row['line_number'], row[metric_name]) for row in source_marker_df.collect()]
        logger.info(f"source count in the same order as it exists in the marker file --> {source_counts}")
        return source_counts

    def generate_summary_file(self, processed_file_name, order_date, dq_issue_distinct_count_per_file_df,
                              csv_output_path, parquet_output_path, number_of_source_files,
                              stats_clean_record_file_level_df, marker_file_path, file_names_in_decrypt, project):

        order_date_dtype = datetime.datetime.strptime(order_date, '%Y%m%d')
        order_date_formatted = order_date_dtype.strftime('%Y-%m-%d')

        source_counts = self.process_source_marker_file(marker_file_path, "count")
        job_run_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        summary_data = []
        stats_clean_record_file_level_df = stats_clean_record_file_level_df.withColumn(
            "extracted_file_name",
            regexp_extract(f"{project}_file_name_path", ".*/([^/]+)$", 1)
        )
        file_names_df = spark.createDataFrame([(name,) for name in file_names_in_decrypt], ["file_name_s3"])

        joined_df = stats_clean_record_file_level_df.join(
            dq_issue_distinct_count_per_file_df.withColumnRenamed("count", "dq_count"),
            stats_clean_record_file_level_df["extracted_file_name"] == dq_issue_distinct_count_per_file_df["file_name"],
            "left"
        ).withColumn("dq_count", coalesce(col("dq_count"), lit(0)))

        final_joined_df = file_names_df.join(
            joined_df,
            joined_df["extracted_file_name"] == file_names_df["file_name_s3"],
            "left"
        )

        stats_for_clean_dq_data_df = final_joined_df.select(
            col("file_name_s3"),
            coalesce(col("count"), lit(0)).alias("load_count"),
            coalesce(col("dq_count"), lit(0)).alias("dq_count")
        ).orderBy("file_name_s3")

        stats_for_clean_dq_data = [
            (stats_row['file_name_s3'], stats_row['load_count'], stats_row['dq_count'])
            for stats_row in stats_for_clean_dq_data_df.orderBy('file_name_s3').collect()
        ]

        logger.info(
            f"List contains [file_name,length_validated_record_count,dq_issue_row_count] - {stats_for_clean_dq_data}")
        logger.info(f"List contains [line_number,source_daily_marker_count] - {source_counts}")

        if len(stats_for_clean_dq_data) == len(source_counts):
            summary_data = []
            for index, record in enumerate(stats_for_clean_dq_data):
                file_name_only = record[0]
                src_count = int(source_counts[index][1])
                successful_load_count = int(record[1])
                clean_record_count = successful_load_count - int(record[2])
                length_rejection_count = src_count - successful_load_count

                summary_data.append({
                    "file_name": file_name_only,
                    "table_name": processed_file_name,
                    "order_date": str(order_date_formatted),
                    "job_run_date": str(job_run_timestamp),
                    "source_file_record_count": src_count,
                    "record_length_rejection_count": length_rejection_count,
                    "total_records_loaded_count": successful_load_count,
                    "clean_record_count": clean_record_count,
                    "data_quality_issue_count": int(record[2]),
                    "number_of_source_files": int(number_of_source_files)
                })
        else:
            logger.error("Discrepancy in source daily marker file counts prevents daily reconciliation")

        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("file_name", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("job_run_date", StringType(), True),
            StructField("source_file_record_count", IntegerType(), True),
            StructField("record_length_rejection_count", IntegerType(), True),
            StructField("total_records_loaded_count", IntegerType(), True),
            StructField("clean_record_count", IntegerType(), True),
            StructField("data_quality_issue_count", IntegerType(), True),
            StructField("number_of_source_files", IntegerType(), True)
        ])

        summary_df = spark.createDataFrame(summary_data, schema)
        csv_output_data_path = os.path.join(csv_output_path, "01_detail", processed_file_name.lower())
        summary_header_part_file_path = os.path.join(csv_output_path, "header")
        summary_header_path = os.path.join(csv_output_path, "00_header")
        logger.info(f"summary spark csv file path --> {csv_output_path}")

        summary_header = summary_df.columns

        header_dict = {col: col for col in summary_header}

        header_row = Row(**header_dict)

        summary_header_df = spark.createDataFrame([header_row])
        logger.info(f"Header path: {summary_header_path}")

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
        bucket_name, summary_header_prefix = self.split_s3_uri(summary_header_path)
        self.rename_csv_files_in_s3(summary_header_part_file_path, base_filename, summary_header_prefix)

        parquet_output_path = os.path.join(parquet_output_path, processed_file_name.lower())
        summary_df.coalesce(1) \
            .write \
            .format("parquet") \
            .mode('overwrite') \
            .save(parquet_output_path)

        logger.info(f"Summary files written to {csv_output_path} and {parquet_output_path}")

    def delete_objects_in_ctlm_prefix(self, bucket_name, prefix):
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        logger.info(f"All objects in prefix deleted.Prefix is {prefix}")

    def main(self, processed_file_location, raw_s3_data_prefix, raw_s3_metadata_prefix, raw_s3_bucket_name,
             processed_file_name, file_prefix, execution_dag_date, num_output_files,
             rejected_fw_misalignment_data_s3_uri, rejected_dq_data_s3_uri,
             snowflake_exception_location, summary_data_s3_uri, snowflake_audit_log_location,
             ctlm_files_s3_uri, landing_decrypted_marker_s3_uri, marker_file_name, project, additional_output_columns):
        logger.info("list down the files s3 uri in a list")

        s3_data_files, s3_data_file_names = self.list_s3_data_files_with_prefix(raw_s3_bucket_name, raw_s3_data_prefix,
                                                                                file_prefix)

        total_file_cnt = len(s3_data_files)
        logger.info(f"Total number of files: {total_file_cnt}")
        file_names_in_decrypt = [path.split('/')[-1] for path in s3_data_files]
        logger.info(f"s3 text file list from decrypt folder - -{file_names_in_decrypt}")

        metadata_file_path = self.list_s3_files(raw_s3_bucket_name, raw_s3_metadata_prefix, execution_dag_date,
                                                file_prefix)
        logger.info(f"{metadata_file_path} --> metadata_dict ")

        s3_uri = f"s3://{raw_s3_bucket_name}/{raw_s3_data_prefix}"
        logger.info(f"source text file path - {s3_uri}")

        bucket_name, ctlm_source_prefix = self.split_s3_uri(ctlm_files_s3_uri)
        self.delete_objects_in_ctlm_prefix(bucket_name, ctlm_source_prefix)

        output_file_path = os.path.join(processed_file_location, processed_file_name.lower())
        exception_file_path = os.path.join(snowflake_exception_location, processed_file_name.lower())
        rejected_fw_misalignment_data_file_output_path = os.path.join(rejected_fw_misalignment_data_s3_uri,
                                                                      processed_file_name.lower())
        rejected_dq_header_part_file_s3_uri = os.path.join(rejected_dq_data_s3_uri, "header")
        rejected_dq_header_file_s3_uri = os.path.join(rejected_dq_data_s3_uri, "00_header")

        rejected_dq_data_file_output_path = os.path.join(rejected_dq_data_s3_uri, "01_detail",
                                                         processed_file_name.lower())

        df = spark.read.text(f"{s3_uri}")
        file_name_path_column = f"{project}_file_name_path"
        df_with_file_name = df.withColumn(file_name_path_column, input_file_name())

        df_with_file_name.cache()

        logger.info(f"Total raw rows read from all files: {df_with_file_name.count()}")
        file_chunk_names = df_with_file_name.select(file_name_path_column).distinct().collect()
        numb_of_files_in_df = len(file_chunk_names)
        logger.info(f"Number of files in raw DF: {str(numb_of_files_in_df)}")
        stats_file_level_df = df_with_file_name.groupBy(file_name_path_column).count().collect()
        logger.info(stats_file_level_df)
        marker_file_path = os.path.join(landing_decrypted_marker_s3_uri, marker_file_name)

        additional_columns_schema = self.get_additional_exception_columns_schema(additional_output_columns,
                                                                                 excp_flg='N')

        processed_data = self.process_fixed_width_file(
            df_with_file_name,
            processed_file_name, file_name_path_column, additional_output_columns
        )

        good_data_df = processed_data.transformed_rows
        rejected_data_with_dq_issues = processed_data.dq_issues_rows
        data_with_incorrect_fw_length_df = processed_data.invalid_width_rows_df
        stats_clean_record_file_level_df = processed_data.stats_clean_record_file_level_df

        data_df = self.create_dataframe_with_schema(good_data_df, self.metadata_dicts,
                                                    optional_fields=additional_columns_schema)
        if not self.exception_schema_fields:
            logger.error("No exceptional dq columns schema passed from the config file")
            raise ValueError("No exceptional dq columns schema provided")
        rejected_data_with_dq_df = self.create_rejected_data_df(rejected_data_with_dq_issues,
                                                                self.exception_schema_fields)

        data_df.coalesce(int(num_output_files)).write.format("parquet").mode('overwrite').save(output_file_path)
        logger.info(f"Clean data is written into {output_file_path}")

        dq_record_count = rejected_data_with_dq_df.count()

        dq_header = rejected_data_with_dq_df.columns

        header_dict = {col: col for col in dq_header}

        header_row = Row(**header_dict)

        dq_header_df = spark.createDataFrame([header_row])

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
            bucket_name, rejected_dq_header_file_s3_prefix = self.split_s3_uri(rejected_dq_header_file_s3_uri)
            self.rename_csv_files_in_s3(rejected_dq_header_part_file_s3_uri, "header",
                                        rejected_dq_header_file_s3_prefix)

        rejected_data_with_dq_df.coalesce(10).write.mode('overwrite').parquet(exception_file_path)
        logger.info(f"DQ data is written into {exception_file_path} and count is {dq_record_count}")

        incorrect_fw_length_record_count = data_with_incorrect_fw_length_df.count()

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

        self.marker_file_creation(processed_file_location, processed_file_name)
        logger.info("Marker file has been written to %s", processed_file_location)
        dq_issue_distinct_count_per_file = rejected_data_with_dq_df.dropDuplicates(
            ['file_name', 'row_business_key']).groupBy("file_name").count().orderBy("file_name")

        self.generate_summary_file(processed_file_name, execution_dag_date, dq_issue_distinct_count_per_file,
                                   summary_data_s3_uri, snowflake_audit_log_location, total_file_cnt,
                                   stats_clean_record_file_level_df,
                                   marker_file_path, file_names_in_decrypt, project)


def parse_arguments(args):
    parser = argparse.ArgumentParser(description='Process S3 data for a fixed-width file ingestion process.')
    parser.add_argument('--raw_s3_data_prefix', required=True, help='S3 data prefix for raw fw data file.')
    parser.add_argument('--raw_s3_metadata_prefix', required=True, help='S3 metadata prefix for raw data.')
    parser.add_argument('--raw_s3_bucket_name', required=True, help='Bucket name for raw S3 data.')
    parser.add_argument('--processed_file_location', required=True, help='S3 URI Location to store processed output '
                                                                         'parquet files.')
    parser.add_argument('--processed_file_name', required=True, help='Table Name for processed files.')
    parser.add_argument('--file_prefix', required=True, help='Input FW File Prefix.')
    parser.add_argument('--num_output_files', type=int, required=True, help='Number of output files.')
    parser.add_argument('--rejected_fw_misalignment_data_s3_uri', required=True, help='S3 URI for misaligned data.')
    parser.add_argument('--rejected_dq_data_s3_uri', required=True, help='S3 URI for data quality issues.')
    parser.add_argument('--execution_dag_date', required=True, help='Execution date for the DAG.')
    parser.add_argument('--summary_data_s3_uri', required=True, help='S3 URI for summary data.')
    parser.add_argument('--snowflake_exception_location', required=True,
                        help='S3 URI Location for Snowflake exceptions.')
    parser.add_argument('--snowflake_audit_log_location', required=True,
                        help='S3 URI Location for Snowflake audit logs.')
    parser.add_argument('--ctlm_files_s3_uri', required=True,
                        help='S3 URI from where feedback files to send to CTL-M S3 bucket.')
    parser.add_argument('--landing_decrypted_marker_s3_uri', required=True, help='S3 URI for decrypted marker files.')
    parser.add_argument('--marker_file_name', required=True, help='Marker file name.')
    parser.add_argument('--project', required=True, help='Project identifier.')
    parser.add_argument('--additional_output_columns', type=str, required=False, default='',
                        help='Facilitate to add additional columns in the output')
    parser.add_argument('--exception_columns', type=str, required=True,
                        help='exception column names in the list to include in the schema.')

    known_args, _ = parser.parse_known_args(args)
    return known_args


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    processed_file_location = args.processed_file_location
    raw_s3_data_prefix = args.raw_s3_data_prefix
    raw_s3_metadata_prefix = args.raw_s3_metadata_prefix
    raw_s3_bucket_name = args.raw_s3_bucket_name
    processed_file_name = args.processed_file_name
    file_prefix = args.file_prefix
    num_output_files = args.num_output_files
    rejected_fw_misalignment_data_s3_uri = args.rejected_fw_misalignment_data_s3_uri
    rejected_dq_data_s3_uri = args.rejected_dq_data_s3_uri
    execution_dag_date = args.execution_dag_date
    snowflake_exception_location = args.snowflake_exception_location
    summary_data_s3_uri = args.summary_data_s3_uri
    snowflake_audit_log_location = args.snowflake_audit_log_location
    ctlm_files_s3_uri = args.ctlm_files_s3_uri
    landing_decrypted_marker_s3_uri = args.landing_decrypted_marker_s3_uri
    marker_file_name = args.marker_file_name
    project = args.project
    additional_output_columns = args.additional_output_columns
    exception_columns = args.exception_columns
    additional_output_columns = args.additional_output_columns.split(',') if args.additional_output_columns else []
    exception_columns = args.exception_columns.split(',') if args.exception_columns else []
    logger.info(f"additional cols schema input --{additional_output_columns}")
    logger.info(f"exception cols schema input --{exception_columns}")

    fixedwidth_ingestion_obj = FixedWidthDataProcess.read_and_process_s3_metadata(raw_s3_bucket_name,
                                                                                  raw_s3_metadata_prefix,
                                                                                  execution_dag_date, file_prefix,
                                                                                  exception_columns)
    fixedwidth_ingestion_obj.main(processed_file_location, raw_s3_data_prefix, raw_s3_metadata_prefix,
                                  raw_s3_bucket_name,
                                  processed_file_name,
                                  file_prefix, execution_dag_date, num_output_files,
                                  rejected_fw_misalignment_data_s3_uri,
                                  rejected_dq_data_s3_uri, snowflake_exception_location, summary_data_s3_uri,
                                  snowflake_audit_log_location, ctlm_files_s3_uri, landing_decrypted_marker_s3_uri,
                                  marker_file_name, project, additional_output_columns)
