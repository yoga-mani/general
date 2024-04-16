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


class SummaryAuditLog:

    def process_source_marker_file(self, marker_file_path, metric_name):

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

    def get_summary_stats(self, stats_clean_record_file_level_df, project, file_names_in_decrypt,
                          dq_issue_distinct_count_per_file_df, source_counts):

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
        return stats_for_clean_dq_data

    def get_summary_df(self, stats_for_clean_dq_data, source_counts, processed_file_name, order_date_formatted,
                       job_run_timestamp, number_of_source_files):
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

        return summary_df

    def create_rename_summary_file(self, csv_output_path, processed_file_name, summary_df, parquet_output_path):
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

    def summary_file(self, ):
        self.generate_summary_file(processed_file_name, execution_dag_date, dq_issue_distinct_count_per_file,
                                   summary_data_s3_uri, snowflake_audit_log_location, total_file_cnt,
                                   stats_clean_record_file_level_df,
                                   marker_file_path, file_names_in_decrypt, project)
