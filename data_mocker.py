import json

from utils import spark
import logging
import sys
import argparse
from utils import mocker
from utils import config_validator
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


def parse_arguments(args):
    parser = argparse.ArgumentParser(description='Data mocker')
    parser.add_argument('--input-file-pattern', type=str, required=True,
                        help='Input file pattern. Example: s3://test_bucket/clvib/20221127/*')
    parser.add_argument('--output-location', type=str, required=True,
                        help='Location where the event type specific files should be copied. '
                             'Example: s3://test_bucket/output')
    parser.add_argument('--file-type', required=True,
                        choices=['csv'], help='Type of input files. Only csv file type supported as of now.')
    parser.add_argument('--csv-delimiter', required=False,
                        type=str, help='The delimiter used in the csv files')
    parser.add_argument('--config-file-location', required=True,
                        type=str, help='s3 config file indicating what columns to mock')
    known_args, _ = parser.parse_known_args(args)
    return known_args


def load_config(config_file_location):
    s3 = boto3.client('s3')
    config_file_location = config_file_location.split('s3://')[1]
    bucket = config_file_location.split("/", 1)[0]
    key = config_file_location.split("/", 1)[1]
    res = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(res['Body'].read().decode('utf-8'))


if __name__ == '__main__':
    print("Printing from main...")
    args = parse_arguments(sys.argv[1:])
    column_configs = load_config(args.config_file_location)
    df = spark.load_file_to_spark_df(args.input_file_pattern, args.file_type, args.csv_delimiter)
    config_validator.validate_config(column_configs, df)

    for column_name in column_configs:
        column_config = column_configs[column_name]
        df = mocker.mock_column(df, column_name, column_config)

    spark.write_df(df, args.output_location, args.file_type, args.csv_delimiter)
