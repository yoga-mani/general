from pyspark.sql import SparkSession

SUPPORTED_FILE_TYPES = ["csv"]


def _get_spark():
    return SparkSession \
        .builder \
        .appName("Mock Data Generator") \
        .getOrCreate()


def _load_csv_to_spark_df(path, delimiter, spark):
    df = spark.read.option("header", "true").option("delimiter", delimiter).csv(path)
    print("Number of records in df: ", df.count())
    df.show()
    return df


def _write_csv_df(df, path, delimiter, spark=_get_spark()):
    df.write.csv(path, header=True, mode="overwrite", sep=delimiter)


def load_file_to_spark_df(path, file_type, delimiter, spark=_get_spark()):
    if file_type not in SUPPORTED_FILE_TYPES:
        raise RuntimeError("File type is not supported..")
    if file_type == "csv":
        return _load_csv_to_spark_df(path, delimiter, spark)


def write_df(df, path, file_type, delimiter, spark=_get_spark()):
    _write_csv_df(df, path, delimiter, spark)
