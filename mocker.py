from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from . import mimesis_utils


def _mock_first_name(df, column_name, column_config):
    custom_udf = udf(mimesis_utils.mock_first_name, StringType())
    if column_config.get('meta') is None or column_config.get('meta').get('gender_column_name') is None:
        return df.withColumn(column_name, custom_udf())
    return df.withColumn(column_name, custom_udf(col(column_config['meta']['gender_column_name'])))


def _mock_last_name(df, column_name, column_config):
    custom_udf = udf(mimesis_utils.mock_last_name, StringType())
    if column_config.get('meta') is None or column_config.get('meta').get('gender_column_name') is None:
        return df.withColumn(column_name, custom_udf())
    return df.withColumn(column_name, custom_udf(col(column_config['meta']['gender_column_name'])))


def _mock_column(df, column_name, mocker_fn):
    custom_udf = udf(mocker_fn, StringType())
    return df.withColumn(column_name, custom_udf())


def _mock_date_of_birth(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_date_of_birth)


def _mock_street_number(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_street_number)


def _mock_street_name(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_street_name)


def _mock_street_suffix(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_street_suffix)


def _mock_city(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_city)


def _mock_state(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_state)


def _mock_zip_code(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_zip_code)


def _mock_email(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_email)


def _mock_phone_number(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_phone_number)


def _mock_contact_details(df, column_name, column_config):
    custom_udf = udf(mimesis_utils.mock_contact_details, StringType())
    return df.withColumn(column_name, custom_udf(col(column_config['meta']['contact_type_column_name'])))


def _mock_account_number(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_account_number)


def _mock_drivers_license(df, column_name, _):
    return _mock_column(df, column_name, mimesis_utils.mock_drivers_license)


TYPES = {
    "firstname": _mock_first_name,
    "lastname": _mock_last_name,
    "date_of_birth": _mock_date_of_birth,
    "street_number": _mock_street_number,
    "street_name": _mock_street_name,
    "street_suffix": _mock_street_suffix,
    "city": _mock_city,
    "state": _mock_state,
    "zip_code": _mock_zip_code,
    "email": _mock_email,
    "phone_number": _mock_phone_number,
    "contact_details": _mock_contact_details,
    "account_number": _mock_account_number,
    "drivers_license": _mock_drivers_license,
}


def mock_column(df, column_name, column_config):
    type_name = column_config['type']
    modified_df = TYPES[type_name](df, column_name, column_config)
    modified_df.show()
    return modified_df
