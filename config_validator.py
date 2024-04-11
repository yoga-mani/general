from . import mocker


def validate_config(config_json, df):
    for column_name in config_json:
        _validate_column_exists_in_df(column_name, df)
        _validate_config(config_json[column_name], df)


def _validate_column_exists_in_df(column_name, df):
    if column_name not in df.columns:
        raise RuntimeError(F'Column {column_name} not present in data')


def _validate_config(column_config, df):
    _validate_type(column_config)
    _validate_meta(column_config, df)


def _validate_meta(column_config, df):
    if column_config.get('meta') is not None:
        meta = column_config.get('meta')
        meta_keys = list(meta.keys())
        column_names = filter(lambda key: key.endswith('column_name'), meta_keys)
        for column_name in column_names:
            _validate_column_exists_in_df(meta[column_name], df)


def _validate_type(column_config):
    type_ = column_config['type']
    if mocker.TYPES.get(type_) is None:
        raise RuntimeError(F'Not a valid type: {column_config["type"]}')
