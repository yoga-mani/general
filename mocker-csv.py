#!/usr/bin/env python

import csv
import yaml
from mimesis.locales import Locale
from mimesis.schema import Field, Schema
from functools import cache
import logging
import argparse
from pathlib import Path
import re

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(description='mocker csv to mock csv supporting lookups and caching.')
parser.add_argument('csv', action='store', help='csv file path.')
parser.add_argument('target_path', action='store', help='target s3 path.')
parser.add_argument('-d', '--test_date', action='store', help='test date.')
parser.add_argument('-c', '--config', action='store', help='config file path.')
parser.add_argument('-v', '--verbose', action='count', help='verbosity level.')
parser.add_argument('--version', action='version', version='%(prog)s 1.0')
args = parser.parse_args()


_ = Field(locale=Locale.EN_AU)
EMAIL_CHECK='[^@]+@[^@]+\.[^@]+'
PHONE_CHECK='[\s0-9]+'


def prepare_mock_spec(h, mock_config):
    idx2name = {}
    name2idx = {}
    mock_spec = []
    for idx, name in enumerate(h):
        idx2name[idx] = name
        name2idx[name] = idx

    for name in h:
        mc = {
            'cname': name,
            'cidx': name2idx[name],
            'lnames': [],
            'lidxs': [],
            'data_provider': None
        }
        if name in mock_config:
            if 'IdLookup' in mock_config[name]:
                mc['lnames'] = mock_config[name]['IdLookup']
                mc['lidxs'] = [name2idx[v] for v in mc['lnames']]
            mc['data_provider'] = mock_config[name]['DataProvider']

        mock_spec.append(mc)
    return mock_spec

@cache
def mock_function(mimesis_provider, cname, cvalue, lname, lvalue, **kwargs):
    if cvalue is not None and cvalue != "" and mimesis_provider is not None:
        return _(mimesis_provider, **kwargs)
    return cvalue

def pick_provider(cvalue, providers_list):
    if providers_list is None or len(providers_list) == 0:
        return {}

    for provider in providers_list:
        if provider['ValueType'] == 'Email' and re.match(EMAIL_CHECK, cvalue):
            return provider
        elif provider['ValueType'] == 'Phone' and re.match(PHONE_CHECK, cvalue):
            return provider
        elif provider['ValueType'] == 'All':
            return provider

    return providers_list[-1]

def mock_row(row_mock_spec, row):
    mocked_row = []
    for idx, ovalue in enumerate(row):
        cspec = row_mock_spec[idx]
        provider = pick_provider(ovalue, cspec['data_provider'])
        if 'MockKwargs' in provider:
            mvalue = mock_function(
                provider.get('Function'),
                cspec['cname'], ovalue,
                '.'.join(cspec['lnames']),
                '#~'.join([row[lidx] for lidx in cspec['lidxs']]),
                **provider['MockKwargs']
            )
        else:
            mvalue = mock_function(
                provider.get('Function'),
                cspec['cname'], ovalue,
                '.'.join(cspec['lnames']),
                '#~'.join([row[lidx] for lidx in cspec['lidxs']]),
            )
        mocked_row.append(mvalue)
    return mocked_row

def create_target_path(source_file, target_path, tdate=''):
    file_name = source_file.name
    if tdate:
        file_name = source_file.stem
        file_name = f"{'_'.join(file_name.split('_')[:-1])}_{tdate}.{source_file.suffix}"
    return Path(target_path, file_name)


mock_config = None
with open(Path(args.config)) as ymlfile:
    try:
        mock_config = yaml.safe_load(ymlfile)
    except yaml.YAMLError as exc:
        logger.error("Unable to parse mock spec file")
        raise exc

source_path = Path(args.csv)
if not source_path.exists() or not source_path.is_dir():
    logger.error("source path doesn't exist or is not a directory", exc_info=True)
    raise

target_path = Path(args.target_path)
if not target_path.is_dir():
    logger.error("target path must be a directory", exc_info=True)
    raise

for csv_path in source_path.iterdir():
    with open(csv_path, newline='') as ifile, open(create_target_path(csv_path, target_path, args.test_date), newline='', mode='w') as ofile:
        reader = csv.reader(ifile, delimiter='|')
        writer = csv.writer(ofile, delimiter='|')
        header = next(reader)
        writer.writerow(header)
        mock_spec = prepare_mock_spec(header, mock_config['MockColumns'])
        for row in reader:
            mocked_row = mock_row(mock_spec, row)
            writer.writerow(mocked_row)
