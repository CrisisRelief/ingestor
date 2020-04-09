import argparse
import json
import sys
from pprint import pformat

import pandas as pd
from jinja2 import Template
import yaml

from .sheet import Sheet, authorize_creds
from .mod_dump import exit_if_no_mod


def epprint(*args, **kwargs):
    print(pformat(*args, **kwargs), file=sys.stderr)


def parse_config(config_file, schema_file):
    with open(config_file) as stream:
        conf = yaml.safe_load(stream)
    with open(schema_file) as stream:
        conf['schema_mapping'] = yaml.safe_load(stream)
    return conf


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-C', '--creds-file', default='credentials.json')
    parser.add_argument(
        '-c', '--config-file', default='config.yml')
    parser.add_argument(
        '-o', '--output-file', default='/dev/stdout',
        help="where to output the result (default: stdout)")
    parser.add_argument(
        '-s', '--schema-file', default='schema-vue.yaml')
    parser.add_argument(
        '-n', '--name', default='ingestion')
    parser.add_argument(
        '-L', '--limit')

    return parser.parse_args(argv)


def get_category_ids(record, taxonomy, taxonomy_fields):
    categories = set()
    for taxonomy_field in taxonomy_fields:
        value = record.get(taxonomy_field)
        if not value:
            continue
        for name in value.split(', '):
            categories.add(name.strip().upper())

    category_ids = set()
    for category_id, category_name in taxonomy.items():
        sanitized_name = category_name.strip().upper()
        if sanitized_name in categories:
            categories.remove(sanitized_name)
            category_ids.add(category_id)

    if categories:
        epprint(f"categories {categories} not in taxonomy {taxonomy.values()}")
    return list(category_ids)


def xform_df_record_pre_json(record, schema_mapping, taxonomy=None, taxonomy_fields=None):
    result = {}
    category_ids = []
    if taxonomy and taxonomy_fields:
        category_ids = get_category_ids(record, taxonomy, taxonomy_fields)
    for key, value in schema_mapping.items():
        result[key] = Template(value).render(record=record, category_ids=category_ids)
    if not any(result.values()):
        return
    return result


def xform_df_pre_json(frame, schema_mapping, taxonomy=None, taxonomy_fields=None):
    return list(filter(
        None,
        [
            xform_df_record_pre_json(row, schema_mapping, taxonomy, taxonomy_fields)
            for row in frame.to_dict('records')
        ]
    ))


def main(args):
    conf = parse_config(args.config_file, args.schema_file)
    epprint(conf)
    gc = authorize_creds(args.creds_file)
    sheet = Sheet(gc, conf['spreadsheet_key'])
    exit_if_no_mod(sheet, args.name)

    aggregate = pd.concat([
        sheet.get_worksheet_df(
            worksheet_spec['name'], conf.get('skip_rows', 0),
            {'__WORKSHEET': worksheet_spec.get('category', worksheet_spec['name'])})
        for worksheet_spec in conf['worksheets']
    ],
                          axis=0)

    if args.limit:
        aggregate = aggregate.head(int(args.limit))

    transformed = xform_df_pre_json(
        aggregate,
        conf['schema_mapping'],
        conf.get('taxonomy'),
        conf.get('taxonomy_fields'),
    )
    if not transformed:
        raise UserWarning("no data")

    with open(args.output_file, 'w') as stream:
        json.dump(transformed, stream)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
