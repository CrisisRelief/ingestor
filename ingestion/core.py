import argparse
import json
import sys
from pprint import pformat
import csv

import pandas as pd
from jinja2 import Template
import yaml

from .sheet import Sheet, authorize_creds
from .mod_dump import exit_if_no_mod
from .drupal import Drupal


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
        '-s', '--schema-file', default='schema-vue.yaml')
    parser.add_argument(
        '-S', '--input-source', default='gsheet'
    )
    parser.add_argument(
        '-o', '--output-file', default='/dev/stdout',
        help="where to output the result (default: stdout)")
    parser.add_argument(
        '-f', '--output-format', default='json'
    )
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


# TODO: rename pre_json -> pre_output
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


# TODO: rename pre_json -> pre_output
def xform_df_pre_json(frame, schema_mapping, taxonomy=None, taxonomy_fields=None):
    return list(filter(
        None,
        [
            xform_df_record_pre_json(row, schema_mapping, taxonomy, taxonomy_fields)
            for row in frame.to_dict('records')
        ]
    ))


def get_df_gsheets(conf, creds_file, name):
    gc = authorize_creds(creds_file)
    sheet = Sheet(gc, conf['spreadsheet_key'])
    exit_if_no_mod(name, sheet.modtime_str)
    skip_rows = conf.get('skip_rows', 0)
    return pd.concat([
        sheet.get_worksheet_df(
            worksheet_spec['name'], skip_rows,
            {'__WORKSHEET': worksheet_spec.get('category', worksheet_spec['name'])})
        for worksheet_spec in conf['worksheets']
    ],
                          axis=0)


def get_df_drupal(conf, creds_file, name):
    with open(creds_file) as stream:
        creds = yaml.safe_load(stream)
    drupal = Drupal(
        base_url=conf['drupal_base_url'],
        username=creds['username'],
        password=creds['password']
    )
    entry_meta = drupal.get_form_entry_meta(conf['form_id'])

    # TODO: extract a mapping of `attributes.drupal_internal__sid` to `attributes.changed` of the entries
    # e.g.
    sid_modtimes = {
        86: "2020-04-08T04:53:07+00:00",
        108: "2020-04-09T06:05:16+00:00",
        109: "2020-04-09T07:23:11+00:00",
        187: "2020-04-28T02:48:07+00:00",
    }

    # get the most recent mod_date of entries
    modtime_str = max(sid_modtimes.values())
    exit_if_no_mod(name, modtime_str)

    # TODO: extract all entries with mod_time greater than the last time this importer ran
    # e.g.
    new_entries = [187, 109, 108]

    return drupal.get_form_entries_df(conf, new_entries)


def main(args):
    conf = parse_config(args.config_file, args.schema_file)
    epprint(conf)

    xform_kwargs = {}
    if args.input_source == 'gsheet':
        aggregate = get_df_gsheets(conf, args.creds_file, args.name)
        xform_kwargs = {
            'taxonomy': conf.get('taxonomy'),
            'taxonomy_fields': conf.get('taxonomy_fields')
        }
    elif args.input_source == 'drupal':
        aggregate = get_df_drupal(conf, args.creds_file, args.name)

    if args.limit:
        aggregate = aggregate.head(int(args.limit))

    transformed = xform_df_pre_json(
        aggregate,
        conf['schema_mapping'],
        **xform_kwargs
    )
    if not transformed:
        raise UserWarning("no data")

    if args.output_format == 'json':
        with open(args.output_file, 'w') as stream:
            json.dump(transformed, stream)
    elif args.output_format == 'csv':
        with open(args.output_file, 'w') as stream:
            writer = csv.DictWriter(stream, fieldnames=conf['schema_mapping'].keys())
            writer.writeheader()
            writer.writerows(transformed)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
