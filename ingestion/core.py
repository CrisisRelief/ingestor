import argparse
import csv
import json
import sys
from os.path import exists as path_exists
from datetime import datetime
from pprint import pformat

import pandas as pd
import yaml
from jinja2 import Template

from .drupal import Drupal
from .mod_dump import exit_if_no_mod, get_last_mod_time
from .sheet import Sheet, authorize_creds


def epprint(*args, **kwargs):
    print(pformat(*args, **kwargs), file=sys.stderr)


def parse_config(config_file, schema_file, taxonomy_file):
    with open(config_file) as stream:
        conf = yaml.safe_load(stream)

    with open(schema_file) as stream:
        conf['schema_mapping'] = yaml.safe_load(stream)
    with open(taxonomy_file) as stream:
        conf['taxonomy'] = yaml.safe_load(stream)
    return conf


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-C', '--creds-file', default='credentials.json')
    parser.add_argument(
        '-c', '--config-file', default='config.yml')
    parser.add_argument(
        '-s', '--schema-file', default='schema-gsheet-vue.yml')
    parser.add_argument(
        '-t', '--taxonomy-file', default='taxonomy-drupal.yml')
    parser.add_argument(
        '-S', '--input-source', default='gsheet'
    )
    parser.add_argument(
        '-a', '--append-output', default=False, action='store_true',
        help="append to existing output file, updating records based on primary_key_field"
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


def xform_df_record_pre_output(record,
                               schema_mapping,
                               taxonomy_names=None,
                               taxonomy_fields=None,
                               xform_extras=None):
    result = {}
    category_ids = []
    if taxonomy_names and taxonomy_fields:
        category_ids = get_category_ids(record, taxonomy_names, taxonomy_fields)
    if xform_extras:
        for xform_extra in xform_extras:
            record = xform_extra(record)
    for key, value in schema_mapping.items():
        result[key] = Template(value).render(record=record, category_ids=category_ids)
    if not any(result.values()):
        return None
    return result


def xform_df_pre_output(frame,
                        schema_mapping,
                        taxonomy_names=None,
                        taxonomy_fields=None,
                        xform_extras=None):
    return list(
        filter(None, [
            xform_df_record_pre_output(row, schema_mapping, taxonomy_names, taxonomy_fields,
                                       xform_extras) for row in frame.to_dict('records')
        ]))


def get_df_gsheets(conf, creds_file, name):
    gcs = authorize_creds(creds_file)
    sheet = Sheet(gcs, conf['spreadsheet_key'])
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

    # extract a mapping of `attributes.drupal_internal__sid` to `attributes.changed` of the entries
    sid_modtimes = {
        item['attributes']['drupal_internal__sid']:
            datetime.fromisoformat(item['attributes']['changed'])
        for item in entry_meta
    }

    # get the most recent mod_date of entries
    last_mod_str = get_last_mod_time(name)
    last_modtime = datetime.fromisoformat(last_mod_str) if last_mod_str else None
    latest_modtime = max(sid_modtimes.values())
    exit_if_no_mod(name, latest_modtime.isoformat())

    # extract all entries with mod_time greater than the last time this importer ran

    if last_modtime:
        new_entries = [
            sid for sid, modtime in sid_modtimes.items() if modtime > last_modtime
        ]
    else:
        new_entries = sid_modtimes.keys()
    return drupal.get_form_entries_df(conf['form_id'], new_entries)


def _process_additions(taxonomy):
    additions = {}
    unprocessed = taxonomy.copy()
    while unprocessed:
        taxonomy_term = unprocessed.pop()
        taxonomy_id, taxonomy_parents = taxonomy_term['id'], taxonomy_term['parents']
        if not taxonomy_parents:
            additions[taxonomy_id] = {'category': taxonomy_term['name']}
            continue
        for parent_id in taxonomy_parents:
            if parent_id in additions:
                additions[taxonomy_id] = additions[parent_id].copy()
                depth = len(additions[taxonomy_id])
                key = 'category' + '_sub' * depth
                additions[taxonomy_id][key] = taxonomy_term['name']
                break
        if taxonomy_id not in additions:
            unprocessed.insert(0, taxonomy_term)
    return additions


def xform_cats_drupal_taxonomy(taxonomy, taxonomy_ids_field):
    additions = _process_additions(taxonomy)

    def xform_cats_drupal(record):
        taxonomy_ids = record[taxonomy_ids_field]
        updates = {}
        for taxonomy_id in taxonomy_ids:
            for key, value in additions[int(taxonomy_id)].items():
                if key in updates:
                    updates[key].add(value)
                else:
                    updates[key] = {value}
        updates = {
            key: ', '.join(value)
            for key, value in updates.items()
        }
        record.update(updates)
        return record

    return xform_cats_drupal


def write_output(transformed, out_file, out_fmt, fieldnames=None):
    with open(out_file, 'w') as stream:
        if out_fmt == 'json':
            json.dump(transformed, stream)
        elif out_fmt == 'csv':
            assert fieldnames, "fieldnames must be provided for CSV format"
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transformed)
        else:
            raise UserWarning(f'unknown output format: {out_fmt}')
    print(f"wrote {len(transformed)} records", file=sys.stderr)


def read_output(out_file, out_fmt):
    result = []
    if path_exists(out_file):
        with open(out_file) as stream:
            if out_fmt == 'json':
                result = json.load(stream)
            elif out_fmt == 'csv':
                result = csv.DictReader(stream)
            else:
                raise UserWarning(f'unknown output format: {out_fmt}')
    return result


def append_output(transformed, out_file, out_fmt, primary_key_field, fieldnames=None):
    existing_data = read_output(out_file, out_fmt)
    primary_key_map = {
        record[primary_key_field]: record
        for record in existing_data
    }
    for record in transformed:
        primary_key_map[record[primary_key_field]] = record
    write_output(list(primary_key_map.values()), out_file, out_fmt, fieldnames)


def main(args):
    epprint(vars(args))
    conf = parse_config(args.config_file, args.schema_file, args.taxonomy_file)
    epprint(conf)

    xform_kwargs = {}
    if args.input_source == 'gsheet':
        aggregate = get_df_gsheets(conf, args.creds_file, args.name)
        xform_kwargs.update(
            taxonomy_fields=conf.get('taxonomy_fields'),
            taxonomy_names={
                taxonomy_term['id']: taxonomy_term['name']
                for taxonomy_term in conf.get('taxonomy')
            }
        )
    elif args.input_source == 'drupal':
        aggregate = get_df_drupal(conf, args.creds_file, args.name)
        xform_kwargs.update(xform_extras=[
            xform_cats_drupal_taxonomy(conf.get('taxonomy'), conf.get('taxonomy_ids_field'))
        ])

    if args.limit:
        aggregate = aggregate.head(int(args.limit))

    transformed = xform_df_pre_output(
        aggregate,
        conf['schema_mapping'],
        **xform_kwargs
    )
    if not transformed:
        print("no data", file=sys.stderr)
        sys.exit(0)

    if args.append_output:
        append_output(
            transformed, args.output_file, args.output_format,
            primary_key_field=conf['primary_key_field'],
            fieldnames=conf['schema_mapping'].keys()
        )
    else:
        write_output(
            transformed, args.output_file, args.output_format,
            fieldnames=conf['schema_mapping'].keys(),
        )


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
