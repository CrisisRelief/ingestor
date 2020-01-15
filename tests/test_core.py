import argparse
import os
import shlex
import sys

import pandas as pd

from . import REPO_ROOT, TEST_DATA

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.core import parse_args, parse_config, xform_df_pre_json
finally:
    sys.path = PATH


class TestCore:
    def test_parse_args(self):
        expected = argparse.Namespace(
            creds_file='foo.json',
            config_file='bar.yml',
            output_file='baz.json',
            limit='10'
        )
        argv = shlex.split('--creds-file foo.json --config-file bar.yml --output-file baz.json --limit 10')

        result = parse_args(argv)

        assert expected == result

    def test_parse_config(self):
        expected = {
            'name': 'foo',
            'spreadsheet_key': '1j2k3l4l-X',
            'worksheets': [
                {'name': 'bar', 'category': 'baz'},
                {'name': 'qux', 'category': 'quux'}
            ],
            'schema_mapping': {
                'corge': 'grault',
                'garply': 'waldo'
            }
        }
        dummy_config_file = os.path.join(TEST_DATA, 'dummy-config.yml')

        result = parse_config(dummy_config_file)

        assert expected == result

    def test_xform_df_row_to_json(self):
        schema_mapping = {
            'corge': 'grault',
            'garply': 'waldo'
        }
        df_in = pd.DataFrame([
            ['a', 'b', 'c'], ['d', 'e', 'f']
        ], columns=['foo', 'waldo', 'grault'])
        expected = [
            {'corge': 'c', 'garply': 'b'},
            {'corge': 'f', 'garply': 'e'},
        ]

        transformed = xform_df_pre_json(df_in, schema_mapping)

        assert expected == transformed

    def test_xform_df_row_to_json_blanks(self):
        schema_mapping = {
            'corge': 'grault',
            'garply': 'waldo'
        }
        df_in = pd.DataFrame([
            ['a', None, None], ['d', 'e', 'f']
        ], columns=['foo', 'waldo', 'grault'])
        expected = [
            {'corge': 'f', 'garply': 'e'},
        ]

        transformed = xform_df_pre_json(df_in, schema_mapping)

        assert expected == transformed
