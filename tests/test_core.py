import argparse
import os
import shlex
import sys

import pandas as pd

from . import REPO_ROOT, TEST_DATA

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.core import parse_args, parse_config, xform_df_pre_json, get_category_ids
finally:
    sys.path = PATH


class TestCore:
    def test_parse_args(self):
        # Given
        expected = argparse.Namespace(
            creds_file='tests/data/dummy-credentials.json',
            config_file='tests/data/dummy-config.yml',
            schema_file='tests/data/dummy-schema.yml',
            output_file='tests/data/dummy-output.json',
        )
        argv = shlex.split(
            '--creds-file tests/data/dummy-credentials.json'
            ' --config-file tests/data/dummy-config.yml'
            ' --schema-file tests/data/dummy-schema.yml'
            ' --output-file tests/data/dummy-output.json'
        )

        # When
        result = parse_args(argv)

        # Then
        for key, value in vars(expected).items():
            assert vars(result)[key] == value

    def test_parse_args_default_stdout(self):
        # Given
        expected = argparse.Namespace(
            creds_file='tests/data/dummy-credentials.json',
            config_file='tests/data/dummy-config.yml',
            schema_file='tests/data/dummy-schema.yml',
            output_file='/dev/stdout',
        )
        argv = shlex.split(
            '--creds-file tests/data/dummy-credentials.json'
            ' --config-file tests/data/dummy-config.yml'
            ' --schema-file tests/data/dummy-schema.yml'
        )

        # When
        result = vars(parse_args(argv))

        # Then
        for key, value in vars(expected).items():
            assert result[key] == value

    def test_parse_config(self):
        # Given
        expected = {
            'name': 'foo',
            'spreadsheet_key': '1j2k3l4l-X',
            'worksheets': [
                {'name': 'bar', 'category': 'baz'},
                {'name': 'qux', 'category': 'quux'}
            ],
            'schema_mapping': {
                'corge': "{{ record['grault'] or '' }}",
                'garply': "{{ record['waldo'] or '' }}"
            },
            'taxonomy': {
                1: "corge",
                68: "grault",
                70: "fred",
            },
            'taxonomy_fields': [
                'garply', 'waldo'
            ]
        }
        dummy_config_file = os.path.join(TEST_DATA, 'dummy-config.yml')
        dummy_schema_file = os.path.join(TEST_DATA, 'dummy-schema.yml')

        # When
        result = parse_config(dummy_config_file, dummy_schema_file)

        # Then
        assert expected == result

    def test_get_category_ids(self):
        # Given
        record = {
            'foo': 'd',
            'garply': 'corge, fred',
            'waldo': ''
        }
        taxonomies = {
            1: "corge",
            68: "grault",
            70: "fred",
        }
        taxonomy_fields = [
            'garply', 'waldo'
        ]
        expected = [1, 70]

        # When
        result = get_category_ids(record, taxonomies, taxonomy_fields)

        # Then
        assert sorted(expected) == sorted(result)

    def test_xform_df_row_to_json(self):
        # Given
        schema_mapping = {
            'corge': "{{ record['grault'] or '' }}",
            'garply': "{{ record['waldo'] or '' }}"
        }
        df_in = pd.DataFrame([
            ['a', 'b', 'c'], ['d', 'e', 'f']
        ], columns=['foo', 'waldo', 'grault'])
        expected = [
            {'corge': 'c', 'garply': 'b'},
            {'corge': 'f', 'garply': 'e'},
        ]

        # When
        transformed = xform_df_pre_json(df_in, schema_mapping)

        # Then
        assert expected == transformed

    def test_xform_df_row_to_json_blanks(self):
        # Given
        schema_mapping = {
            'corge': "{{ record['grault'] or '' }}",
            'garply': "{{ record['waldo'] or '' }}"
        }
        df_in = pd.DataFrame([
            ['a', None, None], ['d', 'e', 'f']
        ], columns=['foo', 'waldo', 'grault'])
        expected = [
            {'corge': 'f', 'garply': 'e'},
        ]

        # When
        transformed = xform_df_pre_json(df_in, schema_mapping)

        # Then
        assert expected == transformed

    def test_xform_df_row_to_json_taxonomy(self):
        # Given
        schema_mapping = {
            'corge': "{{ record['grault'] or '' }}",
            'garply': "{{ record['waldo'] or '' }}",
            'cats': "{{ category_ids | join(', ') }}",
        }
        df_in = pd.DataFrame([
            ['a', 'b', None, 'fred, grault'],
            ['d', 'e', 'f', 'corge, FRED ']
        ], columns=['foo', 'waldo', 'grault', 'plugh'])
        taxonomies = {
            1: "corge",
            68: "grault",
            70: "fred",
        }
        taxonomy_fields = [
            'plugh',
        ]
        expected = [
            {'corge': '', 'garply': 'b', 'cats': '68, 70'},
            {'corge': 'f', 'garply': 'e', 'cats': '1, 70'},
        ]

        # When
        transformed = xform_df_pre_json(df_in, schema_mapping, taxonomies, taxonomy_fields)

        # Then
        assert expected == transformed
