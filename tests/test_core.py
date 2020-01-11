import argparse
import os
import shlex
import sys

from . import REPO_ROOT, TEST_DATA

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.core import parse_args, parse_config
finally:
    sys.path = PATH


class TestCore:
    def test_parse_args(self):
        expected = argparse.Namespace(
            creds_file='foo.json',
            config_file='bar.yml',
            output_file='baz.json'
        )
        argv = shlex.split('--creds-file foo.json --config-file bar.yml --output-file baz.json')

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
