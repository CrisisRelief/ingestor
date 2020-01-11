import argparse
import sys

import yaml

from .sheet import Sheet, authorize_creds


def parse_config(config_file):
    with open(config_file) as stream:
        conf = yaml.safe_load(stream)
    return conf


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--creds-file', default='credentials.json')
    parser.add_argument('--config-file', default='config.yml')
    parser.add_argument('--output-file', default='output.json')

    return parser.parse_args(argv)


def main(args):
    conf = parse_config(args.config_file)

    gc = authorize_creds(args.creds_file)

    sheet = Sheet(gc, conf['spreadsheet_key'])
    for worksheet_spec in conf['worksheets']:
        frame = sheet.get_worksheet_df(worksheet_spec['name'], 6)
        __import__('pdb').set_trace()
        print(frame)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
