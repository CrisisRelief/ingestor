import sys
from .core import parse_config, parse_args
from .sheet import Sheet, authorize_creds


def main(args):
    conf = parse_config(args.config_file)
    gc = authorize_creds(args.creds_file)
    sheet = Sheet(gc, conf['spreadsheet_key'])
    print(sheet.modtime_str)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
