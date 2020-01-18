import sys
import os
from .core import parse_config, parse_args
from .sheet import Sheet, authorize_creds


MOD_FILE_FMT = '.%s.last_mod'


def get_last_mod_time(name):
    mod_file = MOD_FILE_FMT % name
    if os.path.isfile(mod_file):
        with open(mod_file) as stream:
            return stream.readline().rstrip('\n')


def dump_this_mod_time(sheet, name):
    mod_file = MOD_FILE_FMT % name
    with open(mod_file, 'w+') as stream:
        stream.writelines([sheet.modtime_str])


def mod_since_last_run(sheet, name):
    return sheet.modtime_str != get_last_mod_time(name)


def main(args):
    conf = parse_config(args.config_file)
    gc = authorize_creds(args.creds_file)
    sheet = Sheet(gc, conf['spreadsheet_key'])
    print(sheet.modtime_str)


if __name__ == "__main__":
    main(parse_args(sys.argv[1:]))
