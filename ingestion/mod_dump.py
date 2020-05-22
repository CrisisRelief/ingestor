import os
import re
import sys
from sys import stderr


def get_mod_file(name):
    name = re.sub(r'\W', '_', name)
    return '.%s.last_mod' % name


def get_last_mod_time(name):
    mod_file = get_mod_file(name)
    result = None
    if os.path.isfile(mod_file):
        with open(mod_file) as stream:
            result = stream.readline().rstrip('\n')
    print(f"last mod str: {result} ({mod_file})", file=stderr)
    return result


def dump_this_mod_time(name, modtime_str):
    mod_file = get_mod_file(name)
    with open(mod_file, 'w+') as stream:
        stream.writelines([modtime_str])


def mod_since_last_run(name, current_mod_str):
    print(f"current mod str: {current_mod_str}", file=stderr)
    return current_mod_str != get_last_mod_time(name)


def exit_if_no_mod(name, current_mod_str):
    modified = mod_since_last_run(name, current_mod_str)
    dump_this_mod_time(name, current_mod_str)
    if not modified:
        print("exiting", file=stderr)
        sys.exit()
