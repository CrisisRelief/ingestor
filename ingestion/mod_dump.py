import os


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


def exit_if_no_mod(sheet, name):
    modified = mod_since_last_run(sheet, name)
    dump_this_mod_time(sheet, name)
    if not modified:
        exit
