import os


MOD_FILE_FMT = '.%s.last_mod'


def get_last_mod_time(name):
    mod_file = MOD_FILE_FMT % name
    if os.path.isfile(mod_file):
        with open(mod_file) as stream:
            return stream.readline().rstrip('\n')


def dump_this_mod_time(name, modtime_str):
    mod_file = MOD_FILE_FMT % name
    with open(mod_file, 'w+') as stream:
        stream.writelines([modtime_str])


def mod_since_last_run(name, current_mod_str):
    print("current mod str", current_mod_str)
    last_mod_str = get_last_mod_time(name)
    print("last mod str", last_mod_str)
    return current_mod_str != get_last_mod_time(name)


def exit_if_no_mod(name, current_mod_str):
    modified = mod_since_last_run(name, current_mod_str)
    dump_this_mod_time(name, current_mod_str)
    if not modified:
        print("exiting")
        exit
