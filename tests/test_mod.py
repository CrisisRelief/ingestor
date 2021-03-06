import os
import shutil
import sys
import tempfile

from . import REPO_ROOT, TEST_DATA
from .helpers import ChDir

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.mod_dump import get_last_mod_time, dump_this_mod_time, mod_since_last_run
finally:
    sys.path = PATH


class TestMod:
    ingestion_name = 'foo'
    mod_str = '2020-01-14T20:37:29.472Z'
    mod_str_old = '2020-01-13T20:37:29.472Z'
    mod_str_new = '2020-01-15T20:37:29.472Z'

    def test_get_last_mod_time(self):
        modfile_src = os.path.join(TEST_DATA, 'last_mod')
        with \
                tempfile.TemporaryDirectory() as tempdir, \
                ChDir(tempdir):
            modfile_dst = os.path.join(tempdir, f'.{self.ingestion_name}.last_mod')
            shutil.copy(modfile_src, modfile_dst)

            result = get_last_mod_time(self.ingestion_name)

        assert result == self.mod_str

    def test_get_dump_mod_time(self):
        with \
                tempfile.TemporaryDirectory() as tempdir, \
                ChDir(tempdir):
            dump_this_mod_time(self.ingestion_name, self.mod_str)
            result = get_last_mod_time(self.ingestion_name)

        assert result == self.mod_str

    def test_mod_since_last_run_happy(self):
        with \
                tempfile.TemporaryDirectory() as tempdir, \
                ChDir(tempdir):
            dump_this_mod_time(self.ingestion_name, self.mod_str_old)
            assert mod_since_last_run(self.ingestion_name, self.mod_str_new)

    def test_mod_since_last_run_sad(self):
        with \
                tempfile.TemporaryDirectory() as tempdir, \
                ChDir(tempdir):
            dump_this_mod_time(self.ingestion_name, self.mod_str)
            assert not mod_since_last_run(self.ingestion_name, self.mod_str)
