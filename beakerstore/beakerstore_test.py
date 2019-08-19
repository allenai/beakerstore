import pytest
import unittest

from pathlib import Path

from beakerstore import path, DatasetNotFoundError, BeakerOptions, Cache


@pytest.fixture(scope='class')
def cache_test_dir(request, tmpdir_factory):
    request.cls.tmpdir = tmpdir_factory.mktemp('cache_test_dir')


@pytest.mark.usefixtures('cache_test_dir')
class TestBeakerstore(unittest.TestCase):

    def test_directories(self):

        test_cache = Cache(Path(str(self.tmpdir)))

        def single_directory(directory, which_beaker):
            self.assertTrue(path(directory, which_beaker=which_beaker, cache=test_cache).exists())

        # by id
        single_directory('ds_bv0874n13di9',
                         which_beaker=BeakerOptions.INTERNAL)
        # by author and name
        single_directory('chloea/chloea-dedupe-test-ds-2',
                         which_beaker=BeakerOptions.INTERNAL)
        # with trailing /
        single_directory('ds_bv0874n13di9/',
                         which_beaker=BeakerOptions.INTERNAL)

        # larger file
        single_directory('ds_0tssnayto2v2',
                         which_beaker=BeakerOptions.INTERNAL)

        # even larger file
        single_directory('ds_ujlofrenubm0',
                         which_beaker=BeakerOptions.INTERNAL)

        single_directory('ds_jq5fmdtd46zf',
                         which_beaker=BeakerOptions.PUBLIC)

    def test_files(self):

        test_cache = Cache(Path(str(self.tmpdir)))

        def single_file(filename, which_beaker):
            self.assertTrue(path(filename, which_beaker=which_beaker, cache=test_cache).is_file())

        # by id
        single_file('ds_bv0874n13di9/somewords.txt',
                    which_beaker=BeakerOptions.INTERNAL)
        # by author and name
        single_file('chloea/chloea-dedupe-test-ds-2/somewords.txt',
                    which_beaker=BeakerOptions.INTERNAL)

        single_file('ds_fkjp39g5zmul/vocabulary/tokens.txt',
                    which_beaker=BeakerOptions.INTERNAL)

        single_file('ds_mhvfq8ykcln2/input-2.jsonl',
                    which_beaker=BeakerOptions.PUBLIC)

    def test_nonexistent(self):

        test_cache = Cache(Path(str(self.tmpdir)))

        def single(name, which_beaker):
            with self.assertRaises(DatasetNotFoundError):
                path(name, which_beaker=which_beaker, cache=test_cache)

        single('nonexistent',
               which_beaker=BeakerOptions.INTERNAL)
        single('chloea/nonexistent',
               which_beaker=BeakerOptions.INTERNAL)
