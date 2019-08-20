import pytest
import unittest

from pathlib import Path

from ..beakerstore import path, DatasetNotFoundError, BeakerOptions, Cache


@pytest.fixture(scope='class')
def cache_test_dir(request, tmpdir_factory):
    request.cls.tmpdir = tmpdir_factory.mktemp('cache_test_dir')


class TestBeakerstore(unittest.TestCase):

    def single_directory_helper(self, directory, which_beaker, test_cache):
        self.assertTrue(path(directory, which_beaker=which_beaker, cache=test_cache).exists())

    def single_file_helper(self, filename, which_beaker, test_cache):
        self.assertTrue(path(filename, which_beaker=which_beaker, cache=test_cache).is_file())

    def nonexistent_helper(self, name, which_beaker, test_cache):
        with self.assertRaises(DatasetNotFoundError):
            path(name, which_beaker=which_beaker, cache=test_cache)


@pytest.mark.usefixtures('cache_test_dir')
class TestBeakerStorePublic(TestBeakerstore):

    def test_directories(self):
        test_cache = Cache(Path(str(self.tmpdir)))

        # moby (around 1.2MiB)
        # by id
        self.single_directory_helper('ds_1hz9k6sgxi0a', which_beaker=BeakerOptions.PUBLIC,
                                     test_cache=test_cache)

        # by author and name
        self.single_directory_helper('examples/moby', which_beaker=BeakerOptions.PUBLIC,
                                     test_cache=test_cache)

        # a larger dataset (around 182.5MiB)
        self.single_directory_helper('ds_jq5fmdtd46zf', which_beaker=BeakerOptions.PUBLIC,
                                     test_cache=test_cache)

    def test_file(self):
        test_cache = Cache(Path(str(self.tmpdir)))

        # word count (around 18B)
        # by id
        self.single_file_helper('ds_7ap4sx03m63n/words.txt', which_beaker=BeakerOptions.PUBLIC,
                                test_cache=test_cache)

        # by author and name
        self.single_file_helper('lane/wordcount/words.txt', which_beaker=BeakerOptions.PUBLIC,
                                test_cache=test_cache)

    def test_nonexistent(self):
        test_cache = Cache(Path(str(self.tmpdir)))

        self.nonexistent_helper('nonexistent', which_beaker=BeakerOptions.PUBLIC,
                                test_cache=test_cache)
        self.nonexistent_helper('chloea/nonexistent', which_beaker=BeakerOptions.PUBLIC,
                                test_cache=test_cache)


@pytest.mark.usefixtures('cache_test_dir')
class TestBeakerStoreInternal(TestBeakerstore):

    @pytest.mark.internal
    def test_directories(self):
        test_cache = Cache(Path(str(self.tmpdir)))

        # by id
        self.single_directory_helper('ds_bv0874n13di9',
                                     which_beaker=BeakerOptions.INTERNAL, test_cache=test_cache)
        # by author and name
        self.single_directory_helper('chloea/chloea-dedupe-test-ds-2',
                                     which_beaker=BeakerOptions.INTERNAL, test_cache=test_cache)
        # larger file
        self.single_directory_helper('ds_0tssnayto2v2',
                                     which_beaker=BeakerOptions.INTERNAL, test_cache=test_cache)

    @pytest.mark.internal
    def test_file(self):
        test_cache = Cache(Path(str(self.tmpdir)))

        # by id
        self.single_file_helper('ds_bv0874n13di9/somewords.txt',
                                which_beaker=BeakerOptions.INTERNAL, test_cache=test_cache)
        # by author and name
        self.single_file_helper('chloea/chloea-dedupe-test-ds-2/somewords.txt',
                                which_beaker=BeakerOptions.INTERNAL, test_cache=test_cache)

    @pytest.mark.internal
    def test_nonexistent(self):
        test_cache = Cache(Path(str(self.tmpdir)))

        self.nonexistent_helper('nonexistent', which_beaker=BeakerOptions.INTERNAL,
                                test_cache=test_cache)
        self.nonexistent_helper('chloea/nonexistent', which_beaker=BeakerOptions.INTERNAL,
                                test_cache=test_cache)
