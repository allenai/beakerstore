import unittest
from beakerstore import path, DatasetNotFoundError


class TestBeakerstore(unittest.TestCase):

    def test_directories(self):

        def single_directory(directory, public):
            self.assertTrue(path(directory, public=public).exists())

        # by id
        single_directory('ds_bv0874n13di9', public=False)
        # by author and name
        single_directory('chloea/chloea-dedupe-test-ds-2', public=False)
        # with trailing /
        single_directory('ds_bv0874n13di9/', public=False)

    def test_files(self):

        def single_file(filename, public):
            self.assertTrue(path(filename, public=public).is_file())

        # by id
        single_file('ds_bv0874n13di9/somewords.txt', public=False)
        # by author and name
        single_file('chloea/chloea-dedupe-test-ds-2/somewords.txt', public=False)

    def test_nonexistent(self):

        def single(name, public):
            with self.assertRaises(DatasetNotFoundError):
                path(name, public=public)

        single('nonexistent', public=False)
        single('chloea/nonexistent', public=False)
