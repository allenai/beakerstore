import unittest
from beakerstore import path, DatasetNotFoundError, BeakerOptions


class TestBeakerstore(unittest.TestCase):

    def test_directories(self):

        def single_directory(directory, which_beaker):
            self.assertTrue(path(directory, which_beaker=which_beaker).exists())

        # by id
        single_directory('ds_bv0874n13di9',
                         which_beaker=BeakerOptions.INTERNAL)
        # by author and name
        single_directory('chloea/chloea-dedupe-test-ds-2',
                         which_beaker=BeakerOptions.INTERNAL)
        # with trailing /
        single_directory('ds_bv0874n13di9/',
                         which_beaker=BeakerOptions.INTERNAL)

    def test_files(self):

        def single_file(filename, which_beaker):
            self.assertTrue(path(filename, which_beaker=which_beaker).is_file())

        # by id
        single_file('ds_bv0874n13di9/somewords.txt',
                    which_beaker=BeakerOptions.INTERNAL)
        # by author and name
        single_file('chloea/chloea-dedupe-test-ds-2/somewords.txt',
                    which_beaker=BeakerOptions.INTERNAL)

    def test_nonexistent(self):

        def single(name, which_beaker):
            with self.assertRaises(DatasetNotFoundError):
                path(name, which_beaker=which_beaker)

        single('nonexistent',
               which_beaker=BeakerOptions.INTERNAL)
        single('chloea/nonexistent',
               which_beaker=BeakerOptions.INTERNAL)
