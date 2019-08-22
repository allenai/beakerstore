import atexit
import logging
import os
import platform
import requests
import tempfile
import time

from enum import Enum
from pathlib import Path
from random import shuffle
from typing import Optional, Set, Union

from . import __version__

# Logging stuff

logging.basicConfig(format='%(levelname)s %(name)s %(asctime)s  %(message)s', level=logging.INFO)
_logger = logging.getLogger('beakerstore')


# Cleanup stuff
# mostly taken from https://github.com/allenai/datastore

_cleanup_files: Set[Path] = set()


def _cleanup_cleanup_files() -> None:
    global _cleanup_files
    for p in _cleanup_files:
        assert p.is_absolute()   # safety
        p.unlink()
    _cleanup_files = set()


atexit.register(_cleanup_cleanup_files)


def remember_cleanup(p: Union[Path, str]) -> None:
    global _cleanup_files
    if type(p) is str:
        p = Path(p)
    _cleanup_files.add(p.absolute())


def forget_cleanup(p: Union[Path, str]) -> None:
    global _cleanup_files
    if type(p) is str:
        p = Path(p)
    _cleanup_files.remove(p)


class BeakerOptions(Enum):
    INTERNAL = 'internal'
    PUBLIC = 'public'


class Cache:
    def __init__(self, custom_path: Optional[Path] = None):
        self.base_path = Cache._get_default_cache_base() if custom_path is None else custom_path
        if custom_path is not None:
            _logger.info(f'Cache at custom path: {custom_path}')

    @staticmethod
    def _get_default_cache_base() -> Path:

        # close to https://github.com/allenai/datastore

        cache_loc_base = os.environ.get('AI2_BEAKERSTORE_DIR')

        if cache_loc_base is not None:
            cache_loc_base = Path(cache_loc_base)

        else:
            home = Path.home()
            if platform.system() == 'Darwin':
                cache_loc_base = home / 'Library' / 'Caches' / 'beakerstore'
            elif platform.system() == 'Linux':
                cache_loc_base = home / '.ai2' / 'beakerstore'
            else:
                raise ValueError(f'Unsupported platform: {platform.system()}')

        if not cache_loc_base.is_dir():
            cache_loc_base.mkdir(parents=True)

        return cache_loc_base

    def tmp_loc(self) -> Path:
        return self.base_path / 'tmp'

    def cache_base(self) -> Path:
        return self.base_path


class BeakerItem:
    """Corresponds to a dataset or a file within a dataset on Beaker."""
    def __init__(self,
                 is_dir: bool,
                 beaker_info: dict,
                 file_name: Optional[str],
                 which_beaker: BeakerOptions = BeakerOptions.PUBLIC):

        # Note: this corresponds to whether the user wants a whole dataset, or just a file
        # within a dataset. This is different from the Beaker single-file dataset idea.
        self.is_dir = is_dir

        self.beaker_info = beaker_info
        self.file_name = None if file_name == '' else file_name

        assert self.is_dir == (self.file_name is None)

        self.which_beaker = which_beaker

    def dataset_id(self) -> str:
        return self.beaker_info['id']

    def make_directory_manifest_request(self, sess: requests.Session) -> requests.Response:
        url = f'{self._get_file_heap_base_url()}/manifest'
        return self._make_fileheap_request(url, sess)

    def make_one_file_download_request(self, name: str, sess: requests.Session) -> requests.Response:

        # name == self.file_name corresponds to the case where the user specified a file
        # within a dataset. is_dir is False, and this BeakerItem corresponds to one instance
        # of FileCacheEntry.
        # is_dir corresponds to the case where the user specified only a dataset.
        # In addition to being attached to a DirCacheEntry instance, the corresponding BeakerItem
        # will also be attached all the FileCacheEntry instances that correspond to the files
        # within the dataset.
        assert (name == self.file_name) or self.is_dir, \
            'Was expecting a directory BeakerItem or the same filename.'

        url = f'{self._get_file_heap_base_url()}/files/{name}'
        return self._make_fileheap_request(url, sess, stream=True)

    def _get_file_heap_base_url(self) -> str:
        return f'{self._get_storage_address()}/datasets/{self._get_storage_id()}'

    def _make_fileheap_request(self,
                               url: str,
                               sess: requests.Session,
                               stream: bool = False) -> requests.Response:
        headers = {
            'Authorization': f'Bearer {self._get_storage_token()}'
        }

        return sess.get(url, headers=headers, stream=stream)

    def _get_storage_address(self) -> str:
        return self.beaker_info['storage']['address']

    def _get_storage_id(self) -> str:
        return self.beaker_info['storage']['id']

    def _get_storage_token(self) -> str:
        return self.beaker_info['storage']['token']


class CacheEntry:
    """Corresponds to an entry in the cache, already in existence or to be created."""
    def __init__(self, beaker_item: BeakerItem):
        self.beaker_item = beaker_item
        self.cache = None

    def which_beaker(self) -> BeakerOptions:
        return self.beaker_item.which_beaker

    def dataset_id(self) -> str:
        return self.beaker_item.dataset_id()

    def get_cache(self) -> Cache:
        if self.cache is None:
            self.cache = Cache()
        return self.cache

    def set_cache(self, cache: Cache):
        self.cache = cache

    def is_dir(self) -> bool:
        """Does this entry correspond to a dataset?

         The alternative is that this entry corresponds to a file within a dataset.
         Note: this does not correspond to whether this represents a single-file Beaker dataset
         or not.
         """
        raise NotImplementedError()

    def cache_path(self) -> Path:
        """The path to this entry in the cache."""
        return self.cache_base() / self.cache_key()

    def cache_base(self) -> Path:
        """The path to the root of the cache."""
        return self.get_cache().cache_base()

    def cache_key(self) -> str:
        """The key of this entry in the cache.

        This is basically the relative path to this entry from the root of the cache.
        """
        return f'{self.which_beaker().value}/{self.item_name()}'

    def item_name(self) -> str:
        """The name of the item corresponding to this entry.

        This is the dataset id, and the filename (if any). It corresponds to the cache key of this
        entry without the 'public' or 'internal' part.
        """
        raise NotImplementedError()

    def download(self, sess: requests.Session) -> bool:
        """Download the Beaker dataset or file to the corresponding cache location."""
        raise NotImplementedError()

    def _prepare_parent_dir(self):
        parent_dir = self.cache_path().parent
        if not parent_dir.is_dir():
            parent_dir.mkdir(parents=True)

    @staticmethod
    def from_beaker_item(beaker_item: BeakerItem):
        if beaker_item.is_dir:
            return DirCacheEntry(beaker_item)
        else:
            return FileCacheEntry(beaker_item, beaker_item.file_name)


class DirCacheEntry(CacheEntry):
    def __init__(self, beaker_item: BeakerItem):
        super().__init__(beaker_item)

    def is_dir(self):
        return True

    def item_name(self) -> str:
        return self.dataset_id()

    def download(self, sess: requests.Session) -> None:

        dir_res = self.beaker_item.make_directory_manifest_request(sess)

        if not dir_res.status_code == 200:
            raise BeakerstoreError(
                (f'Unable to get the requested directory manifest. '
                 f'Response code: {dir_res.status_code}.'))

        file_names = list(map(lambda f: f['path'], dir_res.json()['files']))
        items_with_details = list(map(lambda file_name: self.dir_to_file(file_name), file_names))

        # not totally necessary but it does mean that if you're running two of this at the same
        # time on the same dataset, they may work on downloading different files (instead of going
        # through the files in the same order, one downloading the current file, the other
        # waiting on the lock)
        shuffle(items_with_details)
        for item in items_with_details:
            item.download(sess)

    def dir_to_file(self, file_name: str):
        """Makes an instance of FileCacheEntry from this instance of DirCacheEntry.

        The resulting entry corresponds to the file with filename 'file_name' within the dataset
        that corresponds to this current entry.
        """
        entry = FileCacheEntry(self.beaker_item, file_name)
        entry.set_cache(self.cache)
        return entry


class FileCacheEntry(CacheEntry):
    def __init__(self, beaker_item: BeakerItem, file_name: str):
        super().__init__(beaker_item)
        self.file_name = file_name

    def is_dir(self):
        return False

    def item_name(self) -> str:
        return f'{self.dataset_id()}/{self.file_name}'

    def already_exists(self) -> bool:
        """Does this entry already exist in the cache?"""
        return self.cache_path().is_file()

    def download(self, sess: requests.Session) -> None:

        if self.already_exists():
            return

        _logger.info(f'Getting {self.file_name} of dataset {self.dataset_id()}.')

        res = self.beaker_item.make_one_file_download_request(self.file_name, sess)

        if not res.status_code == 200:
            raise BeakerstoreError((f'Unable to get the requested file. '
                                    f'Response code: {res.status_code}.'))

        self._prepare_parent_dir()

        lock = CacheLock(self)
        lock.get_lock()

        # If something else downloaded this in the meantime, no need to do it once more.
        if not self.already_exists():
            self._write_file_from_response(res)

        lock.release_lock()

    def _write_file_from_response(self, res: requests.Response) -> None:

        def write_chunks(write_to, chunk_size=1024 * 256):
            for chunk in res.iter_content(chunk_size=chunk_size):
                if chunk:
                    write_to.write(chunk)

        if self.already_exists():
            return

        # prepare the tmp location if necessary
        tmp_dir = self.get_cache().tmp_loc()
        if not tmp_dir.is_dir():
            tmp_dir.mkdir(parents=True)

        # make the file
        tmp_file = tempfile.NamedTemporaryFile(
            dir=tmp_dir,
            prefix=self._tmp_file_prefix(),
            suffix='.tmp',
            delete=False)
        remember_cleanup(tmp_file.name)

        # write to the file
        write_chunks(tmp_file)
        tmp_file.close()

        # put the file in the right place
        Path(tmp_file.name).rename(self.cache_path())
        forget_cleanup(tmp_file.name)

    def _tmp_file_prefix(self) -> str:
        no_subdirs = self.cache_key().replace('/', '%')
        return f'ai2-beakerstore-{no_subdirs}'


class CacheLock:
    def __init__(self, cache_entry: CacheEntry):
        self.lock_loc = Path(f'{cache_entry.cache_path()}.lock')
        self.item_name = cache_entry.item_name()

    def _wait_for_lock(self) -> None:

        if not self.lock_loc.is_file():
            return

        start = time.time()
        _logger.info(f'Waiting for the lock for {self.item_name}.')
        last_message_time = start

        while self.lock_loc.is_file():
            if time.time() - last_message_time > 60:
                now = time.time()
                _logger.info(f'Still waiting for the lock. It\'s been {now - start} seconds.')
                last_message_time = now
            time.sleep(1)

    def get_lock(self) -> None:
        self._wait_for_lock()
        try:
            self.lock_loc.touch(mode=0o644, exist_ok=False)
            remember_cleanup(self.lock_loc)
        except FileExistsError:
            self.get_lock()

    def release_lock(self) -> None:
        self.lock_loc.unlink()
        forget_cleanup(self.lock_loc)


class ItemRequest:
    def __init__(self,
                 given_path: str,
                 which_beaker: BeakerOptions = BeakerOptions.PUBLIC):
        self.given_path = given_path
        self.which_beaker = which_beaker

    def to_beaker_item(self, sess: requests.Session) -> BeakerItem:

        try:
            # this expects a format like: ds_abc
            return self._get_dataset_details_helper(self._path_to_dataset_id(), sess)

        except DatasetNotFoundError as e_id:

            if len(self.given_path.split('/')) > 1:

                try:
                    # we could have been given a dataset in this format: chloea/my-dataset.
                    # Try that.
                    return self._get_dataset_details_helper(self._path_to_author_and_name(), sess)

                except DatasetNotFoundError as e_author_and_name:
                    raise DatasetNotFoundError(f'{e_id}\n{e_author_and_name}')
            else:
                raise e_id

    def _path_to_dataset_id(self) -> str:
        return self.given_path.split('/')[0]

    def _path_to_author_and_name(self) -> str:
        split = self.given_path.split('/')
        assert len(split) > 1
        return '/'.join(split[:2])

    def _get_dataset_details_helper(self,
                                    possible_identifier: str,
                                    sess: requests.Session) -> BeakerItem:

        res = sess.get(self._get_beaker_dataset_url(possible_identifier))

        if res.status_code == 200:
            beaker_info = res.json()

            # add 1 to get past the '/'
            file_path = self.given_path[len(possible_identifier) + 1:]

            is_dir = file_path == ''
            return BeakerItem(is_dir, beaker_info, file_path, which_beaker=self.which_beaker)

        elif res.status_code == 404:
            raise DatasetNotFoundError(f'Could not find dataset \'{possible_identifier}\'.')

        else:
            raise BeakerstoreError(
                (f'Encountered a problem when trying to find dataset \'{possible_identifier}\'. '
                 f'Response status code: {res.status_code}.'))

    def _get_beaker_dataset_url(self, identifier: str) -> str:

        beaker_prefix = 'allenai.' if self.which_beaker == BeakerOptions.INTERNAL else ''
        beaker_base = f'https://{beaker_prefix}beaker.org'

        return f'{beaker_base}/api/v3/datasets/{identifier}'


# some exceptions

class DatasetNotFoundError(Exception):
    pass


class BeakerstoreError(Exception):
    pass


# the central function

def path(given_path: str,
         which_beaker: BeakerOptions = BeakerOptions.PUBLIC,
         cache: Optional[Cache] = None) -> Path:

    sess = requests.Session()
    sess.headers.update({'User-Agent': f'beakerstore/{__version__}'})

    item_request = ItemRequest(given_path, which_beaker)
    beaker_item = item_request.to_beaker_item(sess)
    cache_entry = CacheEntry.from_beaker_item(beaker_item)
    if cache is not None:
        cache_entry.set_cache(cache)
    cache_entry.download(sess)
    return cache_entry.cache_path()
