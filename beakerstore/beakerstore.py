import json
import os
import platform
import tempfile
import time
import urllib.request

from collections import namedtuple
from enum import Enum
from pathlib import Path
from typing import Optional, NewType

from urllib.error import HTTPError


class BeakerOptions(Enum):
    INTERNAL = 'internal'
    PUBLIC = 'public'


BEAKER_URLS = {
    BeakerOptions.INTERNAL: 'https://allenai.beaker.org',
    BeakerOptions.PUBLIC: 'https://beaker.org'
}

# represents the dataset info we get back from beaker
BeakerInfo = NewType('BeakerInfo', dict)


class CacheItem:
    def __init__(self, dataset_id, is_dir, file_name, which_beaker: BeakerOptions):
        self.dataset_id: str = dataset_id
        self.is_dir: bool = is_dir
        self.file_name: Optional[str] = None if file_name == '' else file_name

        assert self.is_dir == (self.file_name is None)

        self.which_beaker: BeakerOptions = which_beaker

    def cache_key(self) -> str:
        subdir = f'{self.which_beaker.value}/{self.dataset_id}'
        if self.is_dir:
            return subdir
        return f'{subdir}/{self.file_name}'

    def item_cache_loc(self) -> Path:
        return _get_local_cache_base() / self.cache_key()

    def already_exists(self) -> bool:
        if self.is_dir:
            return self.item_cache_loc().is_dir()
        return self.item_cache_loc().is_file()

    def dir_to_file(self, file_name: str):
        assert self.is_dir, 'Expected a directory CacheItem. Got a file CacheItem.'
        return CacheItem(self.dataset_id, False, file_name, self.which_beaker)

    def make_cache_entry_from_existing(self, existing: Path) -> None:
        self._prepare_parent_dir()
        existing.rename(self.item_cache_loc())

    def make_cache_entry_from_response(self, res) -> None:
        assert not self.is_dir, 'Expected a file CacheItem. Got a directory CacheItem.'

        if self.already_exists():
            return

        self._prepare_parent_dir()

        lock = CacheLock(self)
        lock.get_lock()

        # If something else downloaded this in the meantime, no need to do it once more.
        if not self.already_exists():
            self._write_file(res)

        lock.release_lock()

    def _prepare_parent_dir(self):
        parent_dir = self.item_cache_loc().parent
        if not parent_dir.is_dir():
            parent_dir.mkdir(parents=True)

    def _write_file(self, res) -> None:
        assert not self.is_dir, 'Expected a file CacheItem. Got a directory CacheItem.'

        if self.already_exists():
            return

        # prepare the tmp location if necessary
        tmp_dir = _get_tmp_loc()
        if not tmp_dir.is_dir():
            tmp_dir.mkdir(parents=True)

        # make the file
        tmp_file = tempfile.NamedTemporaryFile(
            dir=tmp_dir,
            prefix=self._tmp_file_prefix(),
            suffix='.tmp',
            delete=False)

        # write to the file
        # TODO: read in chunks
        tmp_file.write(res.read())
        tmp_file.close()

        # put the file in the right place
        self.make_cache_entry_from_existing(Path(tmp_file.name))

    def _tmp_file_prefix(self) -> str:
        assert not self.is_dir, 'Expected a file CacheItem. Got a directory CacheItem.'
        no_subdirs = self.cache_key().replace('/', '%')
        return f'ai2-beakerstore-{no_subdirs}'


ItemDetails = namedtuple('ItemDetails', ['cache_item', 'beaker_info'])


class CacheLock:
    def __init__(self, cache_item: CacheItem):
        self.lock_loc = Path(f'{cache_item.item_cache_loc()}.lock')

    def _wait_for_lock(self) -> None:

        if not self.lock_loc.is_file():
            return

        start = time.time()
        print(f'Waiting for the lock file here: {self.lock_loc}.')
        last_message_time = start

        while self.lock_loc.is_file():
            if time.time() - last_message_time > 60:
                now = time.time()
                print(f'Still waiting for the lock file. It\'s been {now - start} seconds.')
                last_message_time = now
            time.sleep(1)

    def get_lock(self) -> None:
        self._wait_for_lock()
        try:
            self.lock_loc.touch(mode=0o644, exist_ok=False)
        except FileExistsError:
            self.get_lock()

    def release_lock(self) -> None:
        self.lock_loc.unlink()


# cache locations

def _get_local_cache_base() -> Path:

    cache_loc_base = os.environ.get('AI2_DATASTORE_DIR')

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


def _get_tmp_loc() -> Path:
    return _get_local_cache_base() / 'tmp'


# functions around getting details on a dataset from beaker

def _get_dataset_details(
        given_path: str,
        which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> ItemDetails:

    try:
        # this expects a format like: ds_abc
        return _get_dataset_details_helper(_beaker_path_to_dataset_id(given_path), given_path,
                                           which_beaker)

    except DatasetNotFoundError as e_id:

        if len(given_path.split('/')) > 1:

            try:
                # we could have been given a dataset in this format: chloea/my-dataset. Try that.
                return _get_dataset_details_helper(_beaker_path_to_author_and_name(given_path),
                                                   given_path, which_beaker)

            except DatasetNotFoundError as e_author_and_name:
                raise DatasetNotFoundError(f'{e_id}\n{e_author_and_name}')
        else:
            raise e_id


def _beaker_path_to_dataset_id(given_path: str) -> str:
    return given_path.split('/')[0]


def _beaker_path_to_author_and_name(given_path: str) -> str:
    split = given_path.split('/')
    assert len(split) > 1
    return '/'.join(split[:2])


def _get_dataset_details_helper(
        possible_identifier: str,
        given_path: str,
        which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> ItemDetails:

    try:
        url = _get_beaker_dataset_url(possible_identifier, which_beaker=which_beaker)
        res = urllib.request.urlopen(url)
        beaker_info = json.loads(res.read())

        # add 1 to get past the '/'
        file_path = given_path[len(possible_identifier) + 1:]

        is_dir = file_path == ''
        cache_item = CacheItem(_get_dataset_id(beaker_info), is_dir, file_path,
                               which_beaker=which_beaker)
        return ItemDetails(cache_item, beaker_info)

    except HTTPError as e:

        if e.code == 404:
            raise DatasetNotFoundError(f'Could not find dataset \'{possible_identifier}\'.')
        else:
            raise e


def _get_beaker_dataset_url(
        dataset_id: str,
        which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> str:

    base_url = f'{BEAKER_URLS[which_beaker]}/api/v3/datasets'
    return f'{base_url}/{dataset_id}'


# Functions around getting the desired information out of the details we get from beaker

def _get_storage_address(res: BeakerInfo) -> str:
    return res['storage']['address']


def _get_dataset_id(res: BeakerInfo) -> str:
    return res['id']


def _get_storage_id(res: BeakerInfo) -> str:
    return res['storage']['id']


def _get_storage_token(res: BeakerInfo) -> str:
    return res['storage']['token']


# Functions around getting the desired files from fileheap

def _download(item_details) -> None:
    if item_details.cache_item.already_exists():
        return

    if item_details.cache_item.is_dir:
        _download_directory(item_details)

    else:
        _download_file(item_details)


def _download_directory(item_details: ItemDetails) -> None:
    assert item_details.cache_item.is_dir, \
        f'Expected a file CacheItem. Got a directory CacheItem.'

    if item_details.cache_item.already_exists():
        return

    dir_req = _construct_directory_manifest_request(item_details)
    dir_res = urllib.request.urlopen(dir_req)

    if not dir_res.code == 200:
        raise BeakerstoreError(
            f'Unable to get the requested directory manifest. Response code: {dir_res.code}.')

    def dir_to_file(file_name):
        cache_item = item_details.cache_item.dir_to_file(file_name)
        return ItemDetails(cache_item, item_details.beaker_info)

    file_names = list(map(lambda f: f['path'], json.loads(dir_res.read())['files']))
    items_with_details = list(map(dir_to_file, file_names))

    for item in items_with_details:
        _download_file(item)


def _download_file(item_details: ItemDetails) -> None:
    assert not item_details.cache_item.is_dir, \
        f'Expected a directory CacheItem. Got a file CacheItem.'

    if item_details.cache_item.already_exists():
        return

    req = _construct_one_file_download_request(item_details)
    res = urllib.request.urlopen(req)

    if not res.code == 200:
        raise BeakerstoreError(f'Unable to get the requested file. Response code: {res.code}.')

    item_details.cache_item.make_cache_entry_from_response(res)


def _construct_directory_manifest_request(item_details: ItemDetails) -> urllib.request.Request:
    url = f'{_get_file_heap_base_url(item_details)}/manifest'
    return _construct_fileheap_request(url, item_details)


def _construct_one_file_download_request(item_details: ItemDetails) -> urllib.request.Request:
    url = f'{_get_file_heap_base_url(item_details)}/files/{item_details.cache_item.file_name}'
    return _construct_fileheap_request(url, item_details)


def _construct_fileheap_request(url: str, item_details: ItemDetails) -> urllib.request.Request:
    headers = {
        'Authorization': f'Bearer {_get_storage_token(item_details.beaker_info)}'
    }
    return urllib.request.Request(url, headers=headers)


def _get_file_heap_base_url(item_details: ItemDetails) -> str:
    info = item_details.beaker_info
    return f'{_get_storage_address(info)}/datasets/{_get_storage_id(info)}'


# the central function

def path(given_path: str, which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> Path:
    item_details = _get_dataset_details(given_path, which_beaker)
    if not item_details.cache_item.already_exists():
        _download(item_details)
    return item_details.cache_item.item_cache_loc()


# some exceptions

class DatasetNotFoundError(Exception):
    pass


class BeakerstoreError(Exception):
    pass
