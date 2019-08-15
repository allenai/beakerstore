import json
import urllib.request

from collections import namedtuple
from enum import Enum
from pathlib import Path
from typing import Optional, NewType

from urllib.error import HTTPError


ItemDetails = namedtuple('ItemDetails', ['cache_item', 'beaker_info'])
BeakerInfo = NewType('BeakerInfo', dict)


class BeakerOptions(Enum):
    INTERNAL = 'internal'
    PUBLIC = 'public'


BEAKER_URLS = {
    BeakerOptions.INTERNAL: 'https://allenai.beaker.org',
    BeakerOptions.PUBLIC: 'https://beaker.org'
}


class CacheItem:
    def __init__(self, dataset_id, is_dir, file_name, which_beaker: BeakerOptions):
        self.dataset_id: str = dataset_id
        self.is_dir: bool = is_dir
        self.file_name: Optional[str] = None if file_name == '' else file_name

        assert self.is_dir == (self.file_name is None)

        self.which_beaker: BeakerOptions = which_beaker
        self.cache_loc: Path = _get_local_cache_loc(self.which_beaker)

    def cache_subdir(self) -> Path:
        return self.cache_loc / self.dataset_id

    def cache_key(self) -> Path:
        if self.is_dir:
            return self.cache_subdir()
        return self.cache_subdir() / self.file_name

    def already_exists(self) -> bool:
        if self.is_dir:
            return self.cache_key().exists()
        return self.cache_key().is_file()

    def dir_to_file(self, file_name: str):
        assert self.is_dir, 'Expected a directory CacheItem. Got a file CacheItem.'
        return CacheItem(self.dataset_id, False, file_name, self.which_beaker)


def _get_local_cache_loc(which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> Path:
    # TODO actually put this in the right place
    # maybe lift some stuff from datastore

    default_cache_loc = 'test_downloads'

    cache_loc = Path(default_cache_loc) / 'beaker' / which_beaker.value

    if not cache_loc.exists():
        cache_loc.mkdir(parents=True)
    return cache_loc


# functions around getting details on a dataset from beaker

def get_dataset_details(
        given_path: str,
        which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> ItemDetails:

    try:
        # this expects a format like: ds_abc
        return get_dataset_details_helper(beaker_path_to_dataset_id(given_path), given_path,
                                          which_beaker)

    except DatasetNotFoundError as e_id:

        if len(given_path.split('/')) > 1:

            try:
                # we could have been given a dataset in this format: chloea/my-dataset. Try that.
                return get_dataset_details_helper(beaker_path_to_author_and_name(given_path),
                                                  given_path, which_beaker)

            except DatasetNotFoundError as e_author_and_name:
                raise DatasetNotFoundError(f'{e_id}\n{e_author_and_name}')
        else:
            raise e_id


def beaker_path_to_dataset_id(given_path: str) -> str:
    return given_path.split('/')[0]


def beaker_path_to_author_and_name(given_path: str) -> str:
    split = given_path.split('/')
    assert len(split) > 1
    return '/'.join(split[:2])


def get_dataset_details_helper(
        possible_identifier: str,
        given_path: str,
        which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> ItemDetails:

    try:
        url = get_beaker_dataset_url(possible_identifier, which_beaker=which_beaker)
        res = urllib.request.urlopen(url)
        beaker_info = json.loads(res.read())

        # add 1 to get past the '/'
        file_path = given_path[len(possible_identifier) + 1:]

        is_dir = file_path == ''
        cache_item = CacheItem(get_dataset_id(beaker_info), is_dir, file_path,
                               which_beaker=which_beaker)
        return ItemDetails(cache_item, beaker_info)

    except HTTPError as e:

        if e.code == 404:
            raise DatasetNotFoundError(f'Could not find dataset \'{possible_identifier}\'.')
        else:
            raise e


def get_beaker_dataset_url(
        dataset_id: str,
        which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> str:

    base_url = f'{BEAKER_URLS[which_beaker]}/api/v3/datasets'
    return f'{base_url}/{dataset_id}'


# Functions around getting the desired information out of the details we get from beaker

def get_storage_address(res: BeakerInfo) -> str:
    return res['storage']['address']


def get_dataset_id(res: BeakerInfo) -> str:
    return res['id']


def get_storage_id(res: BeakerInfo) -> str:
    return res['storage']['id']


def get_storage_token(res: BeakerInfo) -> str:
    return res['storage']['token']


# Function around getting the desired files from fileheap

def download(item_details) -> None:
    assert not item_details.cache_item.already_exists(), \
        f'The item already exists. Key {item_details.cache_item.cache_key()}'

    if item_details.cache_item.is_dir:
        download_directory(item_details)

    else:
        download_file(item_details)


def download_directory(item_details: ItemDetails) -> None:
    assert not item_details.cache_item.already_exists(), \
        f'The item already exists. Key {item_details.cache_item.cache_key()}'
    assert item_details.cache_item.is_dir, \
        f'Expected a file CacheItem. Got a directory CacheItem.'

    dir_req = construct_directory_manifest_request(item_details)
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
        download_file(item)


def download_file(item_details: ItemDetails) -> None:
    assert not item_details.cache_item.already_exists(), \
        f'The item already exists. Key {item_details.cache_item.cache_key()}'
    assert not item_details.cache_item.is_dir, \
        f'Expected a directory CacheItem. Got a file CacheItem.'

    req = construct_one_file_download_request(item_details)
    res = urllib.request.urlopen(req)

    if not res.code == 200:
        raise BeakerstoreError(f'Unable to get the requested file. Response code: {res.code}.')

    if not item_details.cache_item.cache_subdir().exists():
        item_details.cache_item.cache_subdir().mkdir()
    with item_details.cache_item.cache_key().open('wb') as f:
        f.write(res.read())


def construct_directory_manifest_request(item_details: ItemDetails) -> urllib.request.Request:
    url = f'{get_file_heap_base_url(item_details)}/manifest'
    return construct_fileheap_request(url, item_details)


def construct_one_file_download_request(item_details: ItemDetails) -> urllib.request.Request:
    url = f'{get_file_heap_base_url(item_details)}/files/{item_details.cache_item.file_name}'
    return construct_fileheap_request(url, item_details)


def construct_fileheap_request(url: str, item_details: ItemDetails) -> urllib.request.Request:
    headers = {
        'Authorization': f'Bearer {get_storage_token(item_details.beaker_info)}'
    }
    return urllib.request.Request(url, headers=headers)


def get_file_heap_base_url(item_details: ItemDetails) -> str:
    info = item_details.beaker_info
    return f'{get_storage_address(info)}/datasets/{get_storage_id(info)}'


# the central function

def path(given_path: str, which_beaker: BeakerOptions = BeakerOptions.PUBLIC) -> Path:
    item_details = get_dataset_details(given_path, which_beaker)
    if not item_details.cache_item.already_exists():
        download(item_details)
    return item_details.cache_item.cache_key()


# some exceptions

class DatasetNotFoundError(Exception):
    pass


class BeakerstoreError(Exception):
    pass
