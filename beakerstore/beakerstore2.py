import json
import logging
import os
import urllib.request

from collections import namedtuple
from typing import Optional

from http.client import HTTPResponse
from urllib.error import HTTPError


TEST_DOWNLOADS = 'test_downloads'

INTERNAL = 'internal'
PUBLIC = 'public'

BEAKER_URLS = {
    INTERNAL: 'https://allenai.beaker.org',
    PUBLIC: 'https://beaker.org'
}


def _get_local_cache_loc(public: bool = True):
    # TODO actually put this in the right place
    # maybe lift some stuff from datastore
    # probably also make this internal
    subdir = 'public' if public else 'internal'
    cache_loc = f'{TEST_DOWNLOADS}/{subdir}'
    if not os.path.exists(cache_loc):
        os.mkdir(cache_loc)
    return cache_loc


ItemDetails = namedtuple('ItemDetails', ['cache_item', 'beaker_info'])


class CacheItem:
    def __init__(self, dataset_id, is_dir, file_name, public):
        self.dataset_id: str = dataset_id
        self.is_dir: bool = is_dir
        self.file_name: Optional[str] = None if file_name == '' else file_name

        assert self.is_dir == (self.file_name is None)

        self.public: bool = public
        self.cache_loc: str = _get_local_cache_loc(self.public)

    def cache_subdir(self):
        return f'{self.cache_loc}/{self.dataset_id}'

    def cache_key(self):
        if self.is_dir:
            return self.cache_subdir()
        return f'{self.cache_subdir()}/{self.file_name}'

    def already_exists(self):
        if self.is_dir:
            return os.path.exists(self.cache_key())
        return os.path.isfile(self.cache_key())

    def dir_to_file(self, file_name):
        assert self.is_dir
        return CacheItem(self.dataset_id, False, file_name, self.public)


class DatasetNotFoundError(Exception):
    pass


class BeakerstoreError(Exception):
    pass


def get_storage_address(res: dict) -> str:
    return res['storage']['address']


def get_dataset_id(res: dict) -> str:
    return res['id']


def get_storage_id(res: dict) -> str:
    return res['storage']['id']


def get_storage_token(res: dict) -> str:
    return res['storage']['token']


def get_beaker_dataset_url(dataset_id: str, public: bool = True) -> str:
    key = PUBLIC if public else INTERNAL
    base_url = f'{BEAKER_URLS[key]}/api/v3/datasets'
    return f'{base_url}/{dataset_id}'


def get_beaker_dataset_info_response(dataset_id: str, public: bool = True) -> HTTPResponse:
    url = get_beaker_dataset_url(dataset_id, public)
    return urllib.request.urlopen(url)


def path_to_dataset_id(given_path: str) -> str:
    return given_path.split('/')[0]


def path_to_author_and_name(given_path: str) -> str:
    split = given_path.split('/')
    assert len(split) > 1
    return '/'.join(split[:2])


def get_dataset_details_helper(
        given_path: str,
        public: bool,
        possible_identifier: str) -> ItemDetails:

    try:
        res = get_beaker_dataset_info_response(possible_identifier, public=public)
        dataset_info = json.loads(res.read())

        # add 1 to get past the '/'
        file_path = given_path[len(possible_identifier) + 1:]

        is_dir = file_path == ''
        cache_item = CacheItem(get_dataset_id(dataset_info), is_dir, file_path, public)
        return ItemDetails(cache_item, dataset_info)

    except HTTPError as e:

        if e.code == 404:
            raise DatasetNotFoundError(f'Could not find dataset \'{possible_identifier}\'.')
        else:
            raise e


def get_dataset_details(given_path: str, public: bool = True) -> ItemDetails:

    try:
        # this expects a format like: ds_abc
        return get_dataset_details_helper(given_path, public, path_to_dataset_id(given_path))

    except DatasetNotFoundError as e_id:

        if len(given_path.split('/')) > 1:

            try:
                # we could have been given a dataset in this format: chloea/my-dataset. Try that.
                return get_dataset_details_helper(given_path, public,
                                                  path_to_author_and_name(given_path))

            except DatasetNotFoundError as e_author_and_name:
                raise DatasetNotFoundError(f'{e_id}\n{e_author_and_name}')
        else:
            raise e_id


def get_file_heap_base_url(item_details: ItemDetails):
    info = item_details.beaker_info
    return f'{get_storage_address(info)}/datasets/{get_storage_id(info)}'


def construct_fileheap_request(url: str, item_details: ItemDetails):
    headers = {
        'Authorization': f'Bearer {get_storage_token(item_details.beaker_info)}'
    }
    return urllib.request.Request(url, headers=headers)


def construct_one_file_download_request(item_details: ItemDetails):
    url = f'{get_file_heap_base_url(item_details)}/files/{item_details.cache_item.file_name}'
    return construct_fileheap_request(url, item_details)


def construct_directory_manifest_request(item_details: ItemDetails):
    url = f'{get_file_heap_base_url(item_details)}/manifest'
    return construct_fileheap_request(url, item_details)


def download_file(item_details: ItemDetails) -> None:
    assert not item_details.cache_item.already_exists()
    assert not item_details.cache_item.is_dir

    req = construct_one_file_download_request(item_details)
    res = urllib.request.urlopen(req)

    if not res.code == 200:
        raise BeakerstoreError('Oh nooooo')

    if not os.path.exists(item_details.cache_item.cache_subdir()):
        os.mkdir(item_details.cache_item.cache_subdir())
    with open(item_details.cache_item.cache_key(), 'wb') as f:
        f.write(res.read())


def download_directory(item_details: ItemDetails) -> None:
    assert not item_details.cache_item.already_exists()
    assert item_details.cache_item.is_dir

    dir_req = construct_directory_manifest_request(item_details)
    dir_res = urllib.request.urlopen(dir_req)

    if not dir_res.code == 200:
        raise BeakerstoreError('Oh nooooo')

    def dir_to_file(file_name):
        cache_item = item_details.cache_item.dir_to_file(file_name)
        return ItemDetails(cache_item, item_details.beaker_info)

    file_names = list(map(lambda f: f['path'], json.loads(dir_res.read())['files']))
    items_with_details = list(map(dir_to_file, file_names))

    for item in items_with_details:
        download_file(item)


def download(item_details):
    assert not item_details.cache_item.already_exists()

    if item_details.cache_item.is_dir:
        download_directory(item_details)

    else:
        download_file(item_details)


def path(given_path: str, public: bool = True) -> str:
    item_details = get_dataset_details(given_path, public)
    if item_details.cache_item.already_exists():
        # TODO: log here
        print(f'{given_path} already exists.')
        return item_details.cache_item.cache_key()
    download(item_details)
    return item_details.cache_item.cache_key()


print(path('ds_bv0874n13di9', public=False))
print(path('ds_bv0874n13di9/', public=False))
print(path('chloea/chloea-dedupe-test-ds-2/somewords.txt', public=False))
print(path('chloea/chloea-dedupe-test-ds-2', public=False))
print(path('ds_bv0874n13di9/somewords.txt', public=False))
try:
    print(path('ds_nonexistent', public=False))
except DatasetNotFoundError as e:
    print(e)
try:
    print(path('chloea/nonexistent', public=False))
except DatasetNotFoundError as e:
    print(e)
