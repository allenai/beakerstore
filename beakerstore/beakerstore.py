import logging
import urllib.request
import json
import os
import collections

from typing import Optional, Tuple

from http.client import HTTPResponse
from urllib.error import HTTPError

TEST_DOWNLOADS = 'test_downloads'

INTERNAL = 'internal'
PUBLIC = 'public'

BEAKER_URLS = {
    INTERNAL: 'https://allenai.beaker.org',
    PUBLIC: 'https://beaker.org'
}
# url = 'https://allenai.beaker.org/api/v3/datasets/ds_2dgtppi2dauo'
# url = 'https://beaker.org/api/v3/datasets/ds_chbhb69q9qh9'


# class Result:
#     def __init__(self, success, msg=None):
#         self.success = success
#         self.msg = msg

# CachePath = collections.namedtuple('CachePath', ['cache_loc', 'directory', 'file', 'is_dir'])
BeakerPath = collections.namedtuple('BeakerPath', ['dataset', 'file', 'is_dir', 'public'],
                                    defaults=(True,))

DatasetDetails = collections.namedtuple('DatasetDetails', ['beaker_path', 'cache_path', 'info'])
Result = collections.namedtuple('Result', ['success', 'message'], defaults=(None,))


def _get_local_cache_loc(public: bool = True):
    # TODO actually put this in the right place
    # maybe lift some stuff from datastore
    # probably also make this internal
    subdir = 'public' if public else 'internal'
    return f'{TEST_DOWNLOADS}/{subdir}'


def _get_beaker_datasets_base_url(public: bool = True) -> str:
    key = PUBLIC if public else INTERNAL
    return f'{BEAKER_URLS[key]}/api/v3/datasets'


def get_beaker_dataset_url(dataset_id: str, public: bool = True) -> str:
    return f'{_get_beaker_datasets_base_url(public)}/{dataset_id}'


def get_beaker_dataset_info_response(dataset_id: str, public: bool = True) -> HTTPResponse:
    url = get_beaker_dataset_url(dataset_id, public)
    return urllib.request.urlopen(url)


def construct_fileheap_request(url, dataset_details):
    headers = {
        'Authorization': f'Bearer {get_storage_token(dataset_details.info)}'
    }
    return urllib.request.Request(url, headers=headers)


def construct_one_file_download_request(dataset_details: DatasetDetails):
    url = f'{get_file_heap_base_url(dataset_details)}/files/{dataset_details.beaker_path.file}'
    return construct_fileheap_request(url, dataset_details)


def construct_directory_manifest_request(dataset_details: DatasetDetails):
    url = f'{get_file_heap_base_url(dataset_details)}/manifest'
    return construct_fileheap_request(url, dataset_details)


def get_file_heap_base_url(dataset_details: DatasetDetails):
    info = dataset_details.info
    return f'{get_storage_address(info)}/datasets/{get_storage_id(info)}'


def path(name: str) -> None:
    pass


def get_storage_address(res: dict) -> str:
    return res['storage']['address']


def get_dataset_id(res: dict) -> str:
    return res['id']


def get_storage_id(res: dict) -> str:
    return res['storage']['id']


def get_storage_token(res: dict) -> str:
    return res['storage']['token']


class DatasetNotFoundError(Exception):
    pass


def get_dataset_details(given_path: str, public: bool = True) -> DatasetDetails:

    split = given_path.split('/')
    curr_ds_name = split[0]

    try:
        res = get_beaker_dataset_info_response(curr_ds_name, public)
        dataset_info = json.loads(res.read())
        file_path = '/'.join(split[1:])
        is_dir = file_path == ''
        beaker_path = BeakerPath(get_dataset_id(dataset_info), file_path, is_dir, public)
        return DatasetDetails(beaker_path, CachePath(beaker_path), dataset_info)

    except HTTPError as e:

        if e.code == 404:

            if not len(split) > 1:
                raise DatasetNotFoundError(f'Could not find dataset {curr_ds_name}.')
            original_ds_name = curr_ds_name
            curr_ds_name = '/'.join(split[:2])

            try:
                res = get_beaker_dataset_info_response(curr_ds_name, public=public)
                dataset_info = json.loads(res.read())
                file_path = '/'.join(split[2:])
                is_dir = file_path == ''
                beaker_path = BeakerPath(get_dataset_id(dataset_info), file_path, is_dir, public)
                return DatasetDetails(beaker_path, CachePath(beaker_path), dataset_info)

            except HTTPError as e2:
                if e2.code == 404:
                    raise DatasetNotFoundError(
                        f'Could not find dataset. Names tried: {original_ds_name}, {curr_ds_name}.')
                else:
                    raise e2

        else:
            raise e


class CachePath:
    def __init__(self, beaker_path: BeakerPath):
        self.subdir = beaker_path.dataset
        self.file = beaker_path.file
        self.is_dir = beaker_path.is_dir

        self.cache_loc = _get_local_cache_loc(beaker_path.public)

        # base_path = f'{self.cache_loc}/{self.subdir}'
        # if self.is_dir:
        #     self.cache_key = base_path
        # else:
        #     self.cache_key = f'{base_path}/{self.file}'

    # def get_local_cache_key(self):
    #     if self.is_dir:
    #         return base_path
    #     return f'{base_path}/{self.file}'

    def already_exists(self):
        if self.is_dir:
            return os.path.exists(self.cache_key)
        return os.path.isfile(self.cache_key)

    def cache_key(self):
        if self.is_dir:
            return self.cache_subdir()
        return f'{self.cache_subdir()}/{self.file}'

    def cache_subdir(self):
        return f'{self.cache_loc}/{self.subdir}'

    def dir_to_files(self, file_paths):
        return list(map(lambda p: CachePath(BeakerPath(self.subdir, p, False)), file_paths))





# def cache_path_from_beaker_path(beaker_path: BeakerPath) -> CachePath:
#     return CachePath(_get_local_cache_loc(), beaker_path.dataset,
#                      beaker_path.file, beaker_path.is_dir)
#

# def get_local_cache_key(dataset_details: DatasetDetails):
    # local_path = f'{_get_local_cache_loc()}/{dataset_details.beaker_path.dataset}'
    # return cache_path_from_beaker_path(dataset_details.beaker_path)
    # if downloading_directory(dataset_details):
    #     return local_path, None
    # else:
    #     return local_path, dataset_details.beaker_path.file


def dataset_already_exists(dataset_details: DatasetDetails):
    # local_path, local_file = get_local_cache_key(dataset_details)
    # cache_path = CachePath(dataset_details.beaker_path)
    # cache_key = cache_path.get_local_cache_key()
    # if cache_path.is
    # if cache_path.is_dir:
    #     return os.path.exists(local_path)
    #
    # if downloading_directory(dataset_details):
    #     return os.path.exists(local_path)
    # else:
    #     return os.path.isfile(f'{local_path}/{local_file}')
    return dataset_details.cache_path.already_exists()


# def downloading_directory(dataset_details: DatasetDetails):
#     return dataset_details.path.file is None


def write_file(dataset_details: DatasetDetails, res):
    # local_path, local_file = get_local_cache_key(dataset_details)
    if not os.path.exists(dataset_details.cache_path.cache_subdir()):
        os.mkdir(dataset_details.cache_path.cache_subdir())
    if not dataset_details.cache_path.is_dir:
        with open(dataset_details.cache_path.cache_key(), 'wb') as f:
            f.write(res.read())
    # if not downloading_directory(dataset_details):
    #     print('in here')
    #     with open(f'{local_path}/{local_file}', 'wb') as f:
    #         f.write(res.read())
    else:
        print('pretended to download directory')


def download_file(dataset_details: DatasetDetails) -> None:
    assert not dataset_already_exists(dataset_details)
    assert not dataset_details.beaker_path.is_dir

    req = construct_one_file_download_request(dataset_details)
    res = urllib.request.urlopen(req)
    if res.code == 200:
        if not os.path.exists(dataset_details.cache_path.cache_subdir()):
            os.mkdir(dataset_details.cache_path.cache_subdir())
        with open(dataset_details.cache_path.cache_key(), 'wb') as f:
            f.write(res.read())


# to list of strings
def manifest_response_to_file_paths(res):
    return list(map(lambda f: f['path'], json.loads(res.read())['files']))


def download_directory(dataset_details: DatasetDetails) -> None:
    assert not dataset_already_exists(dataset_details)
    assert dataset_details.beaker_path.is_dir

    dir_req = construct_directory_manifest_request(dataset_details)
    dir_res = urllib.request.urlopen(dir_req)
    if not dir_res.code == 200:
        raise Exception('nooooo')

    file_paths = manifest_response_to_file_paths(dir_res)
    cache_paths = dataset_details.cache_path.dir_to_files(file_paths)
    for cache_path in cache_paths:
        if not os.path.exists(cache_path.cache_subdir()):
            os.mkdir(cache_path.cache_subdir())
        with open(cache_path.cache_key(), 'wb') as f:
            f.write(res.read())


    req = construct_one_file_download_request(dataset_details)
    res = urllib.request.urlopen(req)
    if res.code == 200:
        cache_path = CachePath(dataset_details.beaker_path)
        if not os.path.exists(cache_path.cache_subdir()):
            os.mkdir(cache_path.cache_subdir())
        with open(cache_path.cache_key(), 'wb') as f:
            f.write(res.read())


def download_dataset(dataset_details: DatasetDetails) -> None:
    if dataset_already_exists(dataset_details):
        return
        # print('yo')
        # return Result(True)

    # print('file path', dataset_details.path.file)
    if dataset_details.beaker_path.is_dir:
        # if downloading_directory(dataset_details):
        req = construct_directory_manifest_request(dataset_details)
    else:
        print('maybe in here')
        req = construct_one_file_download_request(dataset_details)

    res = urllib.request.urlopen(req)
    if res.code == 200:
        write_file(dataset_details, res)
    print(res.code)


def do_both(given_path: str, public: bool = True):
    dataset_details, succ = get_dataset_details(given_path, public)
    if succ.success:
        download_dataset(dataset_details)
    else:
        print('problem\n', succ.message)



results = [
#     get_dataset_details('ds_bv0874n13di9', public=False),
#     get_dataset_details('ds_bv0874n13di9/', public=False),
#     get_dataset_details('chloea/chloea-dedupe-test-ds-2', public=False),
#     get_dataset_details('ds_bv0874n13di9/id.104860', public=False),
#     get_dataset_details('ds_nonexistent', public=False),
#     get_dataset_details('chloea/nonexistent', public=False)
    do_both('ds_bv0874n13di9', public=False),
    do_both('ds_bv0874n13di9/', public=False),
    do_both('chloea/chloea-dedupe-test-ds-2', public=False),
    do_both('ds_bv0874n13di9/somewords.txt', public=False),
    do_both('ds_nonexistent', public=False),
    do_both('chloea/nonexistent', public=False)
# ]
]

# for result in results:
#     details, succ = result
#     if succ.success:
#         print('found it\n', details.path)
#     else:
#         print('problem\n', succ.message)


# def test():
#     res = urllib.request.urlopen(url)
#     dict_version = json.loads(res.read())
#     print(json.dumps(dict_version))
#
#     print('address, ', get_address(dict_version))
#     print('id, ', get_id(dict_version))
#
#     print(f'{get_address(dict_version)}/{get_id(dict_version)}')
#     subfile = 'id.104860'
#     # subfile = 'input-10.jsonl'
#
#     full = f'{get_address(dict_version)}/datasets/{get_id(dict_version)}/files/{subfile}'
#     # full = f'{get_address(dict_version)}/datasets/{get_id(dict_version)}/manifest'
#     print(full)
#     # print(f'{get_address(dict_version)}/{get_id(dict_version)}/files/{subfile}')
#     req = urllib.request.Request(full, headers={ 'Authorization': f'Bearer {get_token(dict_version)}' })
#     res2 = urllib.request.urlopen(req)
#     new_dir = f'{TEST_DOWNLOADS}/ds_2dgtppi2dauo'
#     if not os.path.exists(new_dir):
#         os.mkdir(new_dir)
#     with open(f'{TEST_DOWNLOADS}/ds_2dgtppi2dauo/{subfile}', 'wb') as f:
#         f.write(res2.read())
#     # print(res2.status)
#     # print(res2.read())
#
# test()