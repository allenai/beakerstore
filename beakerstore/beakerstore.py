import urllib.request
import json
import os
import collections

from typing import Optional, Tuple

TEST_DOWNLOADS = 'test_downloads'
from http.client import HTTPResponse
from urllib.error import HTTPError

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

Path = collections.namedtuple('Path', ['dataset', 'file', 'public'], defaults=(True,))
DatasetDetails = collections.namedtuple('DatasetDetails', ['path', 'details'])
Result = collections.namedtuple('Result', ['success', 'message'], defaults=(None,))


def get_local_storage_loc():
    return TEST_DOWNLOADS


def get_beaker_datasets_base_url(public: bool = True) -> str:
    key = PUBLIC if public else INTERNAL
    return f'{BEAKER_URLS[key]}/api/v3/datasets'


def get_beaker_datasets_url(dataset_id: str, public: bool = True) -> str:
    return f'{get_beaker_datasets_base_url(public)}/{dataset_id}'


def get_beaker_dataset_details(dataset_id: str, public: bool = True) -> HTTPResponse:
    url = get_beaker_datasets_url(dataset_id, public)
    # res = urllib.request.urlopen(url)
    # return json.loads(res.read())
    # return json.loads(res.read())
    return urllib.request.urlopen(url)


def make_fileheap_request(url, dataset_details):
    headers = {
        'Authorization': f'Bearer {get_storage_token(dataset_details.details)}'
    }
    return urllib.request.Request(url, headers=headers)


def make_download_one_file_request(dataset_details: DatasetDetails):
    url = f'{get_file_heap_base_url(dataset_details)}/files/{dataset_details.path.file}'
    print(url)
    return make_fileheap_request(url, dataset_details)
    # res = urllib.request.urlopen(req)
    # print(res.status)


def make_download_directory_request(dataset_details: DatasetDetails):
    url = f'{get_file_heap_base_url(dataset_details)}/manifest'
    print(url)
    return make_fileheap_request(url, dataset_details)
    # res = urllib.request.urlopen(req)
    # print(res.status)
    # print(res.read())


def get_file_heap_base_url(dataset_details: DatasetDetails):
    details = dataset_details.details
    return f'{get_storage_address(details)}/datasets/{get_storage_id(details)}'


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


# Path = collections.namedtuple('Thing', ['dataset', 'file'])


def get_dataset_details(given_path: str, public: bool = True) -> Tuple[Optional[DatasetDetails], Result]:

    split = given_path.split('/')
    curr_ds_name = split[0]
    try:
        res = get_beaker_dataset_details(curr_ds_name, public)
        dataset_details = json.loads(res.read())
        file_path = '/'.join(split[1:])
        if file_path == '':
            file_path = None
        path_details = Path(get_dataset_id(dataset_details), file_path, public)
        return DatasetDetails(path_details, dataset_details), Result(True)


        # print('found it!')
    except HTTPError as e:

        if e.code == 404:
            # print('did not find it')

            if not len(split) > 1:
                return None, Result(False, f'Could not find dataset {curr_ds_name}.')
                # print('could not find the dataset')
                # return
            original_ds_name = curr_ds_name
            curr_ds_name = '/'.join(split[:2])
            try:
                res = get_beaker_dataset_details(curr_ds_name, public=public)
                dataset_details = json.loads(res.read())
                file_path = '/'.join(split[2:])
                if file_path == '':
                    file_path = None
                path_details = Path(get_dataset_id(dataset_details), file_path, public)
                return DatasetDetails(path_details, dataset_details), Result(True)
                # print('found it now!')
            except HTTPError as e2:
                if e2.code == 404:
                    return None, Result(False, f'Could not datasets. Names tried: {original_ds_name}, {curr_ds_name}.')
                    # print('still did not find it')
                else:
                    return None, Result(False, f'Encountered an issue trying to find dataset {curr_ds_name}.\n{e}')
        else:
            return None, Result(False, f'Encountered an issue trying to find dataset {curr_ds_name}.\n{e}')

# def beaker_details_to_file_url


def get_specific_local_loc(dataset_details: DatasetDetails):
    local_path = f'{get_local_storage_loc()}/{dataset_details.path.dataset}'
    if downloading_directory(dataset_details):
        return local_path, None
    else:
        return local_path, dataset_details.path.file
        # local_path = f'{local_path}/{dataset_details.path.file}'
    #
    # return local_dir, file_path


def dataset_already_exists(dataset_details: DatasetDetails):
    local_path, local_file = get_specific_local_loc(dataset_details)
    if downloading_directory(dataset_details):
        return os.path.exists(local_path)
    else:
        return os.path.isfile(f'{local_path}/{local_file}')


def downloading_directory(dataset_details: DatasetDetails):
    return dataset_details.path.file is None


def write_file(dataset_details: DatasetDetails, res):
    local_path, local_file = get_specific_local_loc(dataset_details)
    if not os.path.exists(local_path):
        os.mkdir(local_path)
    if not downloading_directory(dataset_details):
        print('in here')
        with open(f'{local_path}/{local_file}', 'wb') as f:
            f.write(res.read())
    else:
        print('pretended to download directory')


def download_dataset(dataset_details: DatasetDetails) -> Result:
    if dataset_already_exists(dataset_details):
        print('yo')
        return Result(True)

    print('file path', dataset_details.path.file)
    if downloading_directory(dataset_details):
        req = make_download_directory_request(dataset_details)
    else:
        print('maybe in here')
        req = make_download_one_file_request(dataset_details)

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