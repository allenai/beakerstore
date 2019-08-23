import pytest


# Most of this a variation of the example for skipping slow tests in pytest documentation
# here: https://docs.pytest.org/en/latest/example/simple.html


def pytest_addoption(parser):
    parser.addoption(
        '--run-internal', action='store_true', default=False, help='run internal Beaker tests'
    )


def pytest_configure(config):
    config.addinivalue_line('markers', 'internal: mark test as a test using internal Beaker')


def pytest_collection_modifyitems(config, items):
    if config.getoption('--run-internal'):
        return
    skip_internal = pytest.mark.skip(reason='need --run-internal option to run')
    for item in items:
        if 'internal' in item.keywords:
            item.add_marker(skip_internal)
