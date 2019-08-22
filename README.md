# beakerstore

## TODO: What is beakerstore?

## Getting beakerstore

Run

```
pip install https://github.com/allenai/beakerstore/releases/download/v0.1.0/beakerstore-0.1.0-py3-none-any.whl
```

## Using beakerstore

```
import beakerstore

# by dataset id
p = beakerstore.path('ds_abc')

# by user and name
p = beakerstore.path('myuser/my_dataset')

# a file from within a dataset
p = beakerstore.path('ds_ghij/my_file.txt')
```
```
# If you're able to talk to internal Beaker and you want a dataset (or a file from a dataset) from there:
p = beakerstore.path('ds_def', beakerstore.BeakerOptions.INTERNAL)
p = beakerstore.path('ds_klmnop/my_other_file.txt', beakerstore.BeakerOptions.INTERNAL)
```

## Working on beakerstore

The following instructions assume you've cloned this repo.

### Building beakerstore locally

Run

```
python setup.py sdist bdist_wheel
```

The `.whl` file will be in the `dist` directory. You can then use `pip install <path to the .whl file>` to install it.


### 'Releasing' beakerstore

Update the version number in [setup.py](./setup.py).

Build beakerstore locally as described in the [section above](./README.md#building-beakerstore-locally).

Create a release on GitHub (documentation [here](https://help.github.com/en/articles/creating-releases)). Attach the `.tar.gz` and `.whl` files in the `dist` directory that were created when you built beakerstore locally to the release.

### Running the tests

Get pytest

```
pip install pytest
```

Run the tests
```
pytest beakerstore/tests
```

By default, only the tests that get datasets from public Beaker are run. If you wish to also run tests that get datasets from internal Beaker too, run

```
pytest --run-internal beakerstore/tests
```
This will run all the tests.
