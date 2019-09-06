# beakerstore

`beakerstore` is a library. You use it to get a local path to a Beaker dataset, or a file within a Beaker dataset.

The first time you attempt to access a dataset (or a file within a dataset), `beakerstore` will get the dataset/file from Beaker, and store on the local file system. If you try to access the same item in the future, `beakerstore` will notice that it is already present, and not attempt to download it again.

You can use `beakerstore` for public datasets on either public Beaker or internal Beaker.

## Getting started

### Getting beakerstore

Clone this repo. Then run

```
python setup.py bdist_wheel
```

in an environment with Python 3.

The `.whl` file will be in the `dist` directory. You can then use `pip install <path to the .whl file>` to install it.

### Using beakerstore

Once, you've installed `beakerstore`, you can do things like:

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

#### Adjusting the cache location

By default, `beakerstore` will store the items you request in `~/Library/Caches/beakerstore` (Mac) or `~/.ai2/beakerstore` (Linux).

You can change this by setting the `AI2_BEAKERSTORE_DIR` environment variable.

You can also change this by creating an instance of `Cache` with the desired path, and passing this to the `path()` function.

For example:
```
custom_cache = beakerstore.beakerstore.Cache(Path('path/to/some/location'))
p = beakerstore.path('ds_qrs', cache=custom_cache)
```

You can see another example of this if you look at the tests [here](./beakerstore/tests/beakerstore_test.py).

## Working on beakerstore

If you'd like to improve `beakerstore`, please feel free to fork this repo, and open a pull request!

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

## Questions

Have a question? Please feel free to open an issue.
