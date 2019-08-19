# beakerstore

## To use:

```
from beakerstore import beakerstore

# from public Beaker
# by dataset id
p = beakerstore.path('ds_abc')

# by user and name
p = beakerstore.path('myuser/my_dataset')

# from internal Beaker
p = beakerstore.path('ds_def', datastore.BeakerOptions.INTERNAL)
```
