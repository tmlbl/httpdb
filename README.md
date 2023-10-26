csvd
====

`csvd` is a super-simple HTTP 1.1-based data system for reading and writing data
in the CSV format. It supports lexicographical ordering of rows based on a
primary key column and basic range queries, and can be used with any tool that
can load a CSV file over a network.

It is designed for small or medium-scale analytics workloads to ingest, catalog
and serve tabular data to many clients simultaneously, without requiring any
client code. Also, due to the portability of HTTP and the CSV format, moving 
data between `csvd` and other data systems is trivial, so it can also be useful
as a staging area for experimentation when used alongside a more full-featured
OLAP system.

# Usage

```python
import pandas as pd
import numpy as np
import requests

my_data = pd.DataFrame(
    np.random.randn(100000, 1),
    columns=["value"],
    index=pd.date_range("20130101", periods=100000, freq="T"),
)

print('writing data...')
requests.post('http://localhost:3737/tables/example', my_data.to_csv(header=True))

print('reading data...')
df = pd.read_csv('http://localhost:3737/tables/example')
print(df)
```

# TODO

* make sure rocksdb is cleaning up
* load testing and identify bottlenecks
    * buffer pool to write larger memory blocks to responses
* implement range queries
* implement memory cache
