httpdb
======

`httpdb` is an incredibly simple yet powerful database system, focused on ease-of-use.
It uses JSON and CSV data formats and simple HTTP semantics, and can be integrated
quickly into projects in any language without an SDK.

```bash
$ echo '[{"id":"a","text":"hello","value":8},{"id":"b","value":10}]' > data.json

$ curl -XPOST --data-binary @data.json -H "content-type: application/json" localhost:3737/data

$ curl -s localhost:3737/data | jq
[
  {
    "id": "a",
    "text": "hello",
    "value": 8
  },
  {
    "id": "b",
    "value": 10
  }
]

$ curl -s 'localhost:3737/data?id=b' | jq
[
  {
    "id": "b",
    "value": 10
  }
]

$ curl -s 'localhost:3737/data?value<10' | jq
[
  {
    "id": "a",
    "text": "hello",
    "value": 8
  }
]
```

`httpdb` especially shines as a backing store for small projects like Python scripts, where
development velocity is paramount. However, it is robust enough to power larger projects
as well. The CSV tables work especially well when using `pandas`.

```python
import requests
import pandas as pd
import numpy as np

HTTPDB_HOST = "http://localhost:3737"

n_rows = 100

df = pd.DataFrame(
    np.random.randn(n_rows, 2),
    columns=["val1", "val2"],
    index=pd.date_range("20130101", periods=n_rows, freq="T"),
)

# Save data as CSV. The index column will be used as the primary key for each row
requests.post(
    f"{HTTPDB_HOST}/pandas",
    df.to_csv(),
    headers={
        "content-type": "text/csv",
    },
)

# DataFrames can be loaded by passing the URL directly to read_csv
loaded = pd.read_csv(f"{HTTPDB_HOST}/pandas?val1>2")
```
