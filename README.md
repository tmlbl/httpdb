httpdb
======

`httpdb` is a database system focused on ease of use. Data is written and
queried using only HTTP requests with standard REST verbs and query strings, and
is returned in CSV or JSON format as specified by HTTP headers. Tables are
created automatically when data is written, and the table schema is inferred
from the shape of the input data. As such, `httpdb` can be integrated into
projects incredibly quickly in any programming language, while offering a
powerful storage backend and flexible query engine.
