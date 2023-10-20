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

