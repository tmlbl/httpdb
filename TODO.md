TODO
====

# Querying

## Range Querying

get /tables/foo?id>1

# Tokens and namespaces

Generate or read an admin token on startup

Middleware to validate token and extract namespace

All db operations require a namespace

post /ns/:name -> admin create namespace
get /ns -> list namespaces (admin) or list of 1 user namespace
