# istat-rest-api-quickstart
A quickstart for invoking ISTAT rest api in python

This project shows how to use the ISTAT rest endpoint
via the pandasdmx package, using a [patched version capable of working
with ISTAT](https://github.com/ioggstream/pandaSDMX/).

The development version has a fix which is required for working
with the ISTAT endpoint, so you cannot use other versions for now.

ISTAT endpoint provides REST apis returning output in:

- XML
- JSON
- CSV 

See the spec here [sdmx-rest.yaml](sdmx-rest.yaml)

## Example usage

A 3-minute tutorial is in [quickstart.py](quickstart.py).

The other files contain slightly more complex queries and processing samples:

- [samples.py](samples.py) has some functions that shows how to extract
  common data from the repo.
- [tests](tests/test_istat.py) you can check usage examples while retrieving
  information and test different queries.
- [queries.yaml](queries.yaml) has some query examples with their HTTP 
  counterparts.


## Test

Run tests via docker with:

```
docker-compose up
```

Or if you have a python 3.7+ with `tox` and `poetry` use:

```
tox
```