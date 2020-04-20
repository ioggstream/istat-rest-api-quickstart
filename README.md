# istat-rest-api-quickstart
A quickstart for invoking ISTAT rest api in python

This project shows how to use the ISTAT rest endpoint
via the pandasdmx package.

The development version has a fix which is required for working
with the ISTAT endpoint, so you cannot use older versions.

ISTAT endpoint provides REST apis returning output in:

- XML
- JSON
- CSV 

See the spec here [sdmx-rest.yaml](sdmx-rest.yaml)

## Example usage

In [tests](tests/test_istat.py) you can check usage examples while retrieving
files using pandasdmx.
