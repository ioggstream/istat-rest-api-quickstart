from pandasdmx import Request
from pathlib import Path
import pytest
import yaml
import pandasdmx as sdmx
# Show http traces.
import http.client as http_client
import logging

from samples import *

http_client.HTTPConnection.debuglevel = 2
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

log = logging.getLogger()


@pytest.fixture()
def istat():
    istat = Request("ISTAT")
    return istat

def harn_get_structure(dataflow):
    if sdmx.__version__ > '0.9':
        d = dataflow.structure
    else:
        d = dataflow.datastructure
    return next(iter(d))
def teardown_function():
    f = Path("out.xml")
    if f.is_file():
        f.unlink()


def test_get_all_dataflows(istat):
    # Datasets in the ISTAT portal have different labels
    #   from the sdmx dataflows. You can look for
    #   the dataflow from the istat website http://sdmx.istat.it/sdmxMetaRepository/DataFlow.aspx?m=y
    #   An i.stat label can map to one or more dataflow
    #   i.stat-label 1:n dataflow.
    # List dataflows shows labels but can't search for it.
    dataflows = get_dataflows(istat)
    assert dataflows

    dataflows = get_dataflows(istat, label="DCCN_TNA")
    assert dataflows


def test_get_one_dataflow(istat):
    q = dict(resource_id="115_333", agency="IT1", version="1.2")
    if sdmx.__version__ > '0.9':
        q.pop('agency', None)
    dflow = istat.dataflow(**q)
    i_stat_code = harn_get_structure(dflow)
    assert i_stat_code == "DCSC_INDXPRODIND_1"


def param_queries():
    queries = yaml.safe_load(Path("queries.yaml").read_text())
    return queries["queries"]


@pytest.mark.parametrize("query", param_queries())
def test_data_queries(istat, query):
    df = get_dataset(istat, query["query"])
    assert not df.empty


def test_flatten(istat):
    query = dict(resource_id="115_333", agency="IT1", params={"startPeriod": "2020"})
    df = get_dataset(client=istat, query=query)
    data = flatten_data(df)
    ipi_dump_data(data)
