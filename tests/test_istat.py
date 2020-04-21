from pandasdmx import Request
from pathlib import Path
import pytest
import yaml

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
    return Request("ISTAT")


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
    dflow = istat.dataflow(resource_id="115_333", agency="IT1", version="1.2")
    i_stat_code = next(iter(dflow.datastructure))
    assert i_stat_code == "DCSC_INDXPRODIND_1"


def test_data_to_file(istat):
    # Get data filtering per-period: don't specify version here!
    # GET /SDMXWS/rest/data/115_333/?startPeriod=2019-10
    res = istat.data(
        resource_id="115_333", agency="IT1", params={"startPeriod": "2020"}
    )
    # dump data to a file
    res.write_source(filename="out.xml")
    assert Path("out.xml").is_file()


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
