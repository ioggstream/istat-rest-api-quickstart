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


@pytest.fixture(scope="session")
def istat():
    return Request("ISTAT")


def teardown_function():
    f = Path("out.xml")
    if f.is_file():
        f.unlink()


def test_dataflows(istat):
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


def test_dataflow(istat):
    # Get a single dataflow
    dflow = istat.dataflow(resource_id="115_333", agency="IT1", version="1.2")
    i_stat_code = list(dflow.msg.datastructure.keys())[0]
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


def test_data_queries(istat):
    queries = yaml.safe_load(Path("queries.yaml").read_text())
    queries = queries["queries"]
    for q in queries:
        df = get_dataset(istat, q['query'])
        assert not df.empty


def report(df):
    series = list(df.series)

    return {
        "sample": "\n".join(str(series[i].key) for i in range(10)),
        "#series": len(series),
        "keys_FREQ": set(s.key.FREQ for s in series),
        "fields": set(
            x
            for s in series
            for x in s.key.__dir__()
            if x[0] == x[0].upper() and x[0] != "_"
        ),
    }


def test_flatten(istat):
    query = dict(resource_id="115_333", agency="IT1", params={"startPeriod": "2020"})
    df = get_dataset(client=istat, query=query)
    data = flatten_data(df)
    ipi_dump_data(data)
