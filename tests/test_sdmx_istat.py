from pandasdmx import Request
import pandasdmx as sdmx
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


def harn_query(istat, qid):
    for q in queries:
        if q.get("id") == qid:
            method = getattr(istat, q["type"])
            if sdmx.__version__ > "0.9":
                q["query"].pop("agency", None)
            return method(**q["query"])


def test_datastructure(istat):
    ret = istat.datastructure()
    assert "DCAR_ATT_NOTAR" in ret.structure


def test_dataflow(istat):
    ret = istat.dataflow()
    assert "101_1015" in ret.dataflow
    assert "DCSP_COLTIVAZIONI" == ret.dataflow["101_1015"].structure.id


def test_get_dataflow(istat):
    ret = harn_query(istat, "dataflow-115_333")
    assert next(iter(ret.structure.keys())) == "DCSC_INDXPRODIND_1"


def param_queries(qid=None):
    queries = Path("tests/test_queries_istat.yaml").read_text()
    queries = yaml.safe_load(queries)['queries']
    if qid:
        return [x for x in queries if x.get("id", "").startswith(qid)]
    return queries


@pytest.mark.parametrize("query", param_queries("data-ipi"))
def test_get_data(istat, query):

    harn_query(istat, qid)
    raise NotImplementedError
