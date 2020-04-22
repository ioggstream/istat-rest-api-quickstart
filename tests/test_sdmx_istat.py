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


def test_datastructure(istat):
    ret = istat.datastructure()
    assert "DCAR_ATT_NOTAR" in ret.structure


def test_dataflow(istat):
    ret = istat.dataflow()
    assert "101_1015" in ret.dataflow
    assert "DCSP_COLTIVAZIONI" in ret.dataflow["101_1015"]
