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


istat = Request("ISTAT")
res = istat.data(
    agency="IT1",
    resource_id="115_333",
    params={"startPeriod": "2020"},
    key={"ADJUSTMENT": ["N", "Y"]},
)
