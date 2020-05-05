from pandasdmx import Request, to_pandas
from pathlib import Path

# Show http traces.
import http.client as http_client
import logging
import csv
from requests import get
from io import StringIO
from samples import *

http_client.HTTPConnection.debuglevel = 2
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

log = logging.getLogger()


istat = Request("ISTAT")


def csv_to_pandas(istat, query):
    if "params" in query:
        query["params"]["detail"] = "serieskeysonly"
    else:
        query["params"] = {"detail": "serieskeysonly"}

    metadata = istat.data(**query)
    query_url = metadata.response.url.replace("&detail=serieskeysonly", "")

    txt = get(
        query_url, headers={"accept": "application/vnd.sdmx.data+csv"}
    ).content.decode("utf-8")
    return pd.read_csv(StringIO(txt))


def query():
    fields = [
        "FREQ",
        "ATECO_2007",
        "ADJUSTMENT",
        "PAESE_RES",
        "TIPO_ESERCIZIO",
        "INDS",
        "TYPE_LOC",
        "TIME_PERIOD",
        "OBS_VALUE",
    ]

    for residenti in ("IT",):
        query = dict(
            resource_id="122_54",
            key={
                "ADJUSTMENT": "N",
                "PAESE_RES": residenti,
                "TIPO_ESERCIZIO": ["ALL", "HOTELLIKE", "OTHER"],
                "INDS": ["AR", "NI"],
                "TYPE_LOC": "ALL",
                "FREQ": "M",
                "ATECO_2007": ["551", "551_553", "552_553"],
            },
            params={
                "startPeriod": "2000-01",
                "endPeriod": "2020-02",
                "dimensionAtObservation": "AllDimensions",
            },
        )
        df = csv_to_pandas(istat, query)
        df[fields].to_csv(f"turismo_2000_{residenti}.csv", index=False)


def get_constraint(record_id):
    df = istat.dataflow(resource_id="122_54")
    c = next(iter(df.constraint.values()))
    for r in c.data_content_region:
        for m in r.member.items():
            pass


def test_presenze_stranieri():
    query()
