"""
A list of example queries to istat sdmx interface.

Each ISTAT dataset is associated to a dataflow.
"""
import pandas as pd
from pandasdmx import Request
import pandasdmx as sdmx
import logging
import csv

log = logging.getLogger()


def get_dataflows(client, label=None) -> list:
    """Return a list of dataflows (resource_id) and their labels.
       If one label is specified, return matching dataflow(s)
    :param client: the api client
    :param label: a label string, eg. DCCN_TNA
    :return a list of dataflows.
    """
    dataflows = client.dataflow()
    return [
        (v.structure.id, k)
        for k, v in dataflows.dataflow.items()
        if not label or v.structure.id == label
    ]


def get_dataset(client, query) -> pd.DataFrame:
    """
    Returns a data series gathered from the endpoint.
    :param client: the api client
    :param query: a query dict like the following

        {
         'resource_id': '115_333',
         'agency': 'IT1',
         'params': {'startPeriod': '2019-10'},
         'key': {'ADJUSTMENT': ['N', 'Y']}
        }

    :return:
    """
    # as of today 2020-04-20 ISTAT SDMX REST endpoint requires
    # the agency to be present
    if "agency" not in query:
        log.warning("If this query does not succeed, try to add the agency param")
    if sdmx.__version__ > "0.9":
        query.pop("agency", None)
    res = client.data(**query)

    # Parse the response to data frame
    if sdmx.__version__ > "0.9":
        return sdmx.writer.write(res)
    return res.write()


def flatten_data(df):
    """
    An example function to process a dataframe returned by get_dataset.
    :param df:
    :return:
    """
    fields = None
    for period, data in df.iterrows():
        # Ensures all multi-indexed data uses the same fields.
        if fields:
            assert fields == data.keys().names
        else:
            fields = data.keys().names
            yield ("date",) + tuple(fields) + ("value",)

        for k, v in data.to_dict().items():

            # Use only 4-levels ateco
            ateco_2007 = k[fields.index("ATECO_2007")]
            if len(ateco_2007) < 4:
                continue

            yield (f"{period}",) + k + (v,)


def to_csv(data, fpath):
    with open(fpath, "wt") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerows(data)


def ipi_dump_data(data):
    to_csv(data, "istat_ipi.csv")


def get_produzione_industriale_dal_2000():
    istat = Request("ISTAT")
    df = get_dataset(
        client=istat,
        query={
            "resource_id": "115_333",
            "params": {"startPeriod": "2000-01"},
            "key": {"ADJUSTMENT": ["N", "Y"], "IND": "IND_PROD2"},
        },
    )
    data = flatten_data(df)
    ipi_dump_data(data)


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


def get_presenze_estero():
    df = sdmx.to_pandas(
        istat.data(
            resource_id="122_54",
            key={"ADJUSTMENT": "N", "PAESE_RES": "WRL_X_ITA"},
            params={"startPeriod": "2000-01", "endPeriod": "2000-02"},
        )
    )
