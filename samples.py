"""
A list of example queries to istat sdmx interface.

Each ISTAT dataset is associated to a dataflow.
"""
import pandas as pd
from pandasdmx import Request


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
    # Ensure agency is Istat. Remember, this
    # is just a sample code :)
    # query["agency"] = "IT1"

    res = client.data(**query)
    # Parse the response to data frame
    df = res.write()
    return df


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


def ipi_dump_data(data):
    import csv

    with open("istat_ipi.csv", "wt") as fh:
        writer = csv.writer(fh)
        writer.writerows(data)


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
