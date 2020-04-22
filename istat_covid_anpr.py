import pandas as pd
import zipfile
from io import BytesIO

data = requests.get(
    "https://www.istat.it/it/files//2020/03/dati-settimanali-comune-16aprile.zip"
)
z = zipfile.ZipFile(BytesIO(data.content))
dest = z.extract(next(x for x in z.filelist if x.filename.endswith(".xlsx")))
df = pd.read_excel(dest)

cols_to_sum = [
    "MALES_2015",
    "MALES_2016",
    "MALES_2017",
    "MALES_2018",
    "MALES_2019",
    "MALES_2020",
    "FEMALE_2015",
    "FEMALE_2016",
    "FEMALE_2017",
    "FEMALE_2018",
    "FEMALE_2019",
    "FEMALE_2020",
    "TOTAL_2015",
    "TOTAL_2016",
    "TOTAL_2017",
    "TOTAL_2018",
    "TOTAL_2019",
    "TOTAL_2020",
]

aggregation_function = dict.fromkeys(cols_to_sum, "sum")
dfg = df.groupby("NUTS 3").agg(aggregation_function)
dfg.to_csv("istat_morti_per_provincia_per_fasce_deta.csv")
