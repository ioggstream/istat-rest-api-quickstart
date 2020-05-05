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
    "MASCHI_2015",
    "MASCHI_2016",
    "MASCHI_2017",
    "MASCHI_2018",
    "MASCHI_2019",
    "MASCHI_2020",
    "FEMMINE_2015",
    "FEMMINE_2016",
    "FEMMINE_2017",
    "FEMMINE_2018",
    "FEMMINE_2019",
    "FEMMINE_2020",
    "TOTALE_2015",
    "TOTALE_2016",
    "TOTALE_2017",
    "TOTALE_2018",
    "TOTALE_2019",
    "TOTALE_2020",
]

aggregation_function = dict.fromkeys(cols_to_sum, "sum")
dfg = df.groupby("NOME_PROVINCIA").agg(aggregation_function)
dfg.to_csv("istat_morti_per_provincia_per_fasce_deta.csv")
