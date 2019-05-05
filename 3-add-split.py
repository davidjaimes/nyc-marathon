import glob as gl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

fnames = gl.glob("*.csv")
fnames = sorted(fnames)
for i, f in enumerate(fnames):
    df = pd.read_csv(f)
    if i == 0:
        df1 = df
    else:
        df1 = pd.concat([df1, df])
df = df1.reset_index().drop(columns=['index', 'Unnamed: 0'])
table = pa.Table.from_pandas(df)
pq.write_table(table, 'nyc-marathon-2010-resultDetails.parquet')
