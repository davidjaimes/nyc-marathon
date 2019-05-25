import glob as gl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

fnames = gl.glob("*.parquet")
fnames = sorted(fnames)
for i, f in enumerate(fnames):
    t = pq.read_table(f)
    if i == 0:
        df1 = t.to_pandas()
    else:
        df1 = pd.concat([df1, t.to_pandas()])
df = df1.reset_index().drop(columns=['index'])
table = pa.Table.from_pandas(df)
pq.write_table(table, 'nyc-marathon-2002-eventRunner.parquet')
