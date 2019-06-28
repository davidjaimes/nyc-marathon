import pandas as pd
import glob as gl
import pyarrow.parquet as pq
import numpy as np
import pyarrow as pa

for year in range(1970, 2019):
    if year == 2012:
        continue
    runner = gl.glob(f'eventRunner/*{year}*')
    details = gl.glob(f'resultDetails/*{year}*')
    t1 = pq.read_table(runner[0])
    t2 = pq.read_table(details[0])
    df1 = t1.to_pandas()
    df2 = t2.to_pandas()

    colnames = list(df1.columns.values) + list(df2.columns.values)
    u, c = np.unique(colnames, return_counts=True)
    df2 = df2.drop(columns=list(u[c > 1]))
    df = pd.concat([df1, df2], axis=1)

    df = df.infer_objects()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, f'parquet/nyc-marathon-{year}.parquet')
