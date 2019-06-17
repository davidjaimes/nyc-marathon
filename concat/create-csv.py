import pandas as pd
import glob as gl
import pyarrow.parquet as pq
import numpy as np

year = 2018
fnames = gl.glob(f'../parquet/*{year}*')
t1 = pq.read_table(fnames[0])
t2 = pq.read_table(fnames[1])
df1 = t1.to_pandas()
df2 = t2.to_pandas()

colnames = list(df1.columns.values) + list(df2.columns.values)
u, c = np.unique(colnames, return_counts=True)
df2 = df2.drop(columns=list(u[c > 1]))
df = pd.concat([df1, df2], axis=1)

df = df.infer_objects()
df.to_csv(f'nyc-marathon-{year}.csv')
