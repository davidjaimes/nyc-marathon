import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests as rq
from tqdm import tqdm

url = 'https://results.nyrr.org/api/runners/resultDetails'
h = {
    'origin': 'https://results.nyrr.org',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'en-US,en;q=0.9',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
    'content-type': 'application/json;charset=UTF-8',
    'accept': 'application/json, text/plain, */*',
    'referer': 'https://results.nyrr.org/',
    'authority': 'results.nyrr.org',
    'cookie': '__cfduid=d1123b77692189442e29a6ae78e6d17391554342089; _gcl_au=1.1.1614746071.1554342091; _ga=GA1.2.1285202861.1554342092; _gid=GA1.2.887065544.1554342092; _fbp=fb.1.1554342092334.1341848325; ARRAffinity=da4c4ff244aae03ae3c7548f243f7b2b5c22567a56a76a62aaebcf4acc7f0bf8; _ga=GA1.3.1285202861.1554342092; _gid=GA1.3.887065544.1554342092; __atuvc=2%7C14; __atuvs=5ca560d54f9be40a001',
    'token': 'ebe04e9c08064538',
}

t = pq.read_table('parquet/nyc-marathon-2018-eventRunner.parquet')
df = t.to_pandas()
for i, id in enumerate(tqdm(df['runnerId'][20000:30000])):
    d = f'{{"runnerId":{id}}}'
    r = rq.post(url, headers=h, data=d)
    json = r.json()['response']
    if json == None:
        if i == 0:
            start += 1
        continue
    elif i == 0:
        s1 = pd.Series(json)
        print(f'Save ID number: {id}')
    else:
        s2 = pd.Series(json)
        s1 = pd.concat([s1, s2], axis=1)
df = pd.DataFrame(s1).T.reset_index().drop(columns=['index'])
df = df.infer_objects()
df.to_csv('split3.csv')