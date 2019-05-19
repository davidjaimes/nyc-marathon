import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests as rq
from tqdm import tqdm

url = 'https://results.nyrr.org/api/runners/eventRunner'
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
raceYear = np.array([2018, 2017, 2016, 2015, 2014, 2013, 2011, 2010, 2009, 2008, 2007, 2006, 2005, 2004, 2003, 2002, 2001, 2000, 1999, 1998, 1997, 1996, 1995, 1994, 1993, 1992, 1991, 1990, 1989, 1988, 1987, 1986, 1985, 1984, 1983, 1982, 1981, 1980, 1979, 1978, 1977, 1976, 1975, 1974, 1973, 1972, 1971, 1970])
eventCode = np.array(['M2018', 'M2017', 'M2016', 'M2015', 'M2014', '40', '108', 'b01107', 'a91101', 'a81102', 'a71104', 'a61105', 'a51106', 'a41107', 'NYC2003', 'NYC2002', 'b11106', 'NYC2000', '991107', '981101', '971102', '961103', '951112', '941106', '931114', '921101', '911103', '901104', '891105', '881106', '871101', '861102', '851027', '841028', '831023', '821024', '811025', '801026', '791021', '781022', '771023', '761024', '750928', '740929', '730930', '721001', '710919', '700913'])
number = 6
year = 2004
yearEvent = eventCode[raceYear == year]
print(f'Year {year} with event code: {yearEvent[0]}')
start = int(f'{number - 1}0000')
end = int(f'{number}0000')
for bib in tqdm(range(start, end)):
    d = f'{{"eventCode":"{yearEvent[0]}","bib":"{bib}"}}'
    r = rq.post(url, headers=h, data=d)
    json = r.json()['response']
    if json == None:
        if bib == start:
            start += 1
        continue
    elif bib == start:
        s1 = pd.Series(json)
        print(f'Save bib number: {bib}')
    else:
        s2 = pd.Series(json)
        s1 = pd.concat([s1, s2], axis=1)
df = pd.DataFrame(s1).T.reset_index().drop(columns=['index'])
df = df.infer_objects()
table = pa.Table.from_pandas(df)
pq.write_table(table, f'bib{number}.parquet')
