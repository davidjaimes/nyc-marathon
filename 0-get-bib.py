import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests as rq
from tqdm import tqdm
import glob as gl
from time import sleep

year = 2018
token = '6112c32703f442f0'
url = 'https://results.nyrr.org/api/runners/finishers-filter'
raceYear = np.array([2018, 2017, 2016, 2015, 2014, 2013, 2011, 2010, 2009, 2008, 2007, 2006, 2005, 2004, 2003, 2002, 2001, 2000, 1999, 1998, 1997, 1996, 1995, 1994, 1993, 1992, 1991, 1990, 1989, 1988, 1987, 1986, 1985, 1984, 1983, 1982, 1981, 1980, 1979, 1978, 1977, 1976, 1975, 1974, 1973, 1972, 1971, 1970])
eventCode = np.array(['M2018', 'M2017', 'M2016', 'M2015', 'M2014', '40', '108', 'b01107', 'a91101', 'a81102', 'a71104', 'a61105', 'a51106', 'a41107', 'NYC2003', 'NYC2002', 'b11106', 'NYC2000', '991107', '981101', '971102', '961103', '951112', '941106', '931114', '921101', '911103', '901104', '891105', '881106', '871101', '861102', '851027', '841028', '831023', '821024', '811025', '801026', '791021', '781022', '771023', '761024', '750928', '740929', '730930', '721001', '710919', '700913'])
totalItems = np.array([52706, 50641, 51274, 49461, 50395, 50134, 47238, 44976, 43545, 38099, 38605, 37880, 36863, 36565, 34735, 31838, 23662, 29372, 31791, 31539, 30434, 28181, 26753, 29732, 26577, 27767, 25775, 23739, 24572, 22317, 21097, 19571, 15737, 14470, 14445, 13566, 13205, 12483, 10455, 8549, 3664, 1539, 334, 259, 105, 92, 156, 55])
headers = {'origin': 'https://results.nyrr.org', 'accept-encoding': 'gzip, deflate, br','accept-language': 'en-US,en;q=0.9', 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36', 'content-type': 'application/json;charset=UTF-8', 'accept': 'application/json, text/plain, */*', 'referer': 'https://results.nyrr.org/', 'authority': 'results.nyrr.org', 'cookie': '__cfduid=de36b16bb4d0acc47786cf2b9997d54d71560818244; _gcl_au=1.1.1592280569.1560818247; _ga=GA1.2.190767401.1560818247; _gid=GA1.2.207092622.1560818247; _fbp=fb.1.1560818247391.393523298; ARRAffinity=851d7f30361f6d0723ca121b2bd6d6718ac17eb6e82712885034bbf6fcabc392; _ga=GA1.3.190767401.1560818247; _gid=GA1.3.207092622.1560818247; ads-disabled=0; __atuvc=22%7C25; __atuvs=5d084f2e8e8264cb005', 'token': f'{token}'}
yearEvent = eventCode[raceYear == year]
getItems = totalItems[raceYear == year]
if getItems[0] <= 5000:
    pages = 1
else:
    pages = np.ceil(getItems[0] / 5000)
for number in tqdm(range(1, int(pages + 1))):
    placeFrom = 5000 * (number - 1)
    data = f'{{"eventCode":"{yearEvent[0]}","runnerId":null,"searchString":null,"countryCode":null,"stateProvince":null,"city":null,"teamName":null,"gender":null,"ageFrom":null,"ageTo":null,"overallPlaceFrom":"{placeFrom}","overallPlaceTo":null,"paceFrom":null,"paceTo":null,"overallTimeFrom":null,"overallTimeTo":null,"gunTimeFrom":null,"gunTimeTo":null,"ageGradedTimeFrom":null,"ageGradedTimeTo":null,"ageGradedPlaceFrom":null,"ageGradedPlaceTo":null,"ageGradedPerformanceFrom":null,"ageGradedPerformanceTo":null,"handicap":null,"sortColumn":"overallTime","sortDescending":false,"pageIndex":1,"pageSize":5000}}'

    response = rq.post(url, headers=headers, data=data)
    json = response.json()['response']['items']
    if number == 1:
        df1 = pd.DataFrame(json)
    else:
        df2 = pd.DataFrame(json)
        df1 = pd.concat([df1, df2])
    print('Number of rows: ', len(df1))

df = df1.reset_index().drop(columns=['index'])
df = df.infer_objects()
print(len(df))
table = pa.Table.from_pandas(df)
pq.write_table(table, f'all-bibs/nyc-marathon-{year}-bibs.parquet')
