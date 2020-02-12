import requests
import pandas as pd
import time
"""
investing.com이라는 사이트를 크롤링하는 예제입니다.
"""
url = 'https://www.investing.com/stock-screener/Service/SearchStocks'

header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/79.0.3945.130 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

payload = {
    'country[]': '5',  ## US-country code
    'sector': '7,5,12,3,8,9,1,6,2,4,10,11', ## all sectors
    'industry': '81,56,59,41,68,67,88,51,72,47,12,8,50,2,71,9,69,45,46,13,94,102,95,58,100,101,87,31,6,38,79,30,77,'
                '28,5,60,18,26,44,35,53,48,49,55,78,7,86,10,1,34,3,11,62,16,24,20,54,33,83,29,76,37,90,85,82,22,14,'
                '17,19,43,89,96,57,84,93,27,74,97,4,73,36,42,98,65,70,40,99,39,92,75,66,63,21,25,64,61,32,91,52,23,'
                '15,80', ## all industries
    'equityType': 'ORD,DRC,Preferred,Unit,ClosedEnd,REIT,ELKS,OpenEnd,Right,ParticipationShare,CapitalSecurity,'
                  'PerpetualCapitalSecurity,GuaranteeCertificate,IGC,Warrant,SeniorNote,Debenture,ETF,ADR,ETC,ETN',
    'pn': 1,
    'order[col]': 'eq_market_cap',
    'order[dir]': 'd'
}

def run(pages):
    json_list = []
    for i in range(1, pages):
        payload['pn'] = i
        req = requests.post(url, data = payload, headers = header).json()
        print('남은 페이지 수 : {}개'.format(pages - i))
        json_list += req['hits']

    df = pd.DataFrame(json_list)
    return df

pages = int(10914/50) + 1
df = run(pages)
