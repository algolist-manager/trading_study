#This is function for DART Crawling
#사용방법: 
#This depend on pykrx
    #pip install pykrx
    #and need API key from opendart.fss.or.kr

#getaccounts(stock, year) 
    #stock -> 6 digit string
    #year -> year as integer
#the price (and other market information) is as of filedate


import pandas as pd
import urllib.request
import os
import json
import re
from datetime import date, timedelta
import time
import codecs
from pykrx import stock

key = "" #Change key using your own

def DUMPJSON(inputdict, filename):
    d = json.dumps(inputdict, indent=4, ensure_ascii=False)
    c = open(filename, "w")
    print(d, file = c)
    c.close()

#Function to get list file for A001 
def A001list(year):
    from datetime import date, timedelta
    formtype = "A001"
    sdate = date(year, 1, 1)     # start date
    edate = date(year, 12, 31)   # end date
    delta = edate - sdate        # as timedelta
    dlist = []
    for i in range(delta.days + 1):
        day = sdate + timedelta(days=i)
        dlist.append(day)
    T = []
    for d in dlist:
        date = d.strftime("%Y%m%d")
        print(date)
        URL = """
            https://opendart.fss.or.kr/api/list.json?crtfc_key={}&pblntf_detail_ty={}&bgn_de={}&end_de={}&page_no=1&page_count=1000
            """.format(key, formtype, date, date)
        K  = urllib.request.urlopen(URL).read().decode('utf-8')
        K0 = json.loads(K)
        if K0['status'] != '000':
            continue
        T = T + K0['list']
        if K0['total_page'] > 1:
            for page in range(2, int(K0['total_page'])+1):
                URL = """
                    https://opendart.fss.or.kr/api/list.json?crtfc_key={}&pblntf_detail_ty={}&bgn_de={}&end_de={}&page_no={}&page_count=1000
                    """.format(key, formtype, date, date, page)
                K  = urllib.request.urlopen(URL).read().decode('utf-8')
                K0 = json.loads(K)
                T = T + K0['list']
        #time.sleep(1)
    OPATH = "./Data"
    if not os.path.exists(OPATH):
        os.makedirs(OPATH)
    Rfile = os.path.join(OPATH, "S1_GetDartList_{}_{}.json".format(formtype, year))
    DUMPJSON(T, Rfile)

#Function to get basic accounts (as following) for given stock and year
    #자산총계
    #유동자산
    #유동부채
    #부채총계
    #자본총계
    #매출액
    #영업이익
    #당기순이익

def getaccounts(snum, year):
    #year is the year of filing (i.e., 2018 fye --> 2019)
    #stock = str(stock)
    acclist = ["자산총계", "부채총계", '자본총계', "유동자산", '유동부채', '매출액', '영업이익', '당기순이익']
    OPATH = "./Data"
    Rfile = os.path.join(OPATH, "S1_GetDartList_{}_{}.json".format("A001", year))
    if not os.path.exists(Rfile):
        print("리스트 파일을 만듭니다")
        A001list(year)
    kdict = json.load(codecs.open(Rfile, 'r', 'utf-8-sig'))
    U  = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?"
    for k in kdict:
        if k['stock_code'] != snum:
            continue
        if re.search(r'^\[', k['report_nm']): #정정공시는 제외
            continue
        conn  = k['corp_code']
        date  = k['rcept_dt']
        name  = k['corp_name']
        iyear = re.search(r'\d{4}', k['report_nm'])[0] #year in the reporting is needed to get the reports
        A = "crtfc_key={}&corp_code={}&bsns_year={}&reprt_code={}".format(key, conn, iyear, "11011")
        if k['stock_code'] == snum:
            break
    L1 = urllib.request.urlopen(U+A).read().decode('utf-8')
    if 'list' not in L1:
        print("Stock {} do not have required information")
    else:
        d1 = stock.get_market_ohlcv_by_date(date, date, snum).reset_index()
        time.sleep(2)
        d2 = stock.get_market_fundamental_by_date(date, date, snum).reset_index()
        L2 = json.loads(L1)['list']
        rdict = {}
        rdict['stock'] = snum
        rdict['filedate'] = date
        rdict['firmname'] = name
        for l in L2:
            if l['account_nm'] in ['자산총계']:
                rdict['fiscal_year_end'] = re.sub('\.', '', l['thstrm_dt'])[:-3]
            if l['account_nm'] in acclist:
                for acc in acclist:
                    if l['account_nm'] == acc:
                        rdict[acc] = re.sub(',', '', l['thstrm_amount'])
        b = pd.DataFrame([rdict]).reset_index()
        c = pd.concat([b, d1], axis = 1)
        d = pd.concat([c, d2], axis = 1)
        d = d.reset_index()
        d = d.drop(['index', 'level_0'], axis = 1)
    return(d)

