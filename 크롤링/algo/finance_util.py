from datetime import datetime, timedelta
import time
import pandas as pd
from bs4 import BeautifulSoup as bs

def seq_mul(list_):
    base = 1
    for com in list_:
        base *= com
    return base


def naming_return(str_listed_data):
    result = []
    for com in list(str_listed_data):
        com = com + '_return'
        result.append(com)
    return result


def tr_date(date):
    year, month, day = date[:4], date[4:6], date[6:]
    tr_date = month + '/' + day + '/' + year
    return tr_date


def tr_date_1(date_1):
    tr_month = {
        'Jan': 1,
        'Feb': 2,
        'Mar': 3,
        'Apr': 4,
        'May': 5,
        'Jun': 6,
        'Jul': 7,
        'Aug': 8,
        'Sep': 9,
        'Oct': 10,
        'Nov': 11,
        'Dec': 12
    }
    year, month, day = int(date_1[-4:]), tr_month[date_1[:3]], int(date_1[4:6])
    tr_date_var = datetime(year, month, day)
    return tr_date_var


def to_date(str_date):
    spl1 = str_date.split('/')
    spl2 = []
    for date_com in spl1:
        spl2.append(int(date_com))
    return datetime(spl2[0], spl2[1], spl2[2])

def to_unix(str_type):
    str_type = pd.to_datetime(str_type) + timedelta(hours=9)
    str_type = int(time.mktime(str_type.timetuple()))
    return str_type


def cum_mul(series):
    result = series.copy()
    for i in range(len(series)):
        result.iloc[i] = seq_mul(series.values[:i+1])
    return result


def read_xml(xml):
    soup = bs(xml, 'lxml')
    column = []
    for tag in soup.find('result').findAll(True):
        column.append(str(tag.name))
    row_list = []
    for result in soup.findAll('result'):
        row = []
        for col in column:
            row.append(result.find(col)['value'])
        row_list.append(row)

    df = pd.DataFrame(row_list, index=range(len(soup.findAll('result'))), columns=column)

    return df

def link_list(multi_layered):
    linked = []
    for i in range(len(multi_layered)):
        linked += multi_layered[i]
    return linked