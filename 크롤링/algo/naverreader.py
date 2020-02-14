"""
개발 환경 : windows 10 / Anaconda3 python=3.6 64BIT
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
from finance_util import link_list
## 제가 올린 파일들 중 finance_util은 크롤링하면서 필요하다 싶은 함수를 혼자서 만드는데, 그것들을 모두 모아 놓은 파일입니다.
from datetime import datetime
import time

class naverreader:
    """
    Naver 주식에서 종목을 가져오는 클래스입니다. 저는 크롤링 대상 웹사이트별로 클래스의 구조를 가능하면 비슷하게 두려고 합니다.
    1. 개별 주식 데이터 가져오는 메서드
    3. 해당 사이트에 있는 전 종목을 가져오는 메서드 : 1.개별 주식 데이터 가져오는 메서드를 전 종목에 걸쳐 돌려주는 메서드입니다.
    4. 업데이트 할 수 있는 메서드 : 3번을 통해 자신의 컴퓨터에 데이터를 보관하고 있다면, 매일 새로운 데이터를 추가해주는 메서드입니다.
    5. 현재 데이터 베이스를 조회하는 메서드 : 지금까지 모아둔 데이터 베이스를 조회합니다. 업데이트가 되어 있다면 바로 파일을 주고, 업데이트가
    되어 있지 않으면 4.의 업데이트를 거친 후 데이터를 보여줍니다.
    6. 기타 : 필요한 다른 데이터들(full_code.pickle : 종목명과 종목코드를 담고 있는 DataFrame. Naver_companies : 네이버에 있는 모든
    회사 종목명과 코드를 담고 있는 DataFrame.)
    """
    def __init__(self):
        self.full_code = pd.read_pickle('./full_code.pickle')
        self.companies = pd.read_pickle('./naver_companies.pickle')

    def get_stock_price(self, num_code, days):
        """
        - 종목코드와 오늘로부터의 일자를 입력하면 한 종목의 과거 데이터를 가져올 수 있습니다.
        * 이 주가 데이터는 액면분할까지는 조정해주지만 배당이나 무상증자를 조정하지는 않는 것 같습니다.
        ** 거래 정지 종목인 경우에는 주가가 0으로 뜬다는 문제가 있기 때문에 데이터 전처리가 중요합니다.
        :param num_code: (str) '005930' 형식의 기업 코드입니다.
        :param days: (int) 이 함수를 실행하는 날짜부터 며칠 전까지의 데이터를 가져올 것인지 결정합니다. 365를 입력해도 1년이
        되지는 않습니다. 거래일 기준이기 떄문에 1년 보다 더 많은 데이터가 들어옵니다. 만약 1년치가 필요하다면 252가 적당할 듯 싶습니다.
        :return: (list) 데이터 프레임 형식이 아니라 리스트로 반환합니다. 전 종목 데이터를 가져올 때 종목마다 데이터프레임으로 바꾸면
        속도가 매우 느려지기 때문에 리스트로 반환합니다.
        """
        url = 'https://fchart.stock.naver.com/sise.nhn' # 네이버 금융의 차트가 있는 화면입니다.
        payload = {
            'symbol': num_code,
            'timeframe': 'day',
            'count': days,
            'requestType': 0
        }
        req = requests.get(url, params=payload).text
        soup = bs(req, 'lxml')
        row_list = []
        for item in soup.findAll('item'):
            row = item['data'].split('|')
            row.append('A' + num_code)
            row_list.append(row)

        return row_list


    def get_naver_total(self, days): # 여기서 얻은 데이터는 관리 종목인 경우 값이 0이다.
        """
        위에서의 get_stock_price 메서드를 모든 종목의 주식에 대해 적용합니다. 이 방법을 한 번 돌릴 때마다 시간이 굉장히 오래 걸리기
        때문에 처음 데이터 베이스를 구축할 때 한번 돌리고, 추후에는 업데이트를 통해 데이터를 모은다는 개념으로 접근하였습니다.
        :param days: (int) get_stock_price의 인수 days와 같은 개념입니다.
        :return: (pandas.DataFrame) 전 종목의 과거 days의 모든 값을 줍니다.
        """
        super_list = []
        for i, code in enumerate(self.companies['short_code']):
            try:
                short_code = code
                print(code) # 시간이 오래 걸리므로 현재 어디까지 진행되었는지 보기 위해 넣었습니다.
                num_code = short_code[1:]
                super_list.append(self.get_stock_price(num_code, days=days))
                if i==9: break # -> 전 종목을 다 돌리려면 시간이 오래 걸리므로, 테스트용으로 일단 10종목만 가져와볼 때 쓰시면 됩니다.
            except KeyError:
                pass

        linked = pd.DataFrame(link_list(super_list))
        linked.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'short_code']
        linked['date'] = linked['date'].apply(lambda x: pd.to_datetime(x))
        linked[['open', 'high', 'low', 'close', 'volume']] = linked[
            ['open', 'high', 'low', 'close', 'volume']].applymap(lambda x: int(x))
        linked.set_index('date', inplace=True)

        return linked

    def get_current_price(self):
        """
         매일 자신이 가지고 있는 데이터 베이스를 업데이트 하기 위해서 전 종목의 현재가를 보여줍니다. 이 가격은 장이 종료된 후에는
         종가가 됩니다. 그래서 장 종료 이후 그 날 하루의 종가를 모으고, 기존의 데이터와 붙이면, 업데이트가 됩니다.
        :return: (pandas.DataFrame) 전 종목의 현재가.
        """

        url = 'https://finance.naver.com/sise/field_submit.nhn?menu=market_sum&returnUrl=http%3A%2F%2Ffinance.naver' \
              '.com%2Fsise%2Fsise_market_sum.nhn%3F%26page%3D1&fieldIds=quant&fieldIds=open_val&fieldIds=high_val' \
              '&fieldIds=low_val '
        payload = dict()
        company_pages = []
        for sosok in [0, 1]:  # 0은 코스피, 1은 코스닥
            payload['sosok'] = sosok
            for page_num in range(1, 33):  # 마지막 페이지는 거의 33
                print('페이지 수 : {}'.format(page_num))
                payload['page'] = page_num
                req = requests.get(url, params=payload).text
                company_pages.append(pd.read_html(req)[1][['시가', '고가', '저가', '현재가', '거래량', '종목명']])
                if page_num == 1: break
            if sosok == 0: break

        df = pd.concat(company_pages)
        current_prices = df
        current_prices.dropna(inplace=True)
        current_prices.reset_index(drop=True, inplace=True)
        current_prices.index = [datetime.today()] * len(current_prices)
        current_prices.columns = ['open', 'high', 'low', 'close', 'volume', 'codeName']
        return current_prices

    def load_data(self): ## 현재가만 구할 수 있으며 종가를 완벽하게 저장하기 위해서는 6시가 넘어야 한다.
        self.saved = pd.read_pickle('./10year.pickle')
        result = self.saved
        if self.saved.index.unique().sort_values()[-1] < pd.to_datetime(datetime.today().strftime('%Y%m%d')):## 조건문을 안 넣으면 중첩된 날짜가 생길 우려
            print('업데이트 필요')
            current_prices = self.get_current_price()
            full_code = self.full_code
            merged = current_prices.merge(full_code, on=['codeName'])
            adjust = merged[['open', 'high', 'low', 'close', 'volume', 'short_code']].copy()
            adjust.index = [pd.to_datetime(datetime.today().strftime('%Y%m%d'))] * len(adjust)
            result = pd.concat([self.saved, adjust])
            result.to_pickle('./10year.pickle')

        else:
            print('업데이트 사항 없음')
        return result


    def get_naver_companies(self):
        """
        네이버의 시가 총액을 볼 수 있는 화면에서 네이버 금융에서 다루고 있는 전 종목을 가져오려고 합니다. 네이버 금융에서는 코스피,
        코스닥은 다루고 있지만 코넥스, K-OTC 등의 종목들은 다루고 있지 않습니다.
        """
        url = 'https://finance.naver.com/sise/sise_market_sum.nhn'
        payload = dict()
        company_pages = []
        for j in [0, 1]:  # 0은 코스피, 1은 코스닥
            for i in range(1, 33):
                payload['sosok'] = j
                payload['page'] = i
                req = requests.get(url, params=payload).text
                company_pages.append(pd.read_html(req)[1])

        df = pd.concat(company_pages)
        codenames = df['종목명']
        codenames.columns = ['codeName']
        codenames.dropna(inplace=True)
        codenames.reset_index(drop=True, inplace=True)

        codenames_df = pd.DataFrame(codenames)
        companies = codenames_df.merge(self.full_code, on=['codeName'])
        return companies


if __name__ == "__main__":
    st = time.time() # 실행시간 체크를 위해 시간을 찍습니다.

    naver = naverreader()
    print(naver.full_code.head()) ## 코드와 종목명을 담고 있는 데이터 프레임입니다.
    print(naver.companies.head()) ## 네이버에 등록된 회사들입니다.

    samsung = naver.get_stock_price(num_code='005930', days=250) # 오늘로부터 1년치 삼성전자 데이터
    print(samsung[:4])

    try:
        full_data = naver.load_data()
        print(full_data)
    except Exception as e:
        print(e)
    # 내 컴퓨터에 보관된 데이터를 가져오는 메서드입니다. 하지만 전 종목 데이터를 아직 수집하지 않은 경우 파일불러오기를 할 때 오류가 납니다.
    # [Errno 2] No such file or directory: './10year.pickle'
    # 따라서 아래의 get_naver_total 함수를 이용해 result를 얻고
    # result.to_pickle('./10year.pickle')
    # 을 이용하여 저장을 해두시면 load_data()가 가능합니다.


    try:
        crawling_start = time.time()
        result = naver.get_naver_total(2500) ## 대략 10년치를 가져오는 메서드입니다. 일단 테스트용으로 10개만 돌려봅니다.
        crawling_end = time.time()
        processing_time = round(crawling_end-crawling_start, 2)
        print("크롤링 하는데 소요된 시간 : {}초".format(processing_time))
        # 제 컴퓨터는 10종목을 뽑는데 4.38초가 나오네요.
        # 총 2500 종목의 10년치 데이터를 다 가져온다고 생각했을 때 4.38초를 250회 시행하므로 250*4.38/60=18.25분이 소요되겠군요.
        # GPU를 쓸 수 있다면 성능은 훨씬 향상됩니다. google colab에서 제공하는 GPU를 사용하니 실행시간이 1/3로 줄어들었습니다.
        print(result)
    except Exception as e:
        print(e)

    print('Wall time : {}분'.format((time.time()-st)/60))