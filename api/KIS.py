import zipfile
import requests
import json
import os
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'))

class KIS_base():
    '''
    Korea Investment & Securities open API base connector
    
    Parameters
    ----------


    Attributes 
    ----------

    '''
    def __init__(self):
        # private attributes: api key, api secret
        self.__app_key = os.environ['KIS_MOCK_APP_KEY']
        self.__app_secret = os.environ['KIS_MOCK_APP_SECRET']
        
        self.mock = None
        self.base_url = "https://openapivts.Koreainvestment.com:{}".format('29443' if self.mock else '9443')
        self.content_type = "application/json"
        self.access_token = self._get_access_token()

    def _get_url(self, path):
        url = f"{self.base_url}/{path}"
        
        return url 

    def _get_access_token(self):
        # information
        url = self._get_url(path = "oauth2/tokenP")
        headers = {"content-type": self.content_type}
        
        body = {
            "grant_type": "client_credentials",
            "appkey" : self.__app_key,
            "appsecret": self.__app_secret
        }

        res = requests.post(url, headers=headers, data=json.dumps(body))
        access_token = res.json()['access_token']
        
        return access_token

    def get_hashkey(self, data):
        '''
        TODO
        '''
        url = self.get_url(path = "uapi/hashkey")

        headers = {
            "content-Type": self.content_type,
            "appKey": self.__app_key,
            "appSecret": self.__app_secret,
        }

        res = requests.post(url, headers=headers, data=json.dumps(data))
        hashkey = res.json()["HASH"]

        return hashkey





class KIS_dataloader(KIS_base):
    '''
    TODO: docstring
    '''
    def __init__(self, mock = True):
        
        self.mock = mock
        super().__init__()
     
    def get_master_file(self, url):
        '''
        마스터 파일 다운로드함 (티커 정보 등...)
        '''
        res = requests.get(url)
        zfile = zipfile.ZipFile(BytesIO(res.content))
        zfile.extractall()


    def parse_kopsi_ticker(self):
        base_dir = os.getcwd()
        file_name = base_dir + "//kospi_code.mst"
        tmp_fil1 = base_dir + "//kospi_code_part1.tmp"
        tmp_fil2 = base_dir + "//kospi_code_part2.tmp"

        wf1 = open(tmp_fil1, mode="w")
        wf2 = open(tmp_fil2, mode="w")

        with open(file_name, mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0:len(row) - 228]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
                rf2 = row[-228:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        part1_columns = ['단축코드', '표준코드', '한글명']
        df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='utf-8')

        field_specs = [2, 1, 4, 4, 4,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 9, 5, 5, 1,
                    1, 1, 2, 1, 1,
                    1, 2, 2, 2, 3,
                    1, 3, 12, 12, 8,
                    15, 21, 2, 7, 1,
                    1, 1, 1, 1, 9,
                    9, 9, 5, 9, 8,
                    9, 3, 1, 1, 1
                    ]

        part2_columns = ['그룹코드', '시가총액규모', '지수업종대분류', '지수업종중분류', '지수업종소분류',
                        '제조업', '저유동성', '지배구조지수종목', 'KOSPI200섹터업종', 'KOSPI100',
                        'KOSPI50', 'KRX', 'ETP', 'ELW발행', 'KRX100',
                        'KRX자동차', 'KRX반도체', 'KRX바이오', 'KRX은행', 'SPAC',
                        'KRX에너지화학', 'KRX철강', '단기과열', 'KRX미디어통신', 'KRX건설',
                        'Non1', 'KRX증권', 'KRX선박', 'KRX섹터_보험', 'KRX섹터_운송',
                        'SRI', '기준가', '매매수량단위', '시간외수량단위', '거래정지',
                        '정리매매', '관리종목', '시장경고', '경고예고', '불성실공시',
                        '우회상장', '락구분', '액면변경', '증자구분', '증거금비율',
                        '신용가능', '신용기간', '전일거래량', '액면가', '상장일자',
                        '상장주수', '자본금', '결산월', '공모가', '우선주',
                        '공매도과열', '이상급등', 'KRX300', 'KOSPI', '매출액',
                        '영업이익', '경상이익', '당기순이익', 'ROE', '기준년월',
                        '시가총액', '그룹사코드', '회사신용한도초과', '담보대출가능', '대주가능'
                        ]

        df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

        df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

        # clean temporary file and dataframe
        del (df1)
        del (df2)
        os.remove(tmp_fil1)
        os.remove(tmp_fil2)
        os.remove('kospi_code.mst')

        return df
    

    def parse_kosdaq_ticker(self):
        base_dir = os.getcwd()
        file_name = base_dir + "//kosdaq_code.mst"
        tmp_fil1 = base_dir + "//kosdaq_code_part1.tmp"
        tmp_fil2 = base_dir + "//kosdaq_code_part2.tmp"

        wf1 = open(tmp_fil1, mode="w")
        wf2 = open(tmp_fil2, mode="w")

        with open(file_name, mode="r", encoding="cp949") as f:
            for row in f:
                rf1 = row[0:len(row) - 222]
                rf1_1 = rf1[0:9].rstrip()
                rf1_2 = rf1[9:21].rstrip()
                rf1_3 = rf1[21:].strip()
                wf1.write(rf1_1 + ',' + rf1_2 + ',' + rf1_3 + '\n')
                rf2 = row[-222:]
                wf2.write(rf2)

        wf1.close()
        wf2.close()

        part1_columns = ['단축코드','표준코드','한글종목명']
        df1 = pd.read_csv(tmp_fil1, header=None, names=part1_columns, encoding='utf-8')

        field_specs = [2, 1,
                    4, 4, 4, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1,
                    1, 1, 1, 1, 9,
                    5, 5, 1, 1, 1,
                    2, 1, 1, 1, 2,
                    2, 2, 3, 1, 3,
                    12, 12, 8, 15, 21,
                    2, 7, 1, 1, 1,
                    1, 9, 9, 9, 5,
                    9, 8, 9, 3, 1,
                    1, 1
                    ]

        part2_columns = ['증권그룹구분코드','시가총액 규모 구분 코드 유가',
                        '지수업종 대분류 코드','지수 업종 중분류 코드','지수업종 소분류 코드','벤처기업 여부 (Y/N)',
                        '저유동성종목 여부','KRX 종목 여부','ETP 상품구분코드','KRX100 종목 여부 (Y/N)',
                        'KRX 자동차 여부','KRX 반도체 여부','KRX 바이오 여부','KRX 은행 여부','기업인수목적회사여부',
                        'KRX 에너지 화학 여부','KRX 철강 여부','단기과열종목구분코드','KRX 미디어 통신 여부',
                        'KRX 건설 여부','(코스닥)투자주의환기종목여부','KRX 증권 구분','KRX 선박 구분',
                        'KRX섹터지수 보험여부','KRX섹터지수 운송여부','KOSDAQ150지수여부 (Y,N)','주식 기준가',
                        '정규 시장 매매 수량 단위','시간외 시장 매매 수량 단위','거래정지 여부','정리매매 여부',
                        '관리 종목 여부','시장 경고 구분 코드','시장 경고위험 예고 여부','불성실 공시 여부',
                        '우회 상장 여부','락구분 코드','액면가 변경 구분 코드','증자 구분 코드','증거금 비율',
                        '신용주문 가능 여부','신용기간','전일 거래량','주식 액면가','주식 상장 일자','상장 주수(천)',
                        '자본금','결산 월','공모 가격','우선주 구분 코드','공매도과열종목여부','이상급등종목여부',
                        'KRX300 종목 여부 (Y/N)','매출액','영업이익','경상이익','단기순이익','ROE(자기자본이익률)',
                        '기준년월','전일기준 시가총액 (억)','그룹사 코드','회사신용한도초과여부','담보대출가능여부','대주가능여부'
                        ]

        df2 = pd.read_fwf(tmp_fil2, widths=field_specs, names=part2_columns)

        df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True)

        # clean temporary file and dataframe
        del (df1)
        del (df2)
        os.remove(tmp_fil1)
        os.remove(tmp_fil2)
        os.remove('kosdaq_code.mst')

        return df
    

    def get_ticker(self, market = 'kospi'):
        '''
        티커 정보 들고옴 
        TODO: docstring
        '''
        base_url = 'https://new.real.download.dws.co.kr/common/master/'
        market_path = {
            'kospi': 'kospi_code.mst.zip',
            'kosdaq': 'kosdaq_code.mst.zip'
        }
        url = os.path.join(base_url, market_path.get(market))

        # download master data 
        self.get_master_file(url)

        # parse data
        if market == 'kospi': 
            ticker = self.parse_kopsi_ticker()

        elif market == 'kosdaq':
            ticker = self.parse_kosdaq_ticker()
        
        return ticker

    def current_price(self, stock_code):
        path = "uapi/domestic-stock/v1/quotations/inquire-price"
        url = self.get_url(path)

        # tr_id = Current stock price
        headers = {
            "Content_type": self.content_type,
            "authorization": f"Bearer {self.access_token}",
            "appKey": self.__app_key,
            "appSecret": self.__app_secret,
            "tr_id": "FHKST01010100",
        }

        # J = Stock, "fid_input_iscd" = Stock code
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": stock_code}
        
        res = requests.get(url, headers=headers, params=params)
        
        # 'stck_prpr' = current price
        res.json()['output']['stck_prpr']

    # TR = buy: "VJTC0802U" or sell: "VTTC0801U"
    def buy_sell_order(self, stock_code, TR, method = "01", quantity = "10", price = "0"):
        path = "/uapi/domestic-stock/v1/trading/order-cash"
        url = self.get_url(path)

        data = {
            "CANO": "",             # Account number first 8 digits
            "ACNT_PRDT_CD": "",     # Account number last 2 digits
            "PDNO": stock_code,     # Stock Code
            "ORD_DVSN": method,     # Order Method (market price = 01)
            "ORD_QTY": quantity,    # Order quantity
            "ORD_UNPR": price,      # order price (market price = 0)
        }

        # tr_id = stock cash buy order
        headers = {
            "Content-Type": self.content_type,
            "authorization": f"Bearer {self.access_token}",
            "appKey": self.__app_key,
            "appSecret": self.__app_secret,
            "tr_id": TR,
            "custtype": "P",
            "hashkey": self.Hashkey(data)
        }
        
        res = requests.post(url, headers=headers, data=json.dumps(data))
        res.json()

        # amendment_order
        amendment = res.json()["output"]["KRX_FWDG_ORD_ORGNO"]
        ODNO = res.json()["output"]["ODNO"]

    def balance(self):
        path = "/uapi/domestic-stock/v1/trading/inquire-balance"
        url = self.get_url(path)

        headers = {
            "Content-Type": self.content_type,
            "authorization": f"Bearer {self.access_token}",
            "appKey": self.__app_key,
            "appSecret": self.__app_secret,
            "tr_id": "VTTC8434R"
        }

        params = {
            "CANO": "",                     # Account number first 8 digits
            "ACNT_PRDT_CD": "",             # Account number last 2 digits
            "AFHR_FLPR_YN": "N",            # Out-of-Hours singple Price
            "OFL_YN": "",                   # Blank
            "INQR_DVSN": "01",              # Inquiry type
            "UNPR_DVSN": "01",              # Price Type
            "FUND_STTL_ICLD_YN": "N",       # Fund Paymet Portion
            "FNCG_AMT_AUTO_RDPT_YN": "N",   # Automatic repayment of Loan
            "PRCS_DVSN": "00",              # Processing Type ("00" = Include Previous Day)
            "CTX_AREA_FK100": "",           # Continuous inquiry search criteria
            "CTX_AREA_NK100": "",           # Continuous inquiry key
        }

        res = requests.get(url, headers=headers, params=params)
        res.json()['output1'] # held securities
        res.json()['output2'] # account balance

        # ap = pd.DataFrame.from_records(res.json()['output1'])



"""
    def amendment_order(self):
        amendment = res.json()["output"]["KRX_FWDG_ORD_ORGNO"]
        ODNO = res.json()["output"]["ODNO"]
"""

# test
kis = KIS_dataloader()
kospi_df = kis.get_ticker(market = 'kospi')
kospi_df.to_csv('kospi_ticker.csv', encoding='euc-kr')
kosdaq_df = kis.get_ticker(market = 'kosdaq')
kosdaq_df.to_csv('kosdaq_ticker.csv', encoding = 'euc-kr')

