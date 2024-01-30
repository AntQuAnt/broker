import os
import time 
import json
import requests 
import zipfile
import xmltodict
import pandas as pd
from io import BytesIO
import pprint
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'))

class Dart():
    '''
    get some financial data using Dart open api

    Parameters
    ----------
    api_key: str
        your api key 
    
    Attributes
    ----------
        api_key: str
            your api key
        base_url: str
            url of dart open api
        code: DataFrame
            corporate code dataframe
    '''
    def __init__(self, api_key): 
        self.api_key = api_key
        self.base_url = 'https://opendart.fss.or.kr/api/'
        self.code = self._get_corp_code()

    # get corporate code
    def _get_corp_code(self):
        url = os.path.join(self.base_url, 'corpCode.xml')
        params = {
            'crtfc_key': self.api_key    
        }
        res = requests.get(url, params = params)

        if not res.ok:
            print('Failed to get corporate code.')
            print(f'HTTP status code: {res.status_code}')

        else:
            code = zipfile.ZipFile(BytesIO(res.content))
            code  = code.read('CORPCODE.xml').decode('utf-8')
            code_dict = xmltodict.parse(code)
            code_df = pd.DataFrame(code_dict['result']['list'])

            # delete unlisted corporates 
            code_df = code_df[code_df['stock_code'].notnull()]
            code_df.reset_index(drop = True, inplace = True)
            
        return code_df
    

    def get_balance_sheet(self, code, year, rprt_code = '11011'):
        '''
        get balance sheet data from dart

        Parameters
        ----------
            code: str
                corporate code of desired company (note. it's not stock code)
                use "," if you want multiple corporates (e.g. "00000000,00000001,00000002")
            year: str 
                business year of balance sheet
                use "~" if you want multiple years (e.g. '2015~2020')
            rprt_code: {'11013', '11012', '11014', '11011'}, default: '11011'
                report type code (11013: 1Q, 11012: 2Q, 11014: 3Q, 11011:4Q)

        Returns
        -------
            balance_sheet: DataFrame
                DataFrame which contains financial statements data 
        '''
        # corporate code list
        code_list = str(code).strip().split(',')
        
        # year list 
        if '~' in year:
            start_year, end_year = str(year).strip().split('~')
            year_list = range(int(start_year), int(end_year) + 1)
            year_list = list(map(str, year_list))

        else: 
            year_list = [year]

        # url
        url = os.path.join(self.base_url, 'fnlttSinglAcnt.json')        
        
        # request parameters
        params = {
            'crtfc_key': self.api_key,  # api key
            'corp_code': None,          # corporate code
            'bsns_year': None,          # business year
            'reprt_code': rprt_code,    # report code
            'fs_div': 'CFS'             # OFS: 재무제표, CFS: 연결재무제표
        }

        bs_df = pd.DataFrame(None)
        for tmp_code in code_list: 
            
            is_corp = self.code['corp_code'] == tmp_code
            corp_name = self.code[is_corp].corp_name.values[0]
            corp_stock_code = self.code[is_corp].stock_code.values[0]

            for tmp_year in year_list:

                params['corp_code'] = tmp_code
                params['bsns_year'] = tmp_year

                res = requests.get(url, params = params)

                # request error
                if not res.ok: 
                    print('Failed to get corporate code.')
                    print(f'HTTP status code: {res.status_code}')
                
                else: 
                    data_dict = json.loads(res.content)

                    # if there are no data
                    if data_dict['status'] != '000':
                        print(f'code: {data_dict['status']} / message: {data_dict['message']}')

                    else:     
                        tmp_df = pd.DataFrame(data_dict['list'])
                        
                        # preprocess data
                        data = {
                            'code': corp_stock_code,
                            'name': corp_name
                        }
                        for k, v in zip(tmp_df.account_nm.values, tmp_df.thstrm_amount.values):
                            data.update({k: v})
                        
                        bs = pd.DataFrame(data, index = [tmp_year])
                        
                        # concat to bs_df
                        bs_df = pd.concat([bs_df, bs])

                        time.sleep(0.2)
        
        return bs_df




### Test Code ###
API_KEY = os.environ['DART_API_KEY']
test = Dart(API_KEY)

# corporate code
print('=' * 30)
print('corporate code')
print('=' * 30)
print(test.code)
print()

# get balance sheet 
print('=' * 30)
print('balance sheet')
print('=' * 30)
code = '00126380,00401731,00164742,00164779'     # 삼성전자, LG전자, 현대차, SK하이닉스
year = '2015~2023'                               # 2015년부터 2023년까지
df = test.get_balance_sheet(code, year, rprt_code='11011')
print(df)




