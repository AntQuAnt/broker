import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'))

# key
app_key = os.environ['KIS_MOCK_APP_KEY']
app_secret = os.environ['KIS_MOCK_APP_SECRET']
content_type = "application/json"

# base url (mock = 29443, real = 9443)
url_base = "https://openapivts.Koreainvestment.com:29443"


class key():
    def __init__(self):
        self.access_token = self.KIS_token()

    def KIS_token(self):
        # information
        headers = {"content-type": content_type}
        path = "oauth2/tokenP"
        body = {
            "grant_type": "client_credentials",
            "appkey" : app_key,
            "appsecret": app_secret
        }

        url = self.get_url(path)
        res = requests.post(url, headers=headers, data=json.dumps(body))
        return res.json()['access_token']

    def get_url(self, path):
        return f"{url_base}/{path}"


    def Hashkey(self, datas):
        path = "uapi/hashkey"
        url = self.get_url(path)

        headers = {
            "content-Type": content_type,
            "appKey": app_key,
            "appSecret": app_secret,
        }

        res = requests.post(url, headers=headers, data=json.dumps(datas))
        hashkey = res.json()["HASH"]

        return hashkey


class Util(key):
    def current_price(self, stock_code):
        path = "uapi/domestic-stock/v1/quotations/inquire-price"
        url = self.get_url(path)

        # tr_id = Current stock price
        headers = {
            "Content_type": content_type,
            "authorization": f"Bearer {self.access_token}",
            "appKey": app_key,
            "appSecret": app_secret,
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
            "Content-Type": content_type,
            "authorization": f"Bearer {self.access_token}",
            "appKey": app_key,
            "appSecret": app_secret,
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
            "Content-Type": content_type,
            "authorization": f"Bearer {self.access_token}",
            "appKey": app_key,
            "appSecret": app_secret,
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




