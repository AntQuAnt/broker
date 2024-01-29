import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.getcwd(), '.env'))

# key
app_key = os.environ['KIS_MOCK_APP_KEY']
app_secret = os.environ['KIS_MOCK_APP_SECRET']

# base url (mock = 29443, real = 9443)
url_base = "https://openapivts.Koreainvestment.com:29443"

def KIS_token():

    # information
    headers = {"content-type": "application/json"}
    path = "oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey" : app_key,
        "appsecret": app_secret
    }

    url = f"{url_base}/{path}"
    res = requests.post(url, headers=headers, data=json.dumps(body))
    access_token = res.json()['access_token']

    return access_token

def hashkey(datas):

    path = "uapi/hashkey"
    url = f"{url_base}/{path}"
    headers = {
        'content-Type': 'application/json',
        'appKey': app_key,
        'appSecret': app_secret,
    }

    res = requests.post(url, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]

    return hashkey




