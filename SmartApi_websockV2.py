import pandas as pd
from natsort import natsorted
from datetime import datetime
import threading
import pyotp,time
import pandasgui
from pandasgui import show
import xlwings as xw
import time
import datetime
import numpy as np



from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

wb = xw.Book('Live_Feed_Data.xlsx')
sht1 = wb.sheets("Live_Data")
sht2 = wb.sheets("Symbol_Input")
json = pd.read_json('https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json')
    
df_initial = pd.read_excel('Live_Feed_Data.xlsx', 'Symbol_Input',usecols=['Symbol'])

df_initial.rename(columns={'Symbol':'symbol'},inplace=True)
df_3=pd.merge(df_initial,json[['symbol','token']],on='symbol',how='left')
df_3.rename(columns={'symbol':'Symbol'},inplace=True)
df_3=df_3.replace(np.nan, '', regex=True)

print(df_3)

del df_3['Symbol']
# print(df_3)
sht2["B1"].options(pd.DataFrame, header=1, index=False, expand='table').value = df_3
           



obj = SmartConnect(api_key="kFa5C8jL")
api_key = 'kFa5C8j'
username = 'B38590'
pwd = '2302'
token = 'EEHSUJJLWVFSZBRPEXVXKSFCLE'
totp = pyotp.TOTP(token)
totp = totp.now()
print(totp)
obj = SmartConnect(api_key="kFa5C8jL")
data = obj.generateSession(username, pwd,totp)
print(data)
refreshToken = data['data']['refreshToken']
print(refreshToken)
feedToken = obj.getfeedToken()

print(feedToken)

jwtToken = data['data']['jwtToken']

print(jwtToken)
correlation_id = "marketpaathshala"
action = 1
mode = 3






token_list = [{"exchangeType": 1, "tokens": ["26000","11536"]},{"exchangeType": 2, "tokens": ["35079","52581","67300","48224","176644"]},{"exchangeType": 5, "tokens": ["257264","258935"]}]

# exchange type ( 1 = nse_cm,  2= nse_fo, 5= mcx_fo, 7= ncx_fo, 13= cde_fo)





sws = SmartWebSocketV2(jwtToken, api_key, username, feedToken)

LIVE_FEED_JSON = {}



def on_data(wsapp, msg):
    try:
        # print("Ticks: {}".format(msg))
        LIVE_FEED_JSON[msg['token']] = {'1' :datetime.datetime.fromtimestamp(msg['exchange_timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S') ,'3' :msg['last_traded_price']/100 ,'4' :msg['average_traded_price']/100 ,'5' :msg['high_price_of_the_day']/100 ,'6':msg['low_price_of_the_day']/100 ,'7' :msg['closed_price']/100 ,'8':msg['volume_trade_for_the_day'],'9':msg['open_interest']}
        # print(LIVE_FEED_JSON)
        df1=pd.DataFrame(LIVE_FEED_JSON)
        df2=df1.T
        df3 = df2.sort_index(ascending=True)
        print(df3)

        df=pd.DataFrame(LIVE_FEED_JSON)
        df1=df.reindex(index=natsorted(df.index))
        df3=df1.reindex(columns=natsorted(df1.columns)).T
            
        print(df3)
        sht1.range('D2').value = df3
        # pandasgui=df3
        # show(pandasgui)

    except Exception as e:
        print(e)
        


     
         



def on_open(wsapp):
    print("on open")
    sws.subscribe(correlation_id, mode, token_list)


def on_error(wsapp, error):
    print(error)


def on_close(wsapp):
    print("Close")





# Assign the callbacks.
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

sws.connect()
