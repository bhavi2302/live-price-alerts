import pandas as pd
import pyotp
import xlwings as xw
import time
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Load Excel file
wb = xw.Book('stocks.xlsx')
sht = wb.sheets["Resistance"]

# Read data from Excel
df = sht.range('A1').options(pd.DataFrame, header=1, index=False, expand='table').value

# Debug: Print DataFrame structure and sample tokens
print("DataFrame Columns:", df.columns.tolist())
print("Sample Tokens:", df['token'].head().tolist())

# Initialize SmartConnect
apikey = '02ji3d16'
username = 'B38590'
pwd = '2302'
token = 'EEHSUJJLWVFSZBRPEXVXKSFCLE'

totp = pyotp.TOTP(token).now()
obj = SmartConnect(api_key=apikey)
data = obj.generateSession(username, pwd, totp)
feedToken = obj.getfeedToken()
jwtToken = data['data']['jwtToken']

# WebSocket setup
correlation_id = "subscription_id"
mode = 3
tokens = df['token'].astype(str).str.strip('.0').tolist()  # Remove ".0" from tokens
token_list = [{"exchangeType": 1, "tokens": tokens}]
sws = SmartWebSocketV2(jwtToken, apikey, username, feedToken)

def on_data(wsapp, msg):
    try:
        if 'token' in msg and 'last_traded_price' in msg:
            token = msg['token'].strip()  # Ensure no whitespace
            ltp = msg['last_traded_price'] / 100  # Adjust decimal
            print(f"Received LTP: Token={token}, LTP={ltp}")

            # Find row index in DataFrame
            row_index = df.index[df['token'].astype(str).str.strip('.0') == token].tolist()
            if row_index:
                excel_row = row_index[0] + 2  # Excel rows start at 1, header is row 1
                sht.range(f'C{excel_row}').value = ltp
                print(f"Updated Excel Row {excel_row} with LTP={ltp}")
            else:
                print(f"Token {token} not found in DataFrame")
    except Exception as e:
        print(f"Error: {e}")

def on_open(wsapp):
    sws.subscribe(correlation_id, mode, token_list)
    print("Subscribed to tokens")

def on_error(wsapp, error):
    print(f"Error: {error}")
    time.sleep(5)
    connect_websocket()

def on_close(wsapp):
    print("Connection closed")
    time.sleep(5)
    connect_websocket()

def connect_websocket():
    try:
        sws.connect()
    except Exception as e:
        print(f"Connection failed: {e}. Retrying...")
        time.sleep(5)
        connect_websocket()

# Assign callbacks
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

# Start WebSocket
connect_websocket()

# Keep the script running
while True:
    time.sleep(1)