import os
import json
import time
import pandas as pd
import pyotp
import gspread
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from oauth2client.service_account import ServiceAccountCredentials

# Function to get Google Sheet
def get_google_sheet():
    json_file_path = 'dotted-vortex-449412-b6-6721cd2b915a.json'  # Replace with your JSON file
    with open(json_file_path) as f:
        creds_dict = json.load(f)

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)
    sh = gc.open('stocks')
    worksheet = sh.worksheet('Resistance')
    return worksheet

# Initialize SmartConnect
apikey = 'uqvuflLJ'
username = 'B38590'
pwd = '2302'
token = 'EEHSUJJLWVFSZBRPEXVXKSFCLE'

totp = pyotp.TOTP(token).now()
obj = SmartConnect(api_key=apikey)
data = obj.generateSession(username, pwd, totp)
feedToken = obj.getfeedToken()
jwtToken = data['data']['jwtToken']

# Google Sheets setup
worksheet = get_google_sheet()
df = pd.DataFrame(worksheet.get_all_records())

# Debug: Print DataFrame structure and sample tokens
print("DataFrame Columns:", df.columns.tolist())
print("Sample Tokens:", df['token'].head().tolist())

# WebSocket setup
correlation_id = "subscription_id"
mode = 3
tokens = df['token'].astype(str).tolist()
token_list = [{"exchangeType": 1, "tokens": tokens}]
sws = SmartWebSocketV2(jwtToken, apikey, username, feedToken)

def on_data(wsapp, msg):
    try:
        if isinstance(msg, str):
            msg = json.loads(msg)
        if 'token' in msg and 'last_traded_price' in msg:
            token = str(msg['token']).strip()  # Ensure no whitespace
            ltp = msg['last_traded_price'] / 100  # Adjust decimal
            print(f"Received LTP: Token={token}, LTP={ltp}")

            # Find row index in DataFrame
            row_index = df.index[df['token'].astype(str) == token].tolist()
            if row_index:
                google_sheet_row = row_index[0] + 2  # Google Sheets rows start at 1, header is row 1
                worksheet.update_cell(google_sheet_row, 3, ltp)
                print(f"Updated Google Sheet Row {google_sheet_row} with LTP={ltp}")
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
