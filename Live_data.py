import os
import csv
import pandas as pd
import pyotp
import time
import json  # Add this import statement
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2

# Initialize SmartConnect using environment variables
apikey = os.getenv('API_KEY')
username = os.getenv('USERNAME')
pwd = os.getenv('PWD')
token = os.getenv('TOKEN')

totp = pyotp.TOTP(token).now()
obj = SmartConnect(api_key=apikey)
data = obj.generateSession(username, pwd, totp)
feedToken = obj.getfeedToken()
jwtToken = data['data']['jwtToken']

# Read tokens and symbols from CSV
tokens = []
symbol_map = {}

with open('stocks.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        tokens.append(row['token'])
        symbol_map[row['token']] = row['symbol']

# WebSocket setup
mode = 3
token_list = [{"exchangeType": 1, "tokens": tokens}]
sws = SmartWebSocketV2(jwtToken, apikey, username, feedToken)

live_data = {}

def on_data(wsapp, message):
    try:
        if isinstance(message, str):
            message = json.loads(message)
        if 'token' in message and 'last_traded_price' in message:
            token = str(message['token']).strip()
            ltp = message['last_traded_price'] / 100
            print(f"Received LTP: Token={token}, LTP={ltp}")
            live_data[symbol_map[token]] = ltp

            # Optionally: Save live_data to CSV or database if required.
            
            # Debug: Print the live data
            print(f"Live Data: {live_data}")
    except Exception as e:
        print(f"Error in on_data: {e}")

def on_open(wsapp):
    sws.subscribe(mode, token_list)
    print("Subscribed to tokens")

def on_error(wsapp, error):
    print(f"WebSocket Error: {error}")
    time.sleep(5)
    connect_websocket()

def on_close(wsapp):
    print("WebSocket connection closed")
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
