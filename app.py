import sys
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import pandas as pd
import json
import threading
import time
from logzero import logger

# Manually add the correct path to SmartApi
smartapi_path = 'C:\\Users\\bhave\\Documents\\Astrotrade108\\venv\\Lib\\site-packages\\SmartApi'
if smartapi_path not in sys.path:
    sys.path.append(smartapi_path)

# Replace with your actual details
api_key = 'hMpRK7QB'
username = 'B38590'
pwd = '2302'
smartApi = SmartConnect(api_key)

try:
    token = "EEHSUJJLWVFSZBRPEXVXKSFCLE"
    totp = pyotp.TOTP(token).now()
except Exception as e:
    logger.error("Invalid Token: The provided token is not valid.")
    raise e

data = smartApi.generateSession(username, pwd, totp)
print("Session Data:", data)  # Add this line for debugging

if data['status'] == False:
    logger.error(data)
    exit(1)
else:
    authToken = data['data']['jwtToken']
    feedToken = smartApi.getfeedToken()

# Example MCX tokens, replace with actual tokens from your MCX market data
mcx_token_list = ["57920", "57919"]  # Example tokens for MCX market

correlation_id = "abc123"
mode = 1

token_list = [{"exchangeType": 2, "tokens": mcx_token_list}]

# WebSocket V2 Setup
sws = SmartWebSocketV2(authToken, api_key, username, feedToken)

def on_data(wsapp, message):
    logger.info("Ticks: {}".format(message))
    print("Raw message received: {}".format(message))  # Add this line for debugging

    try:
        data = json.loads(message)
        print("Parsed data:", data)  # Add this line for debugging
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON message: {}".format(e))
        print("Failed to decode JSON message:", e)  # Add this line for debugging
        return

    if not data:
        logger.info("No data received.")
        print("No data received.")  # Add this line for debugging
        return

    symbol = data.get("symbol", "N/A")
    ltp = data.get("ltp", "N/A")

    stock_data = [{
        "symbol": symbol,
        "ltp": ltp
    }]
    logger.info(f"Emitted stock data: {stock_data}")
    print("Emitted stock data:", stock_data)  # Add this line for debugging

def on_open(wsapp):
    logger.info("WebSocket connection opened.")
    print("WebSocket connection opened.")
    logger.info(f"Subscribing with correlation_id: {correlation_id}, mode: {mode}, token_list: {token_list}")
    print(f"Subscribing with correlation_id: {correlation_id}, mode: {mode}, token_list: {token_list}")
    sws.subscribe(correlation_id, mode, token_list)

def on_error(wsapp, error):
    logger.error(error)
    print("WebSocket Error:", error)  # Add this line for debugging

def on_close(wsapp):
    logger.info("WebSocket connection closed. Reconnecting in 5 seconds...")
    print("WebSocket connection closed. Reconnecting in 5 seconds...")  # Add this line for debugging
    time.sleep(5)  # Wait for 5 seconds before retrying
    logger.info("Reconnecting...")
    sws.connect()

# Assign the callbacks.
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close

def start_websocket():
    logger.info("Starting WebSocket connection...")
    print("Starting WebSocket connection...")  # Add this line for debugging
    sws.connect()

# Start the WebSocket connection
threading.Thread(target=start_websocket).start()



