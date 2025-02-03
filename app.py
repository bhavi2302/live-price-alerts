import os
from flask import Flask, render_template, jsonify
import threading
import time
import json
import pyotp
from SmartApi import SmartConnect
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
import telegram

app = Flask(__name__)

# Initialize variables
live_data = {}
historical_data = {}  # Temporary storage for historical high and low prices
alerts = []

# Initialize Telegram bot
bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
chat_id = os.getenv('TELEGRAM_CHAT_ID')
bot = telegram.Bot(token=bot_token)

# Function to send Telegram alert
def send_telegram_alert(message):
    bot.send_message(chat_id=chat_id, text=message)

# Initialize SmartConnect
apikey = os.getenv('API_KEY')
username = os.getenv('USERNAME')
pwd = os.getenv('PWD')
token = os.getenv('TOKEN')

totp = pyotp.TOTP(token).now()
obj = SmartConnect(api_key=apikey)
data = obj.generateSession(username, pwd, totp)
feedToken = obj.getfeedToken()
jwtToken = data['data']['jwtToken']

# WebSocket setup
mode = 3
tokens = ['13061', '445', '11868', '23729', '1030']  # Example tokens
token_list = [{"exchangeType": 1, "tokens": tokens}]
sws = SmartWebSocketV2(jwtToken, apikey, username, feedToken)

some_threshold = 100  # Define your threshold value

def on_data(wsapp, message):
    global live_data
    try:
        if isinstance(message, str):
            message = json.loads(message)
        if 'token' in message and 'last_traded_price' in message:
            token = str(message['token']).strip()
            ltp = message['last_traded_price'] / 100
            print(f"Received LTP: Token={token}, LTP={ltp}")
            live_data[token] = ltp
            
            # Update historical data
            if token not in historical_data:
                historical_data[token] = {'high': ltp, 'low': ltp}
            else:
                if ltp > historical_data[token]['high']:
                    historical_data[token]['high'] = ltp
                if ltp < historical_data[token]['low']:
                    historical_data[token]['low'] = ltp

            # Check criteria and trigger alerts
            if ltp > some_threshold:
                alert_message = f"Alert: Token {token} LTP {ltp} exceeds threshold"
                alerts.append(alert_message)
                send_telegram_alert(alert_message)
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

# Start WebSocket in a separate thread
ws_thread = threading.Thread(target=connect_websocket)
ws_thread.daemon = True
ws_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/live_data')
def get_live_data():
    return jsonify(live_data)

@app.route('/historical_data')
def get_historical_data():
    return jsonify(historical_data)

@app.route('/alerts')
def get_alerts():
    return jsonify(alerts)

if __name__ == "__main__":
    app.run(debug=True)





