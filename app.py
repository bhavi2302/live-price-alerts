from flask import Flask, render_template
import os
import threading
import time
import requests
from logzero import logger

# Import your scripts
from live_data import get_live_data  # Replace with actual function
from historical_data import get_historical_data  # Replace with actual function

app = Flask(__name__)

# Function to send message to Telegram
def send_telegram_message(message):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': message
    }
    requests.post(url, json=payload)

@app.route('/')
def index():
    return "Web App Running"

@app.route('/alert')
def alert():
    live_data = get_live_data()  # Fetch live data
    historical_levels = get_historical_data()  # Fetch historical levels

    # Check conditions and send alert
    for level in historical_levels:
        if live_data['price'] >= level['price']:
            message = f"Alert: {live_data['symbol']} reached level {level['price']}"
            send_telegram_message(message)
            return message

    return "No alert triggered."

if __name__ == '__main__':
    app.run(debug=True)
