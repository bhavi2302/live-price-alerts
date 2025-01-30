import requests
import pandas as pd
import pyotp
import time
from SmartApi.smartConnect import SmartConnect
from datetime import datetime

# Define API credentials
api_key = '02ji3d16'
username = 'B38590'
pwd = '2302'
token = 'EEHSUJJLWVFSZBRPEXVXKSFCLE'

# Create object of call
obj = SmartConnect(api_key=api_key)

# Login API call
print("Generating session...")
totp = pyotp.TOTP(token).now()

# Set timeout and retry configuration
requests.adapters.DEFAULT_RETRIES = 5
timeout_duration = 15

def generate_session(username, pwd, totp):
    while True:
        try:
            data = obj.generateSession(username, pwd, totp)
            return data
        except requests.exceptions.RequestException as e:
            print(f"Error generating session: {e}. Retrying...")
            time.sleep(5)  # Wait before retrying

data = generate_session(username, pwd, totp)
print("Session data:", data)

auth_token = data['data']['jwtToken']
print("Auth token:", auth_token)

# Read the list of tokens from an Excel file
excel_path_input = r"C:\Users\bhave\Documents\Astrotrade108\stocks.xlsx"
tokens_df = pd.read_excel(excel_path_input)
tokens = tokens_df['token'].astype(str).tolist()  # Read from 'token' column

# Define the date range
start_date = "2025-01-03"
start_time = "10:47"
end_date = "2025-01-06"
end_time = "15:30"

# Initialize an empty DataFrame to store the results
results_df = pd.DataFrame(columns=['Script', 'High', 'Low'])

# Function to fetch historical data from Angel One API
def fetch_historical_data(token, start_date, start_time, end_date, end_time):
    print(f"Fetching data for token: {token}")
    historic_param = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "FIVE_MINUTE",
        "fromdate": f"{start_date} {start_time}", 
        "todate": f"{end_date} {end_time}"
    }

    while True:
        try:
            data_historical = obj.getCandleData(historic_param)
            print(f"Response JSON for token {token}: {data_historical}")
            if 'data' in data_historical and data_historical['data']:
                df = pd.DataFrame(data_historical['data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%dT%H:%M:%S%z')
                return df[['timestamp', 'high', 'low']]
            else:
                return pd.DataFrame()  # Return an empty DataFrame on failure
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for token {token}: {e}. Retrying...")
            time.sleep(5)  # Wait before retrying

# Define batch size and delay
batch_size = 3  # Number of tokens per batch
delay_seconds = 5  # Delay between batches (to stay within rate limits)

# Process tokens in batches
for i in range(0, len(tokens), batch_size):
    batch_tokens = tokens[i:i + batch_size]
    for token in batch_tokens:
        df = fetch_historical_data(token, start_date, start_time, end_date, end_time)
        if not df.empty:
            highest_price = df['high'].max()
            lowest_price = df['low'].min()
            print(f"Token: {token}, High: {highest_price}, Low: {lowest_price}")

            # Append the results to the DataFrame using pd.concat
            results_df = pd.concat([
                results_df,
                pd.DataFrame({'Script': [token], 'High': [highest_price], 'Low': [lowest_price]})
            ], ignore_index=True)
        else:
            print(f"Token: {token}, No data available")

    # Introduce a delay between batches to handle rate limits
    print(f"Batch {i // batch_size + 1} processed. Waiting for {delay_seconds} seconds before next batch.")
    time.sleep(delay_seconds)

# Save the results to Excel
excel_path_output = r"C:\Users\bhave\Documents\Astrotrade108\Stock_historical_data.xlsx"
results_df.to_excel(excel_path_output, index=False)
print(f"Data saved to {excel_path_output}")