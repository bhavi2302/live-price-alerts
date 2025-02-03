import time
from live_data import get_live_data

def continuous_fetch():
    while True:
        live_data = get_live_data()
        # Process and store live data as needed
        time.sleep(60)  # Wait for 60 seconds before fetching again

if __name__ == "__main__":
    continuous_fetch()
