import os
import time
import csv
from dotenv import load_dotenv
import requests
from datetime import datetime
import hashlib
import hmac

# Load environment variables from .env file
load_dotenv()

# Get environment variables
API_KEY = os.getenv('BYBIT_API_KEY')
API_SECRET = os.getenv('BYBIT_API_SECRET')
REQUEST_INTERVAL = int(os.getenv('REQUEST_INTERVAL', 60))  # Default to 60 seconds
CSV_FILE = os.getenv('CSV_FILE', 'balances.csv')

class BybitBalanceGrabber:
    def __init__(self, api_key, api_secret, csv_file):
        self.api_key = api_key
        self.api_secret = api_secret
        self.csv_file = csv_file
        self.base_url = "https://api.bybit.com"
        self.init_csv()

    def init_csv(self):
        # Initialize the CSV file with headers if it doesn't exist
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["timestamp", "asset", "balance"])

    def get_balances(self):
        # Request account balance from Bybit API
        endpoint = f"{self.base_url}/v5/account/wallet-balance"
        params = {
            'api_key': self.api_key,
            'timestamp': int(time.time() * 1000),
            'accountType': 'UNIFIED'
        }

        params['sign'] = self.generate_signature(params)
        print(params['sign'])

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            print(data)
            return data['totalWalletBalance']
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None

    def generate_signature(self, params):
        # Generate a signature using HMAC SHA256
        query_string = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()


    def save_balances(self, balance):
        # Save balances with timestamps to the CSV file
        with open(self.csv_file, mode='a', newline='') as file:
            writer = csv.writer(file)
            timestamp = datetime.now(datetime.timezone.utc).isoformat()
            writer.writerow([timestamp, balance])

    def run(self):
        while True:
            balance = self.get_balances()
            if balance:
                self.save_balances(balance)
            time.sleep(REQUEST_INTERVAL)

if __name__ == "__main__":
    grabber = BybitBalanceGrabber(API_KEY, API_SECRET, CSV_FILE)
    grabber.run()
