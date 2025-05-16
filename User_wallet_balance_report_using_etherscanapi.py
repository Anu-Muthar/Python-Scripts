import json
import csv
import psycopg2
from postmarker.core import PostmarkClient
from datetime import datetime, timedelta
from time import sleep
import pandas as pd
from requests import get

# Constants
API_KEY = "your_dummy_api_key"
BASE_URL = "https://api.etherscan.io/api"
ETHER_VALUE = 10 ** 18
REPORT_DATE = '2022-06-16'

# Database connection (Use your actual connection details here)
con = psycopg2.connect(
    database="your_database",
    user="your_username",
    password="your_password",
    host="your_host_url",
    port="5432"
)
print("Database connected successfully.")
cur = con.cursor()

# Fetch unique wallet addresses from the table
query_wallets = """
SELECT string_agg(wallet_address, ',') AS wallet_address 
FROM nft_applications 
WHERE DATE(created_at::date) <= %s 
  AND wallet_address <> '' 
  AND wallet_address LIKE '0x%%'"
cur.execute(query_wallets, (REPORT_DATE,))
wallet_records = cur.fetchone()
address_list = list(set(wallet_records[0].split(","))) if wallet_records[0] else []

# Helper: Construct the API URL
def make_api_url(module, action, address, **kwargs):
    url = f"{BASE_URL}?module={module}&action={action}&address={address}&apikey={API_KEY}"
    for key, value in kwargs.items():
        url += f"&{key}={value}"
    return url

# Fetch ETH balance for wallet addresses
ether_balance_list = []
def get_account_balance(addresses):
    url = make_api_url("account", "balancemulti", addresses, startblock=0, endblock=99999999, page=1, offset=10000, sort="asc")
    response = get(url)
    if response.status_code == 200:
        result = response.json().get("result", [])
        for tx in result:
            balance = int(tx['balance']) / ETHER_VALUE if tx['balance'].strip() else 0
            ether_balance_list.append({
                "displayName": None,
                "account": tx["account"],
                "balance": balance
            })
    else:
        print(f"Invalid response for addresses: {addresses}")

# Process addresses in batches of 20
batch = []
for count, address in enumerate(address_list):
    batch.append(address)
    if len(batch) == 20 or count == len(address_list) - 1:
        get_account_balance(",".join(batch))
        batch = []
        sleep(2)  # Respect rate limits

# Fetch user display names for wallet addresses
query_display_names = """
WITH user_info AS (
    SELECT first_name, last_name, alias, display_field, wallet_address,
           CASE 
               WHEN display_field = 'first_name' THEN COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')
               WHEN display_field = 'last_name' THEN COALESCE(last_name, '') || ' ' || COALESCE(first_name, '')
               ELSE COALESCE(alias, '') 
           END AS display_name
    FROM nft_applications
    WHERE wallet_address <> '' 
      AND wallet_address LIKE '0x%%' 
      AND DATE(created_at::date) <= %s
)
SELECT DISTINCT display_name, wallet_address 
FROM user_info
"""
cur.execute(query_display_names, (REPORT_DATE,))
user_records = dict(cur.fetchall())

# Update display names in balance list
for record in ether_balance_list:
    wallet = record["account"]
    record["displayName"] = user_records.get(wallet, "Unknown")

# Write to CSV
filename = f"WalletData-{REPORT_DATE}.csv"
with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['displayName', 'account', 'balance']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(ether_balance_list)

# Email Report
postmark = PostmarkClient(server_token='your_postmark_server_token')
email = postmark.emails.Email(
    From='reports@yourdomain.com',
    To='recipient@yourdomain.com',
    Subject=f'Users Wallet Report Till {REPORT_DATE}',
    HtmlBody="Please find attached the wallet report."
)
email.attach(filename)
email.send()

print(f"{datetime.now()} - Wallet report sent successfully.")
