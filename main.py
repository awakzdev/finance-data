import os
import argparse
import requests
import base64
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv
import csv
import re
import pandas as pd


def normalize_header(header_row):
    """Trim whitespace from each column in the header row."""
    return [col.strip() for col in header_row]


def validate_and_fix_csv(csv_filename):
    """
    Validates the CSV format and fixes it if corrupted.

    The correct format should have the header:
    Date,Open,High,Low,Close,Adj Close,Volume
    followed by data lines starting with a date in dd/mm/yyyy format.
    """
    expected = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    pattern = re.compile(r'\d{2}/\d{2}/\d{4}')

    try:
        with open(csv_filename, 'r', encoding='utf-8', newline='') as f:
            rows = list(csv.reader(f))

        # find header
        idx = next((i for i,row in enumerate(rows) if normalize_header(row) == expected), None)
        if idx is None:
            print(f"Header not found in {csv_filename}.")
            return False

        valid = [rows[idx]]
        for row in rows[idx+1:]:
            if row and pattern.match(row[0]) and len(row) == len(expected):
                valid.append(row)
            else:
                break

        if len(valid) < 2:
            print(f"No valid data rows in {csv_filename}.")
            return False

        with open(csv_filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(valid)

        return True

    except Exception as e:
        print(f"Error validating {csv_filename}: {e}")
        return False


def main(symbol: str = None):
    # Load GitHub token
    load_dotenv()
    github_token = os.getenv('TOKEN') or os.getenv('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")

    repo = 'awakzdev/finance-data'
    branch = 'main'
    today = datetime.now().strftime('%Y-%m-%d')

    # Step 1: Process each symbol into its own CSV
    symbols = [symbol] if symbol else []
    if not symbols:
        if not os.path.exists('symbols.csv'):
            raise FileNotFoundError("symbols.csv not found")
        with open('symbols.csv', 'r', encoding='utf-8') as f:
            symbols = [line.strip() for line in f if line.strip()]

    for sym in symbols:
        try:
            print(f"Fetching data for symbol: {sym}")
            df = yf.download(sym, start='2006-06-21', end=today)
            if df.empty:
                print(f"No data fetched for symbol: {sym}")
                continue

            # flatten columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.index = df.index.strftime('%d/%m/%Y')
            if 'Adj Close' not in df.columns:
                df['Adj Close'] = df['Close']
            df = df[['Open','High','Low','Close','Adj Close','Volume']]

            filename = f"{sym.replace('^','').lower()}_stock_data.csv"
            df.to_csv(filename, index=True, index_label='Date')
            print(f"CSV {filename} saved.")

            if not validate_and_fix_csv(filename):
                print(f"Skipping upload for {filename}")
                continue

            # upload to GitHub
            url = f'https://api.github.com/repos/{repo}/contents/{filename}'
            headers = {'Authorization': f'token {github_token}'}
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                sha = res.json()['sha']
                print(f"Updating {filename}")
            elif res.status_code == 404:
                sha = None
                print(f"Creating {filename}")
            else:
                print(f"Error accessing {filename}: {res.json()}")
                continue

            content_b64 = base64.b64encode(open(filename,'rb').read()).decode('utf-8')
            payload = {'message': f'Update {sym} stock data', 'content': content_b64, 'branch': branch}
            if sha:
                payload['sha'] = sha

            up = requests.put(url, headers=headers, json=payload)
            if up.status_code in (200,201):
                print(f"Successfully pushed {filename}")
            else:
                print(f"Failed to push {filename}: {up.json()}")

        except Exception as e:
            print(f"Error processing {sym}: {e}")

    # Step 2: Merge predictedQLD.csv + qld_stock_data.csv → qld2_stock_data.csv
    pred_file = 'predictedQLD.csv'
    real_file = 'qld_stock_data.csv'
    merged_file = 'qld2_stock_data.csv'

    if os.path.exists(pred_file) and os.path.exists(real_file):
        df_pred = pd.read_csv(pred_file, index_col='Date', parse_dates=False)
        df_real = pd.read_csv(real_file, index_col='Date', parse_dates=False)
        # ensure same columns order
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_pred = df_pred[cols]
        df_real = df_real[cols]

        df_merged = pd.concat([df_pred, df_real])
        df_merged.to_csv(merged_file, index_label='Date')
        print(f"Merged {pred_file} + {real_file} into {merged_file}")

        if validate_and_fix_csv(merged_file):
            url = f'https://api.github.com/repos/{repo}/contents/{merged_file}'
            headers = {'Authorization': f'token {github_token}'}
            res = requests.get(url, headers=headers)
            if res.status_code == 200:
                sha = res.json()['sha']
                print(f"Updating {merged_file}")
            elif res.status_code == 404:
                sha = None
                print(f"Creating {merged_file}")
            else:
                print(f"Error accessing {merged_file}: {res.json()}")
                sha = None

            content_b64 = base64.b64encode(open(merged_file,'rb').read()).decode('utf-8')
            payload = {'message': 'Update merged QLD data', 'content': content_b64, 'branch': branch}
            if sha:
                payload['sha'] = sha

            up = requests.put(url, headers=headers, json=payload)
            if up.status_code in (200,201):
                print(f"Successfully pushed {merged_file}")
            else:
                print(f"Failed to push {merged_file}: {up.json()}")
    else:
        print(f"Cannot merge QLD data: {pred_file} or {real_file} missing.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch & upload stock CSVs and merge QLD data")
    parser.add_argument("--symbol", "-s", help="Optional single symbol to process", default=None)
    args = parser.parse_args()
    main(symbol=args.symbol)
