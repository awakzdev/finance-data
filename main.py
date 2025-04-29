import os
import argparse
import requests
import base64
import yfinance as yf
from datetime import datetime, timedelta
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
    
    If the CSV is corrupted, it removes any lines after the first invalid row.
    
    Args:
        csv_filename (str): The path to the CSV file to validate and fix.
    
    Returns:
        bool: True if the CSV was valid or successfully fixed, False otherwise.
    """
    expected_header = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')  # Matches dd/mm/yyyy

    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            lines = list(reader)

        # Find the index where the correct header starts (using normalization)
        header_index = -1
        for i, row in enumerate(lines):
            if normalize_header(row) == expected_header:
                header_index = i
                break

        if header_index == -1:
            print(f"Header not found in {csv_filename}. The file may be corrupted.")
            return False

        valid_rows = lines[header_index:]

        if len(valid_rows) < 2:
            print(f"No data found after header in {csv_filename}.")
            return False

        # Validate data rows: stop reading after the first invalid row.
        valid_data = [valid_rows[0]]  # header
        for row in valid_rows[1:]:
            if not row:
                continue
            if date_pattern.match(row[0]) and len(row) == len(expected_header):
                valid_data.append(row)
            else:
                break

        if len(valid_data) < 2:
            print(f"No valid data found in {csv_filename}.")
            return False

        with open(csv_filename, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(valid_data)

        print(f"CSV {csv_filename} validated and fixed successfully.")
        return True

    except Exception as e:
        print(f"An error occurred while validating {csv_filename}: {e}")
        return False


def update_qld2():
    """
    Maintain qld2_stock_data.csv:
    - On first run: create predicted history by doubling QQQ prices before QLD inception.
    - On every run: append new actual QLD rows since last date in qld2_stock_data.csv.
    """
    repo_fname    = 'qld2_stock_data.csv'
    inception_str = '2006-06-21'
    today_str     = datetime.now().strftime('%Y-%m-%d')

    # If the file already exists, read it
    if os.path.exists(repo_fname):
        # ensure Date index is parsed as datetime (dayfirst for dd/mm/yyyy)
        existing = pd.read_csv(
            repo_fname,
            index_col='Date',
            parse_dates=True,
            dayfirst=True
        )
        
        last_date = existing.index.max()
        if last_date.strftime('%Y-%m-%d') >= today_str:
            print("qld2 is already up to date through", last_date.date())
            return
        start_new = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # First run: generate predicted history from QQQ
        qqq = yf.download('QQQ', start='2000-01-01', end=inception_str)
        if isinstance(qqq.columns, pd.MultiIndex):
            qqq.columns = qqq.columns.get_level_values(0)
        for c in ['Open','High','Low','Close','Adj Close']:
            qqq[c] = qqq[c] * 2
        qqq.index = qqq.index.strftime('%d/%m/%Y')
        qqq.to_csv(repo_fname, index_label='Date')
        start_new = inception_str

    # Fetch new actual QLD data
    qld_new = yf.download('QLD', start=start_new, end=today_str)
    if qld_new.empty:
        print("No new QLD rows to add.")
        return
    if isinstance(qld_new.columns, pd.MultiIndex):
        qld_new.columns = qld_new.columns.get_level_values(0)
    qld_new.index = qld_new.index.strftime('%d/%m/%Y')
    if 'Adj Close' not in qld_new.columns:
        qld_new['Adj Close'] = qld_new['Close']
    qld_new = qld_new[['Open','High','Low','Close','Adj Close','Volume']]

    df_existing = pd.read_csv(repo_fname, index_col='Date', parse_dates=True)
    combined   = pd.concat([df_existing, qld_new])
    combined.to_csv(repo_fname, index_label='Date')
    print(f"Appended {len(qld_new)} new QLD rows to {repo_fname}.")


def main(symbol: str = None):
    load_dotenv()
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    else:
        print(f"TOKEN loaded, length: {len(github_token)} characters")

    repo   = 'awakzdev/finance-data'
    branch = 'main'
    today_date = datetime.now().strftime('%Y-%m-%d')

    # Determine symbols list
    if symbol:
        symbols = [symbol]
        print(f"Processing only symbol from argument: {symbol}")
    else:
        symbols_file = 'symbols.csv'
        if not os.path.exists(symbols_file):
            raise FileNotFoundError(f"{symbols_file} does not exist. Please create it before running the script.")
        with open(symbols_file, 'r', encoding='utf-8') as f:
            symbols = [line.strip() for line in f if line.strip()]

    for symbol in symbols:
        # Special case for qld2
        if symbol.lower() == 'qld2':
            update_qld2()
            continue

        try:
            print(f"Fetching data for symbol: {symbol}")
            data = yf.download(symbol, start='2006-06-21', end=today_date)
            if data.empty:
                print(f"No data fetched for symbol: {symbol}")
                continue

            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            data.index = data.index.strftime('%d/%m/%Y')
            if 'Price' in data.columns:
                data.drop(columns='Price', inplace=True)
            if 'Adj Close' not in data.columns:
                data['Adj Close'] = data['Close']
            expected_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            data = data[expected_cols]

            sanitized_symbol = symbol.replace('^', '')
            csv_filename = f'{sanitized_symbol.lower()}_stock_data.csv'
            data.to_csv(csv_filename, index=True, index_label='Date')
            print(f"CSV {csv_filename} saved successfully.")

            if not validate_and_fix_csv(csv_filename):
                print(f"Skipping upload for {csv_filename} due to validation failure.")
                continue

            url = f'https://api.github.com/repos/{repo}/contents/{csv_filename}'
            headers = {'Authorization': f'token {github_token}'}
            response = requests.get(url, headers=headers)
            response_json = response.json()

            if response.status_code == 200:
                sha = response_json['sha']
                print(f'File {csv_filename} exists, updating it.')
            elif response.status_code == 404:
                sha = None
                print(f'File {csv_filename} does not exist, creating a new one.')
            else:
                print(f'Unexpected error while accessing {csv_filename}: {response_json}')
                continue

            with open(csv_filename, 'rb') as f:
                content = f.read()
            content_base64 = base64.b64encode(content).decode('utf-8')

            commit_message = f'Update {sanitized_symbol} stock data'
            payload = {
                'message': commit_message,
                'content': content_base64,
                'branch': branch
            }
            if sha:
                payload['sha'] = sha

            response = requests.put(url, headers=headers, json=payload)
            if response.status_code in [200, 201]:
                print(f'File {csv_filename} updated successfully in the repository.')
            else:
                print(f'Failed to update the file {csv_filename} in the repository.')
                print('Response:', response.json())

        except Exception as e:
            print(f'An error occurred while processing symbol {symbol}: {e}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch & upload stock CSVs")
    parser.add_argument(
        "--symbol", "-s",
        help="(Optional) Single symbol to process instead of symbols.csv",
        default=None
    )
    args = parser.parse_args()
    main(symbol=args.symbol)
