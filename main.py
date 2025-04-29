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

        # Extract the relevant rows starting from the header
        valid_rows = lines[header_index:]

        # Verify that there is at least one data row
        if len(valid_rows) < 2:
            print(f"No data found after header in {csv_filename}.")
            return False

        # Validate data rows: stop reading after the first invalid row.
        valid_data = [valid_rows[0]]  # Include the header
        for row in valid_rows[1:]:
            if not row:
                continue  # Skip empty lines
            if date_pattern.match(row[0]):
                if len(row) == len(expected_header):
                    valid_data.append(row)
                else:
                    print(f"Skipping row with incorrect number of columns: {row}")
            else:
                print(f"Encountered non-data row: {row}. Discarding all subsequent lines.")
                break

        if len(valid_data) < 2:
            print(f"No valid data found in {csv_filename}.")
            return False

        # Rewrite the CSV with valid data only
        with open(csv_filename, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(valid_data)

        print(f"CSV {csv_filename} validated and fixed successfully.")
        return True

    except Exception as e:
        print(f"An error occurred while validating {csv_filename}: {e}")
        return False

def main(symbol: str = None):
    # Load GitHub token
    load_dotenv()
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    else:
        print(f"TOKEN loaded, length: {len(github_token)} characters")

    repo = 'awakzdev/finance-data'
    branch = 'main'

    # Step 1: Today's date for Yahoo Finance
    today_date = datetime.now().strftime('%Y-%m-%d')

    # Decide symbols list
    if symbol:
        symbols = [symbol.upper()]
        print(f"Processing only symbol from argument: {symbol.upper()}")
    else:
        symbols_file = 'symbols.csv'
        if not os.path.exists(symbols_file):
            raise FileNotFoundError(f"{symbols_file} does not exist.")
        with open(symbols_file, 'r', encoding='utf-8') as f:
            symbols = [line.strip().upper() for line in f if line.strip()]

    for symbol in symbols:
        try:
            # Fetch historical data
            print(f"Fetching data for symbol: {symbol}")
            data = yf.download(symbol, start='2006-06-21', end=today_date)
            if data.empty:
                print(f"No data fetched for symbol: {symbol}")
                continue

            # Flatten MultiIndex columns if present
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Format the index dates as dd/mm/yyyy
            data.index = data.index.strftime('%d/%m/%Y')

            # Drop 'Price' if exists, ensure 'Adj Close' exists
            if 'Price' in data.columns:
                data.drop(columns='Price', inplace=True)
            if 'Adj Close' not in data.columns:
                data['Adj Close'] = data['Close']

            # Reorder to [Open,High,Low,Close,Adj Close,Volume]
            expected_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            data = data[expected_cols]

            # Save the per-symbol CSV
            sanitized = symbol.replace('^', '').lower()
            csv_filename = f'{sanitized}_stock_data.csv'
            data.to_csv(csv_filename, index=True, index_label='Date')
            print(f"CSV {csv_filename} saved successfully.")

            # Validate/fix it
            if not validate_and_fix_csv(csv_filename):
                print(f"Skipping upload for {csv_filename} due to validation failure.")
                continue

            # -----------------------------------------------------------------
            # If symbol is QLD, also build qld2_stock_data.csv
            if symbol == 'QLD':
                pred_file     = 'predictedQLD.csv'
                combined_file = 'qld2_stock_data.csv'

                # Read predicted + actual
                pred_df   = pd.read_csv(pred_file,
                                        parse_dates=['Date'],
                                        index_col='Date',
                                        dayfirst=True)
                actual_df = pd.read_csv(csv_filename,
                                        parse_dates=['Date'],
                                        index_col='Date',
                                        dayfirst=True)

                # Stack predicted first, then actual
                combined_df = pd.concat([pred_df, actual_df])

                # Back to dd/mm/yyyy strings on index
                combined_df.index = combined_df.index.strftime('%d/%m/%Y')

                # Overwrite combined CSV
                combined_df.to_csv(combined_file, index_label='Date')
                print(f"Combined CSV {combined_file} saved successfully.")

                # Validate the combined file
                if not validate_and_fix_csv(combined_file):
                    print(f"Skipping upload for {combined_file} due to validation failure.")
                # (You can add GitHub upload logic here for qld2_stock_data.csv)
            # -----------------------------------------------------------------

            # Step 5: GitHub SHA lookup for the individual symbol file
            url = f'https://api.github.com/repos/{repo}/contents/{csv_filename}'
            headers = {'Authorization': f'token {github_token}'}
            response = requests.get(url, headers=headers)
            response_json = response.json()

            if response.status_code == 200:
                sha = response_json['sha']
                print(f'File {csv_filename} exists, updating it.')
            elif response.status_code == 404:
                sha = None
                print(f'File {csv_filename} does not exist, creating it.')
            else:
                print(f'Unexpected GitHub error: {response_json}')
                continue

            # Read & base64-encode
            with open(csv_filename, 'rb') as f:
                content = f.read()
            content_b64 = base64.b64encode(content).decode('utf-8')

            # Prepare payload
            payload = {
                'message': f'Update {sanitized} stock data',
                'content': content_b64,
                'branch': branch
            }
            if sha:
                payload['sha'] = sha

            # Push it
            resp = requests.put(url, headers=headers, json=payload)
            if resp.status_code in (200, 201):
                print(f'File {csv_filename} updated successfully in repo.')
            else:
                print(f'Failed to update {csv_filename}:', resp.json())

        except Exception as e:
            print(f'Error processing {symbol}: {e}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch & upload stock CSVs")
    parser.add_argument(
        "--symbol", "-s",
        help="(Optional) Single symbol to process instead of symbols.csv",
        default=None
    )
    args = parser.parse_args()
    main(symbol=args.symbol)
