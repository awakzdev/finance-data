import os
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

        # Debug: Print the first few lines of the CSV for inspection
        print(f"Debug: First 3 lines from {csv_filename}:")
        for line in lines[:3]:
            print(line)

        # Find the index where the correct header starts (using normalization)
        header_index = -1
        for i, row in enumerate(lines):
            if normalize_header(row) == expected_header:
                header_index = i
                break

        if header_index == -1:
            print(f"Header not found in {csv_filename}. The file may be corrupted.")
            return False

        # Debug: Print the header row found
        print(f"Header found in {csv_filename} at line {header_index}: {lines[header_index]}")

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

def main():
    # Retrieve the GitHub token from environment variables
    load_dotenv()
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    else:
        print(f"TOKEN loaded, length: {len(github_token)} characters")
    
    repo = 'awakzdev/finance-data'
    branch = 'main'
    
    # Step 1: Fetch today's date in the format YYYY-MM-DD
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    # Symbols to process
    symbols = ['QLD', '^NDX','SSO','UWM','TQQQ','UPRO','TNA','SPYU','MSTU','NVDL','PLTU','PTIR','QQQ','SPY']
    
    for symbol in symbols:
        try:
            # Step 2: Fetch historical data for the symbol
            print(f"Fetching data for symbol: {symbol}")
            data = yf.download(symbol, start='2006-06-21', end=today_date)
            if data.empty:
                print(f"No data fetched for symbol: {symbol}")
                continue

            # Flatten multi-index columns if necessary
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Convert the index (dates) to the desired format (dd/mm/yyyy)
            data.index = data.index.strftime('%d/%m/%Y')
            
            # Adjust columns to match the expected CSV format:
            # Expected header: Date,Open,High,Low,Close,Adj Close,Volume
            # For example, for QLD the DataFrame might have: Price, Close, High, Low, Open, Volume
            # Remove the 'Price' column if it exists.
            if 'Price' in data.columns:
                data.drop(columns='Price', inplace=True)
            
            # Create 'Adj Close' if it's not present (using 'Close' as a fallback)
            if 'Adj Close' not in data.columns:
                data['Adj Close'] = data['Close']
            
            # Reorder the columns to match the expected header.
            expected_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
            data = data[expected_cols]
            
            # Step 3: Save the data to a CSV file with the symbol as a prefix.
            sanitized_symbol = symbol.replace('^', '')
            csv_filename = f'{sanitized_symbol.lower()}_stock_data.csv'
            
            # Write CSV with "Date" as the index label so the header becomes:
            # Date,Open,High,Low,Close,Adj Close,Volume
            data.to_csv(csv_filename, index=True, index_label='Date')
            print(f"CSV {csv_filename} saved successfully.")

            # Step 4: Validate and fix the CSV if necessary.
            if not validate_and_fix_csv(csv_filename):
                print(f"Skipping upload for {csv_filename} due to validation failure.")
                continue  # Skip uploading this file

            # Step 5: Get the current file's SHA from GitHub (if it exists)
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

            # Step 6: Read the new CSV file and encode it in base64.
            with open(csv_filename, 'rb') as f:
                content = f.read()
            content_base64 = base64.b64encode(content).decode('utf-8')

            # Step 7: Create the payload for the GitHub API request.
            commit_message = f'Update {sanitized_symbol} stock data'
            payload = {
                'message': commit_message,
                'content': content_base64,
                'branch': branch
            }
            if sha:
                payload['sha'] = sha

            # Step 8: Push the file to the repository.
            response = requests.put(url, headers=headers, json=payload)
            if response.status_code in [200, 201]:
                print(f'File {csv_filename} updated successfully in the repository.')
            else:
                print(f'Failed to update the file {csv_filename} in the repository.')
                print('Response:', response.json())

        except Exception as e:
            print(f'An error occurred while processing symbol {symbol}: {e}')

if __name__ == "__main__":
    main()
