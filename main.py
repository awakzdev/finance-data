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
    Validates and fixes a CSV to ensure it starts with the header:
    Date,Open,High,Low,Close,Adj Close,Volume
    and that all subsequent rows begin with a valid dd/mm/yyyy date.
    """
    expected_header = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')

    try:
        with open(csv_filename, 'r', encoding='utf-8', newline='') as f:
            rows = list(csv.reader(f))

        # find header index
        header_idx = next((i for i, row in enumerate(rows)
                           if normalize_header(row) == expected_header), None)
        if header_idx is None:
            print(f"Header not found in {csv_filename}.")
            return False

        valid_rows = []
        for row in rows[header_idx:]:
            if not row:
                continue
            if row == rows[header_idx]:  # header row
                valid_rows.append(row)
            elif date_pattern.match(row[0]) and len(row) == len(expected_header):
                valid_rows.append(row)
            else:
                break

        if len(valid_rows) < 2:
            print(f"No valid data rows in {csv_filename}.")
            return False

        with open(csv_filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(valid_rows)

        return True

    except Exception as e:
        print(f"Error validating {csv_filename}: {e}")
        return False


def update_qld2(repo, branch, github_token):
    """
    Updates qld2_stock_data.csv by:
      1) Generating predicted 2×QQQ history before QLD inception (first run).
      2) Appending new QLD rows since last date on subsequent runs.
    Then pushes the updated CSV back to GitHub.
    """
    fname = 'qld2_stock_data.csv'
    inception = '2006-06-21'
    today = datetime.now().strftime('%Y-%m-%d')

    if os.path.exists(fname):
        # read without auto-parsing, then coerce to datetime
        df = pd.read_csv(fname, index_col='Date', parse_dates=False)
        df.index = pd.to_datetime(df.index, dayfirst=True, errors='coerce')
        df = df[df.index.notna()]
        last = df.index.max()
        if last.strftime('%Y-%m-%d') >= today:
            print(f"qld2 up to date through {last.date()}")
            return
        # fetch from the day after last
        start = (last + timedelta(days=1)).strftime('%Y-%m-%d')
        new = yf.download('QLD', start=start, end=today)
        if not new.empty:
            if isinstance(new.columns, pd.MultiIndex):
                new.columns = new.columns.get_level_values(0)
            new.index = new.index.strftime('%d/%m/%Y')
            if 'Adj Close' not in new.columns:
                new['Adj Close'] = new['Close']
            new = new[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
            df_new = pd.concat([df, new])
            df_new.to_csv(fname, index_label='Date')
            print(f"Appended {len(new)} new rows to {fname}.")
    else:
        # first-ever run: build predicted history from QQQ
        hist = yf.download('QQQ', start='2000-01-01', end=inception)
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        for c in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
            hist[c] = hist[c] * 2
        hist.index = hist.index.strftime('%d/%m/%Y')
        hist.to_csv(fname, index_label='Date')
        print(f"Initialized predicted history in {fname}.")
        # now recursively append actual QLD
        update_qld2(repo, branch, github_token)
        return

    # push back to GitHub
    if validate_and_fix_csv(fname):
        url = f'https://api.github.com/repos/{repo}/contents/{fname}'
        headers = {'Authorization': f'token {github_token}'}
        resp = requests.get(url, headers=headers)
        sha = resp.status_code == 200 and resp.json().get('sha')
        with open(fname, 'rb') as f:
            content = base64.b64encode(f.read()).decode()
        payload = {
            'message': 'Update qld2_stock_data.csv',
            'content': content,
            'branch': branch
        }
        if sha:
            payload['sha'] = sha
        put = requests.put(url, headers=headers, json=payload)
        if put.status_code in (200, 201):
            print(f"Pushed {fname} ({'updated' if sha else 'created'}).")
        else:
            print(f"Failed push: {put.json()}")


def main(symbol=None):
    load_dotenv()
    token = os.getenv('TOKEN')
    if not token:
        raise RuntimeError("TOKEN env var not set")
    repo = 'awakzdev/finance-data'
    branch = 'main'
    today = datetime.now().strftime('%Y-%m-%d')

    if symbol:
        syms = [symbol]
    else:
        with open('symbols.csv', 'r', encoding='utf-8') as f:
            syms = [s.strip() for s in f if s.strip()]

    for s in syms:
        if s.lower() == 'qld2':
            update_qld2(repo, branch, token)
            continue
        print(f"Fetching {s}")
        df = yf.download(s, start='2006-06-21', end=today)
        if df.empty:
            print(f"No data for {s}")
            continue
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.index = df.index.strftime('%d/%m/%Y')
        if 'Adj Close' not in df.columns:
            df['Adj Close'] = df['Close']
        df = df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        fn = f"{s.replace('^','').lower()}_stock_data.csv"
        df.to_csv(fn, index_label='Date')
        if not validate_and_fix_csv(fn):
            continue
        url = f'https://api.github.com/repos/{repo}/contents/{fn}'
        hdr = {'Authorization': f'token {token}'}
        r = requests.get(url, headers=hdr)
        sha = r.status_code == 200 and r.json().get('sha')
        with open(fn, 'rb') as f:
            cb = base64.b64encode(f.read()).decode()
        pl = {'message': f'Update {s} data', 'content': cb, 'branch': branch}
        if sha:
            pl['sha'] = sha
        p = requests.put(url, headers=hdr, json=pl)
        print(f"Pushed {fn}: {p.status_code}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--symbol', default=None)
    args = parser.parse_args()
    main(symbol=args.symbol)
