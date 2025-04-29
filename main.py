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
    return [col.strip() for col in header_row]

def validate_and_fix_csv(csv_filename):
    expected_header = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as infile:
            lines = list(csv.reader(infile))
        header_index = -1
        for i, row in enumerate(lines):
            if normalize_header(row) == expected_header:
                header_index = i
                break
        if header_index < 0 or len(lines) < header_index + 2:
            print(f"❌ {csv_filename} is missing a valid header or data.")
            return False

        valid = [lines[header_index]]
        for row in lines[header_index+1:]:
            if not row: continue
            if date_pattern.match(row[0]) and len(row) == len(expected_header):
                valid.append(row)
            else:
                break

        if len(valid) < 2:
            print(f"❌ No valid data in {csv_filename}.")
            return False

        with open(csv_filename, 'w', newline='', encoding='utf-8') as outfile:
            csv.writer(outfile).writerows(valid)
        print(f"✅ CSV {csv_filename} validated/fixed.")
        return True

    except Exception as e:
        print(f"❌ Error validating {csv_filename}: {e}")
        return False

def main(symbol: str = None):
    # load token
    load_dotenv()
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    print(f"TOKEN loaded ({len(github_token)} chars)")

    repo   = 'awakzdev/finance-data'
    branch = 'main'
    today  = datetime.now().strftime('%Y-%m-%d')

    # symbols list
    if symbol:
        symbols = [symbol.upper()]
    else:
        if not os.path.exists('symbols.csv'):
            raise FileNotFoundError("symbols.csv not found")
        symbols = [s.strip().upper() for s in open('symbols.csv') if s.strip()]

    for sym in symbols:
        try:
            print(f"\n🔍 Fetching {sym}…")
            df = yf.download(sym, start='2006-06-21', end=today)
            if df.empty:
                print(f"⚠️ No data for {sym}, skipping.")
                continue

            # flatten & format
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.index = df.index.strftime('%d/%m/%Y')
            if 'Price' in df.columns:
                df.drop(columns='Price', inplace=True)
            if 'Adj Close' not in df.columns:
                df['Adj Close'] = df['Close']
            df = df[['Open','High','Low','Close','Adj Close','Volume']]

            # write individual CSV
            s = sym.replace('^','').lower()
            csv_file = f"{s}_stock_data.csv"
            df.to_csv(csv_file, index=True, index_label='Date')
            print(f"💾 Wrote {csv_file}")

            if not validate_and_fix_csv(csv_file):
                print(f"⚠️ Skipping upload for {csv_file}")
                continue

            # ─── Special: build qld2_stock_data.csv ───
            if sym == 'QLD':
                pred_file     = 'predictedQLD.csv'
                combined_file = 'qld2_stock_data.csv'

                # read actual
                actual_df = pd.read_csv(csv_file,
                                        parse_dates=['Date'],
                                        index_col='Date',
                                        dayfirst=True)

                # read predicted or fallback empty
                if os.path.exists(pred_file):
                    pred_df = pd.read_csv(pred_file,
                                          parse_dates=['Date'],
                                          index_col='Date',
                                          dayfirst=True)
                else:
                    print(f"⚠️ {pred_file} not found, combining empty + actual only.")
                    pred_df = pd.DataFrame(columns=actual_df.columns)

                # concat & write
                combo = pd.concat([pred_df, actual_df])
                combo.index = combo.index.strftime('%d/%m/%Y')
                combo.to_csv(combined_file, index_label='Date')
                print(f"💾 Wrote {combined_file}")

                # validate combined
                if not validate_and_fix_csv(combined_file):
                    print(f"⚠️ Combined file {combined_file} failed validation.")
                # (you can add GitHub upload logic here if you like)

            # ─── GitHub upload for the single CSV ───
            url     = f"https://api.github.com/repos/{repo}/contents/{csv_file}"
            headers = {'Authorization': f"token {github_token}"}
            r       = requests.get(url, headers=headers)
            status  = r.status_code
            sha     = r.json().get('sha') if status == 200 else None

            action = "Updating" if sha else "Creating"
            print(f"{action} {csv_file} in repo…")

            content_b64 = base64.b64encode(open(csv_file,'rb').read()).decode()
            payload = {
                'message': f"Update {s} stock data",
                'content': content_b64,
                'branch': branch
            }
            if sha:
                payload['sha'] = sha

            put = requests.put(url, headers=headers, json=payload)
            if put.status_code in (200,201):
                print(f"✅ {csv_file} pushed to GitHub.")
            else:
                print(f"❌ GitHub error for {csv_file}:", put.json())

        except Exception as e:
            print(f"❌ Error processing {sym}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Fetch & upload stock CSVs")
    parser.add_argument("--symbol", "-s", help="Single symbol to process", default=None)
    args = parser.parse_args()
    main(symbol=args.symbol)
