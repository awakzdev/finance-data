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

# ── ensure all output files live next to this script ──
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

def normalize_header(header_row):
    return [col.strip() for col in header_row]

def validate_and_fix_csv(csv_filename):
    expected_header = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    date_pattern = re.compile(r'\d{2}/\d{2}/\d{4}')
    try:
        with open(csv_filename, 'r', newline='', encoding='utf-8') as infile:
            lines = list(csv.reader(infile))
        # find header
        header_index = next((i for i,row in enumerate(lines)
                             if normalize_header(row)==expected_header), -1)
        if header_index<0 or len(lines)<header_index+2:
            print(f"❌ {csv_filename} missing header or data.")
            return False
        valid = [lines[header_index]]
        for row in lines[header_index+1:]:
            if not row: continue
            if date_pattern.match(row[0]) and len(row)==len(expected_header):
                valid.append(row)
            else:
                break
        if len(valid)<2:
            print(f"❌ No valid rows in {csv_filename}.")
            return False
        with open(csv_filename,'w',newline='',encoding='utf-8') as out:
            csv.writer(out).writerows(valid)
        print(f"✅ CSV {csv_filename} validated/fixed.")
        return True
    except Exception as e:
        print(f"❌ Error validating {csv_filename}: {e}")
        return False

def upload_to_github(local_path, repo, branch, github_token, commit_msg):
    """Generic helper to PUT a file up to GitHub"""
    name = os.path.basename(local_path)
    url = f"https://api.github.com/repos/{repo}/contents/{name}"
    headers = {'Authorization': f"token {github_token}"}
    # check for existing sha
    r = requests.get(url, headers=headers)
    sha = r.json().get('sha') if r.status_code==200 else None
    print(f"{'Updating' if sha else 'Creating'} {name} in repo…")
    content_b64 = base64.b64encode(open(local_path,'rb').read()).decode()
    payload = {
        'message': commit_msg,
        'content': content_b64,
        'branch': branch
    }
    if sha:
        payload['sha'] = sha
    p = requests.put(url, headers=headers, json=payload)
    if p.status_code in (200,201):
        print(f"✅ {name} pushed to GitHub.")
    else:
        print(f"❌ GitHub error for {name}:", p.json())

def main(symbol: str=None):
    load_dotenv()
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    print(f"TOKEN loaded ({len(github_token)} chars)")

    repo   = 'awakzdev/finance-data'
    branch = 'main'
    today  = datetime.now().strftime('%Y-%m-%d')

    # build symbols list
    if symbol:
        symbols = [symbol.upper()]
    else:
        csv_syms = os.path.join(SCRIPT_DIR, 'symbols.csv')
        if not os.path.exists(csv_syms):
            raise FileNotFoundError("symbols.csv not found")
        symbols = [s.strip().upper() for s in open(csv_syms) if s.strip()]

    for sym in symbols:
        try:
            print(f"\n🔍 Fetching {sym}…")
            df = yf.download(sym, start='2006-06-21', end=today)
            if df.empty:
                print(f"⚠️ No data for {sym}, skipping.")
                continue

            # flatten & reformat
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            df.index = df.index.strftime('%d/%m/%Y')
            if 'Price' in df.columns:
                df.drop(columns='Price', inplace=True)
            if 'Adj Close' not in df.columns:
                df['Adj Close'] = df['Close']
            df = df[['Open','High','Low','Close','Adj Close','Volume']]

            # write individual CSV
            base = sym.replace('^','').lower()
            csv_file = os.path.join(SCRIPT_DIR, f"{base}_stock_data.csv")
            df.to_csv(csv_file, index=True, index_label='Date')
            print(f"💾 Wrote {csv_file}")

            if not validate_and_fix_csv(csv_file):
                print(f"⚠️ Skipping upload for {csv_file}")
                continue

            # upload it
            upload_to_github(
                local_path=csv_file,
                repo=repo,
                branch=branch,
                github_token=github_token,
                commit_msg=f"Update {base} stock data"
            )

            # ── if QLD, build & upload qld2_stock_data.csv ──
            if sym == 'QLD':
                pred_file     = os.path.join(SCRIPT_DIR, 'predictedQLD.csv')
                combined_path = os.path.join(SCRIPT_DIR, 'qld2_stock_data.csv')

                # actual
                actual_df = pd.read_csv(csv_file,
                                        parse_dates=['Date'],
                                        index_col='Date',
                                        dayfirst=True)
                # predicted, or empty
                if os.path.exists(pred_file):
                    pred_df = pd.read_csv(pred_file,
                                          parse_dates=['Date'],
                                          index_col='Date',
                                          dayfirst=True)
                else:
                    print(f"⚠️ {pred_file} not found, using actual only.")
                    pred_df = pd.DataFrame(columns=actual_df.columns)

                combo = pd.concat([pred_df, actual_df])
                # ── Force a DatetimeIndex before formatting ──
                combo.index = pd.to_datetime(combo.index, dayfirst=True)
                combo.index = combo.index.map(lambda dt: dt.strftime('%d/%m/%Y'))
                combo.to_csv(combined_path, index_label='Date')
                print(f"💾 Wrote {combined_path}")

                if validate_and_fix_csv(combined_path):
                    upload_to_github(
                        local_path=combined_path,
                        repo=repo,
                        branch=branch,
                        github_token=github_token,
                        commit_msg="Update qld2 combined data"
                    )
                else:
                    print(f"⚠️ Skipping upload for combined file.")

        except Exception as e:
            print(f"❌ Error processing {sym}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Fetch & upload stock CSVs")
    parser.add_argument(
        "--symbol", "-s",
        help="Single symbol to process",
        default=None
    )
    args = parser.parse_args()
    main(symbol=args.symbol)
