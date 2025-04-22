import sys
import os
import base64
import requests
from dotenv import load_dotenv
import subprocess

SYMBOLS_FILE = 'symbols.csv'
REPO = 'awakzdev/finance-data'
BRANCH = 'main'

def commit_symbols_csv(github_token):
    """Push the updated symbols.csv file to GitHub"""
    url = f"https://api.github.com/repos/{REPO}/contents/{SYMBOLS_FILE}"
    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github+json'
    }

    try:
        # Read new file content and encode
        with open(SYMBOLS_FILE, 'rb') as f:
            content = f.read()
        content_base64 = base64.b64encode(content).decode('utf-8')

        # Check if file already exists to get its SHA
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            sha = response.json()['sha']
            print(f"{SYMBOLS_FILE} exists in repo, updating it.")
        elif response.status_code == 404:
            sha = None
            print(f"{SYMBOLS_FILE} does not exist in repo, creating it.")
        else:
            print(f"Unexpected error checking symbols.csv: {response.status_code}, {response.text}")
            return

        payload = {
            'message': 'Add symbol to symbols.csv',
            'content': content_base64,
            'branch': BRANCH
        }
        if sha:
            payload['sha'] = sha

        # PUT request to update or create the file
        put_response = requests.put(url, headers=headers, json=payload)
        if put_response.status_code in [200, 201]:
            print(f"{SYMBOLS_FILE} committed to GitHub successfully.")
        else:
            print(f"Failed to commit {SYMBOLS_FILE} to GitHub.")
            print("Response:", put_response.json())

    except Exception as e:
        print(f"Exception during GitHub update: {e}")

def add_symbol(symbol):
    symbol = symbol.strip().replace('\ufeff', '').upper()
    if not symbol:
        print("Empty symbol received.")
        return False

    symbols = []
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, 'r', encoding='utf-8-sig') as f:
            symbols = [line.strip().upper() for line in f if line.strip()]

    if symbol in symbols:
        print(f"Symbol {symbol} already exists in {SYMBOLS_FILE}.")
        return False
    else:
        symbols.append(symbol)
        symbols = sorted(set(symbols))
        with open(SYMBOLS_FILE, 'w', encoding='utf-8') as f:
            for sym in symbols:
                f.write(sym + '\n')
        print(f"Symbol {symbol} added to {SYMBOLS_FILE}.")
        return True

def run_main_py(symbol=None):
    print("Running main.py...")
    
    cmd = ["python", "main.py"]
    if symbol:
        cmd += ["--symbol", symbol]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print(result.stdout)
    if result.stderr:
        print("Errors during main.py execution:")
        print(result.stderr)

def main():
    if len(sys.argv) < 2:
        print("No symbol provided.")
        return

    new_symbol = sys.argv[1]

    # Load GitHub token from environment
    load_dotenv()
    github_token = os.getenv('TOKEN')
    if not github_token:
        raise ValueError("TOKEN environment variable not set")
    else:
        print(f"TOKEN loaded, length: {len(github_token)} characters")

    added = add_symbol(new_symbol)

    if added:
        commit_symbols_csv(github_token)
        run_main_py(symbol=new_symbol)
    else:
        print("Symbol already exists, skipping main.py.")

if __name__ == '__main__':
    main()
