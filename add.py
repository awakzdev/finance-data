import sys
import os
import subprocess

SYMBOLS_FILE = 'symbols.csv'

def add_symbol(symbol):
    symbol = symbol.strip().upper()
    if not symbol:
        print("Empty symbol received.")
        return False

    symbols = []
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, 'r', encoding='utf-8') as f:
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

def run_main_py():
    print("Running main.py...")
    result = subprocess.run(["python", "main.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("Errors during main.py execution:")
        print(result.stderr)

def main():
    if len(sys.argv) < 2:
        print("No symbol provided.")
        return

    new_symbol = sys.argv[1]
    added = add_symbol(new_symbol)
    if added:
        print("No need to run main.py")
    # Call main.py regardless of whether it was added or already existed
    else:
        run_main_py()

if __name__ == '__main__':
    main()
