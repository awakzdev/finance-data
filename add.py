import sys
import os

SYMBOLS_FILE = 'symbols.csv'

def main():
    if len(sys.argv) < 2:
        print("No symbol provided.")
        return

    symbol = sys.argv[1].strip().upper()
    if not symbol:
        print("Empty symbol received.")
        return

    # Load existing symbols or initialize empty list
    symbols = []
    if os.path.exists(SYMBOLS_FILE):
        with open(SYMBOLS_FILE, 'r', encoding='utf-8') as f:
            symbols = [line.strip().upper() for line in f if line.strip()]

    # Add if not already present
    if symbol in symbols:
        print(f"Symbol {symbol} already exists in {SYMBOLS_FILE}.")
    else:
        symbols.append(symbol)
        symbols = sorted(set(symbols))  # Optional: keep sorted and unique
        with open(SYMBOLS_FILE, 'w', encoding='utf-8') as f:
            for sym in symbols:
                f.write(sym + '\n')
        print(f"Symbol {symbol} added to {SYMBOLS_FILE}.")

if __name__ == '__main__':
    main()
