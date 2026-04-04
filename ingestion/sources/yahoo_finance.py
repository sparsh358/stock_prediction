import os
import sys
import yfinance as yf
import pandas as pd

def fetch_stock_data(symbol="AAPL", start="2015-01-01", end="2026-04-01"):
    """Fetch stock data from Yahoo Finance"""
    print(f"Downloading {symbol}...")
    df = yf.download(symbol, start=start, end=end, progress=False)
    df.reset_index(inplace=True)
    print(f"Downloaded {len(df)} rows for {symbol}")
    return df

if __name__ == "__main__":
    # Get symbol from command line arg or use default
    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    
    os.makedirs("data", exist_ok=True)
    
    df = fetch_stock_data(symbol)
    filename = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
    df.to_csv(filename, index=False)
    print(f"Saved to {filename}")

    print("Data saved successfully!")