"""Quick verification of updated data"""
import os
import pandas as pd

print('Checking updated data files...\n')
stocks = ['TCS_NS', 'INFY_NS', 'HDFCBANK_NS', 'RELIANCE_NS', 'ICICIBANK_NS', 'SBIN_NS', 'MARUTI_NS']

for stock in stocks:
    filepath = f'data/{stock}_ohlcv.csv'
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
        latest = df.iloc[-1]
        print(f'✓ {stock}')
        print(f'  Rows: {len(df)} | Latest Date: {latest["Date"]} | Close: {latest["Close"]:.2f}')
    else:
        print(f'✗ {stock} - FILE NOT FOUND')

print('\n✅ Data cross-check complete!')
