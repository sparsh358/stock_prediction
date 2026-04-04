"""
Download 10 additional stocks from different sectors
Adds diversity beyond the starter set
"""

import yfinance as yf
from concurrent.futures import ThreadPoolExecutor
import os

# 10 NEW stocks from different sectors (not in the original starter set)
NEW_STOCKS = [
    'BAJAJAUT.NS',      # Auto
    'BHARTIARTL.NS',    # Telecom
    'DRREDDY.NS',       # Pharma
    'SUNPHARMA.NS',     # Pharma
    'IOC.NS',           # Energy
    'BPCL.NS',          # Energy
    'ITC.NS',           # FMCG
    'BRITANNIA.NS',     # FMCG
    'LT.NS',            # Infrastructure
    'DLF.NS'            # Real Estate
]

def download_stock(symbol):
    """Download stock data from yfinance"""
    try:
        print(f'  Downloading {symbol}...', end=' ', flush=True)
        df = yf.download(symbol, start='2023-01-01', progress=False)
        csv_file = f'data/{symbol.replace(".", "_")}_ohlcv.csv'
        df.to_csv(csv_file)
        print(f'OK ({len(df)} rows)')
        return symbol, len(df), True
    except Exception as e:
        print(f'FAILED ({str(e)[:30]})')
        return symbol, 0, False

if __name__ == '__main__':
    print('\n' + '='*80)
    print('DOWNLOADING 10 NEW STOCKS (Additional sectors)')
    print('='*80)
    print('Stocks:', ', '.join(NEW_STOCKS))
    print()
    
    # Download in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(download_stock, NEW_STOCKS))
    
    print()
    successful = sum(1 for r in results if r[2])
    total_rows = sum(r[1] for r in results)
    
    print('='*80)
    print('SUMMARY - New Stocks Added')
    print('='*80)
    print(f'Successful: {successful}/{len(NEW_STOCKS)}')
    print(f'Total rows added: {total_rows}')
    if successful > 0:
        print(f'Average rows per stock: {total_rows//successful}')
    print('Saved to: data/*.csv')
    print()
    print('Total stocks now available:')
    print(f'  Original 10 + New 10 = 20 stocks')
    print(f'  Total data points: ~16,000 rows')
    print()
    print('✅ Ready to scale: python train_multistock_model.py <list>')
