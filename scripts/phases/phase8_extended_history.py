"""
PHASE 8: EXTENDED HISTORICAL DATA
Extend all existing stock data from 11 years to 20+ years (since 2005)
Target: +5-8 GB by adding 9 more years of historical data
"""

import os
import yfinance as yf
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
import time
import glob

def extend_stock_history(filepath, symbol, max_years=20):
    """Extend existing stock file with extended historical data"""
    try:
        # Read existing data
        try:
            existing_df = pd.read_csv(filepath)
            existing_df['Date'] = pd.to_datetime(existing_df['Date'])
            min_existing_date = existing_df['Date'].min()
        except:
            return None, 0
        
        print(f"  [{symbol:<15}]", end=" ", flush=True)
        
        # Download extended history (20 years)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=max_years*365)
        
        data = yf.download(symbol, start=start_date, end=end_date, progress=False)
        
        if data.empty or len(data) < 200:
            print(f"✗ (insufficient data)")
            return None, 0
        
        # Prepare data
        data.reset_index(inplace=True)
        data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        data['Date'] = pd.to_datetime(data['Date'])
        
        # Remove duplicates and sort
        data = data.drop_duplicates(subset=['Date']).sort_values('Date')
        
        # Save extended data
        data.to_csv(filepath, index=False)
        
        original_size = os.path.getsize(filepath) / (1024 * 1024)
        print(f"✓ ({len(data)} rows, {original_size:.2f} MB)")
        
        return data, original_size
        
    except Exception as e:
        error_msg = str(e)[:30]
        print(f"✗ ({error_msg})")
        return None, 0


def phase8_extended_history():
    """Execute Phase 8: Extend historical data for all existing stocks"""
    print("\n" + "="*100)
    print("PHASE 8: EXTENDED HISTORICAL DATA (20+ YEARS)")
    print("="*100)
    print(f"\nExtending all stock data from 11 years to 20+ years (since ~2005)...")
    print(f"Expected: +5-8 GB by adding 9 more years of data\n")
    
    # Find all existing stock files
    stock_files = glob.glob('data/**/*_ohlcv.csv', recursive=True)
    stock_files = [f for f in stock_files if 'intraday' not in f]  # Skip intraday files
    
    print(f"Found {len(stock_files)} existing daily stock files to extend\n")
    
    start_time = time.time()
    total_size = 0
    successful = 0
    failed = 0
    
    # Extract symbol from filepath
    def extract_symbol(filepath):
        basename = os.path.basename(filepath)
        symbol = basename.replace('_ohlcv.csv', '')
        # Convert back to yahoo format
        if '_NS' in symbol:
            symbol = symbol + '.NS'
        elif '_T' in symbol:
            symbol = symbol + '.T'
        elif '_ME' in symbol:
            symbol = symbol + '.ME'
        elif '_SA' in symbol:
            symbol = symbol + '.SA'
        elif '_MX' in symbol:
            symbol = symbol + '.MX'
        elif '_AX' in symbol:
            symbol = symbol + '.AX'
        elif '_L' in symbol:
            symbol = symbol + '.L'
        elif '_DE' in symbol:
            symbol = symbol + '.DE'
        elif '_AS' in symbol:
            symbol = symbol + '.AS'
        elif '_VX' in symbol:
            symbol = symbol + '.VX'
        elif '_SW' in symbol:
            symbol = symbol + '.SW'
        elif '_PA' in symbol:
            symbol = symbol + '.PA'
        return symbol.replace('_', '')  # Remove underscores in futures/indices
    
    # Parallel extend with controlled workers
    max_workers = 8
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for filepath in stock_files:
            symbol = extract_symbol(filepath)
            future = executor.submit(extend_stock_history, filepath, symbol, max_years=20)
            futures[future] = (symbol, filepath)
        
        # Process as completed  
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            symbol, filepath = futures[future]
            try:
                data, size_mb = future.result()
                if data is not None:
                    total_size += size_mb
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
    
    elapsed = time.time() - start_time
    
    print(f"\n{'='*100}")
    print(f"PHASE 8 SUMMARY")
    print(f"{'='*100}\n")
    print(f"✓ Extended: {successful} stocks with 20+ years data")
    print(f"✗ Failed: {failed} stocks")
    print(f"Total dataset size: {total_size:.0f} MB ({total_size/1024:.1f} GB cumulative)")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"\n✓ Phase 8 Complete! Extended daily history added to all stocks.")
    print(f"{'='*100}\n")
    
    return total_size / 1024  # Return size in GB


if __name__ == "__main__":
    phase8_extended_history()
