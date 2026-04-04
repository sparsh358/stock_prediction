"""
Bulk Download Script for Multiple Indian Stocks
Downloads historical data for many stocks in parallel
"""

import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
from indian_stocks_config import INDIAN_STOCKS, STARTER_STOCKS, HIGH_VOLUME_STOCKS

def download_stock_data(symbol, start_date='2023-01-01', end_date=None, save=True):
    """
    Download historical data for a single stock
    
    Args:
        symbol: Stock symbol (e.g., 'TCS.NS')
        start_date: Start date for data
        end_date: End date for data (default: today)
        save: Whether to save to CSV
    
    Returns:
        DataFrame with OHLCV data
    """
    
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        print(f"  Downloading {symbol}...", end=" ", flush=True)
        data = yf.download(symbol, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            print(f"NO DATA")
            return None
        
        data = data.reset_index()
        
        if save:
            # Create data directory if needed
            os.makedirs('data', exist_ok=True)
            filename = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
            data.to_csv(filename, index=False)
        
        print(f"OK ({len(data)} rows)")
        return data
    
    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")
        return None


def bulk_download(stocks=None, num_workers=5, save=True):
    """
    Download multiple stocks in parallel
    
    Args:
        stocks: List of stock symbols (default: STARTER_STOCKS)
        num_workers: Number of parallel download threads
        save: Whether to save each stock to CSV
    
    Returns:
        Dictionary with stock data
    """
    
    if stocks is None:
        stocks = STARTER_STOCKS
    
    print("="*80)
    print(f"BULK DOWNLOAD - {len(stocks)} Stocks")
    print("="*80)
    print(f"Start Date: 2023-01-01")
    print(f"End Date: Today")
    print(f"Workers: {num_workers}\n")
    
    results = {}
    
    # Use ThreadPoolExecutor for parallel downloads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(download_stock_data, symbol, save=save): symbol 
            for symbol in stocks
        }
        
        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                data = future.result()
                results[symbol] = data
            except Exception as e:
                print(f"Error downloading {symbol}: {e}")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    successful = sum(1 for v in results.values() if v is not None)
    print(f"Successful: {successful}/{len(stocks)}")
    
    if save:
        print(f"Saved to: data/*.csv")
    
    total_rows = sum(len(v) for v in results.values() if v is not None)
    print(f"Total rows: {total_rows:,}")
    
    return results


def download_by_sector(sector_name, num_workers=3):
    """Download all stocks in a specific sector"""
    
    from indian_stocks_config import SECTOR_GROUPS
    
    if sector_name not in SECTOR_GROUPS:
        print(f"Sector not found. Available sectors:")
        for s in SECTOR_GROUPS.keys():
            print(f"  - {s}")
        return
    
    stocks = SECTOR_GROUPS[sector_name]
    print(f"\nDownloading {sector_name} ({len(stocks)} stocks)")
    return bulk_download(stocks, num_workers=num_workers)


def check_data_status():
    """Check which stocks have been downloaded"""
    
    print("="*80)
    print("DATA STATUS")
    print("="*80)
    
    for symbol in STARTER_STOCKS:
        filename = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
        if os.path.exists(filename):
            df = pd.read_csv(filename)
            print(f"{symbol:15} : {len(df):4} rows  (Latest: {df.iloc[-1]['Date']})")
        else:
            print(f"{symbol:15} : NOT DOWNLOADED")


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'all':
            stocks = list(INDIAN_STOCKS.keys())
            print(f"Downloading ALL {len(stocks)} stocks (this will take a while)...")
            bulk_download(stocks, num_workers=10, save=True)
        
        elif sys.argv[1] == 'high-volume':
            print(f"Downloading HIGH VOLUME stocks...")
            bulk_download(HIGH_VOLUME_STOCKS, num_workers=5, save=True)
        
        elif sys.argv[1] == 'check':
            check_data_status()
        
        elif sys.argv[1].startswith('sector:'):
            sector = sys.argv[1].replace('sector:', '')
            download_by_sector(sector)
        
        else:
            # Download single stock
            download_stock_data(sys.argv[1])
    
    else:
        # Default: Download starter set
        print("No argument provided. Downloading STARTER set...")
        bulk_download(STARTER_STOCKS, num_workers=5, save=True)
        
        print("\n" + "="*80)
        print("USAGE")
        print("="*80)
        print("python bulk_download.py                    # Download starter set (10 stocks)")
        print("python bulk_download.py all                # Download all 40+ stocks")
        print("python bulk_download.py high-volume        # Download top 10 stocks")
        print("python bulk_download.py sector:Banking     # Download Banking sector")
        print("python bulk_download.py TCS.NS             # Download single stock")
        print("python bulk_download.py check              # Check download status")
