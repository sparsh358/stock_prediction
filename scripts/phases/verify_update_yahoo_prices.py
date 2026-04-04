"""
Cross-check and Update Stock Prices from Yahoo Finance
Verifies data accuracy and imports fresh prices for all stocks
"""

import os
import yfinance as yf
import pandas as pd
from datetime import datetime
from indian_stocks_config import INDIAN_STOCKS, STARTER_STOCKS, HIGH_VOLUME_STOCKS


def fetch_fresh_data(symbol, start_date='2015-01-01', end_date=None):
    """Fetch fresh data from Yahoo Finance"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        print(f"  Fetching {symbol} from Yahoo Finance...", end=" ", flush=True)
        data = yf.download(symbol, start=start_date, end=end_date, progress=False)
        data = data.reset_index()
        print(f"✓ ({len(data)} rows)")
        return data
    except Exception as e:
        print(f"✗ ERROR: {str(e)[:50]}")
        return None


def load_existing_data(symbol):
    """Load existing CSV data"""
    filename = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
    if os.path.exists(filename):
        try:
            df = pd.read_csv(filename)
            return df
        except Exception as e:
            print(f"  Error reading existing file: {e}")
            return None
    return None


def compare_data(existing_df, fresh_df, symbol):
    """Compare existing vs fresh data"""
    if existing_df is None:
        print(f"\n  ⚠ {symbol}: No existing data found - will create new file")
        return {'status': 'new', 'differences': 0}
    
    if fresh_df is None:
        print(f"\n  ✗ {symbol}: Could not fetch fresh data")
        return {'status': 'error', 'differences': 0}
    
    try:
        # Convert Date columns to datetime
        existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
        fresh_df['Date'] = pd.to_datetime(fresh_df['Date'], errors='coerce')
        
        # Find common dates
        existing_dates = set(existing_df['Date'].dropna())
        fresh_dates = set(fresh_df['Date'].dropna())
        common_dates = existing_dates & fresh_dates
        
        if len(common_dates) == 0:
            print(f"\n  ⚠ {symbol}: No common dates found")
            return {'status': 'no_overlap', 'differences': 0}
        
        # Check for price differences
        differences = 0
        existing_subset = existing_df[existing_df['Date'].isin(common_dates)].copy()
        fresh_subset = fresh_df[fresh_df['Date'].isin(common_dates)].copy()
        
        # Sort by date for comparison
        existing_subset = existing_subset.sort_values('Date').reset_index(drop=True)
        fresh_subset = fresh_subset.sort_values('Date').reset_index(drop=True)
        
        # Compare Close prices (main indicator)
        if 'Close' in existing_subset.columns and 'Close' in fresh_subset.columns:
            existing_close = pd.to_numeric(existing_subset['Close'], errors='coerce')
            fresh_close = pd.to_numeric(fresh_subset['Close'], errors='coerce')
            
            # Count price differences
            if len(existing_close) == len(fresh_close):
                # Allow small floating point differences
                diff_mask = (abs(existing_close - fresh_close) > 0.01)
                differences = diff_mask.sum()
        
        new_rows = len(fresh_dates - existing_dates)
        
        status = 'match' if differences == 0 else 'mismatch'
        
        print(f"\n  Status: {status.upper()}")
        print(f"    Common dates: {len(common_dates)}")
        print(f"    Price differences: {differences}")
        print(f"    New rows in fresh data: {new_rows}")
        print(f"    Existing rows: {len(existing_df)}")
        print(f"    Fresh rows: {len(fresh_df)}")
        
        return {'status': status, 'differences': differences, 'new_rows': new_rows}
    except Exception as e:
        print(f"\n  ⚠ {symbol}: Error comparing data - {str(e)[:50]}")
        return {'status': 'error', 'differences': 0}


def update_csv_file(fresh_df, symbol):
    """Save fresh data to CSV"""
    if fresh_df is None or fresh_df.empty:
        print(f"  ✗ Cannot save: No data for {symbol}")
        return
    
    filename = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
    os.makedirs('data', exist_ok=True)
    
    # Select only the columns we need: Date, Open, High, Low, Close, Volume
    # Yahoo Finance returns: Date, Open, High, Low, Close, Adj Close, Volume
    if 'Adj Close' in fresh_df.columns:
        fresh_df = fresh_df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
    
    fresh_df.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    fresh_df.to_csv(filename, index=False)
    print(f"  ✓ Updated: {filename}")


def verify_and_update_all_stocks():
    """Main verification and update process"""
    print("\n" + "="*80)
    print("YAHOO FINANCE DATA VERIFICATION & UPDATE")
    print("="*80)
    
    # 20 stocks for your model
    stocks_20 = [
        'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'RELIANCE.NS', 'ICICIBANK.NS',
        'SBIN.NS', 'MARUTI.NS', 'HINDUNILVR.NS', 'AXISBANK.NS', 'HDFCBANK.NS',
        'BHARTIARTL.NS', 'DRREDDY.NS', 'SUNPHARMA.NS', 'IOC.NS', 'BPCL.NS',
        'ITC.NS', 'BRITANNIA.NS', 'LT.NS', 'DLF.NS'
    ]
    
    stocks = stocks_20
    
    results_summary = {
        'match': [],
        'mismatch': [],
        'new': [],
        'error': [],
        'no_overlap': []
    }
    
    print(f"\nProcessing {len(stocks)} stocks...")
    print(f"Start Date: 2015-01-01")
    print(f"End Date: {datetime.now().strftime('%Y-%m-%d')}\n")
    
    for i, symbol in enumerate(stocks, 1):
        print(f"\n[{i}/{len(stocks)}] {symbol}")
        print("-" * 50)
        
        # Fetch fresh data
        fresh_data = fetch_fresh_data(symbol)
        
        # Load existing data
        existing_data = load_existing_data(symbol)
        
        # Compare
        comparison = compare_data(existing_data, fresh_data, symbol)
        status = comparison['status']
        
        # Update CSV with fresh data
        if fresh_data is not None:
            update_csv_file(fresh_data, symbol)
            results_summary[status].append(symbol)
        else:
            results_summary['error'].append(symbol)
    
    # Print summary
    print("\n\n" + "="*80)
    print("SUMMARY REPORT")
    print("="*80)
    print(f"\n✓ Matched (no discrepancies): {len(results_summary['match'])}")
    if results_summary['match']:
        print(f"  {', '.join(results_summary['match'][:5])}")
        if len(results_summary['match']) > 5:
            print(f"  ... and {len(results_summary['match']) - 5} more")
    
    print(f"\n⚠ Mismatched (differences found): {len(results_summary['mismatch'])}")
    if results_summary['mismatch']:
        print(f"  {', '.join(results_summary['mismatch'][:5])}")
        if len(results_summary['mismatch']) > 5:
            print(f"  ... and {len(results_summary['mismatch']) - 5} more")
    
    print(f"\n✓ New files created: {len(results_summary['new'])}")
    if results_summary['new']:
        print(f"  {', '.join(results_summary['new'][:5])}")
    
    print(f"\n✗ Errors: {len(results_summary['error'])}")
    if results_summary['error']:
        print(f"  {', '.join(results_summary['error'][:5])}")
    
    print(f"\nTotal updated: {len(stocks) - len(results_summary['error'])}/{len(stocks)}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "="*80)
    print("✓ All files have been updated with fresh Yahoo Finance data!")
    print("="*80)


if __name__ == "__main__":
    verify_and_update_all_stocks()
