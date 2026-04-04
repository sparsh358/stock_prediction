"""
INTRADAY DATA DOWNLOADER - Add Hourly/Minute Candles to Expand Dataset
Converts from daily data to multiple timeframes for bigger dataset
"""

import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures


def download_intraday_data(symbol, interval='1h', period='10y'):
    """
    Download intraday data (hourly/minute candles)
    
    Intervals available:
    - '1m', '5m', '15m', '30m', '60m' (1m-60m)
    - '1h', '1d', '1wk', '1mo'
    
    Note: 1m and 5m limited to 60 days, use '1h' for longer history
    """
    try:
        print(f"  Downloading {symbol} ({interval})...", end=" ", flush=True)
        
        # For minute data, Yahoo Finance limits to 60 days
        # For hourly, we can get up to 10 years
        data = yf.download(symbol, interval=interval, period=period, progress=False)
        
        if data.empty:
            print("NO DATA")
            return None, 0
        
        data.reset_index(inplace=True)
        print(f"✓ ({len(data)} rows)")
        
        return data, os.path.getsize(f"temp_{symbol}.csv") if os.path.exists(f"temp_{symbol}.csv") else 0
    except Exception as e:
        print(f"ERROR: {str(e)[:40]}")
        return None, 0


def save_intraday_data(data, symbol, interval):
    """Save intraday data to organized directory structure"""
    if data is None or data.empty:
        return 0
    
    # Create directory structure
    market_dir = f'data/intraday/{interval}'
    os.makedirs(market_dir, exist_ok=True)
    
    # Save file
    filename = f"{market_dir}/{symbol.replace('.', '_')}.csv"
    data.to_csv(filename, index=False)
    
    file_size = os.path.getsize(filename)
    return file_size


def download_intraday_expansion(interval='1h', stocks_list=None, max_workers=5):
    """
    Download intraday data for specified stocks
    
    Args:
        interval: '1m', '5m', '15m', '1h', '1d'
        stocks_list: List of symbols, or None for top stocks
        max_workers: Number of parallel downloads
    """
    print("\n" + "="*100)
    print(f"INTRADAY DATA DOWNLOADER - {interval.upper()} CANDLES")
    print("="*100)
    
    # Default top stocks if none provided
    if stocks_list is None:
        stocks_list = [
            # Top Indian stocks
            'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'RELIANCE.NS', 'ICICIBANK.NS',
            'SBIN.NS', 'MARUTI.NS', 'HINDUNILVR.NS', 'AXISBANK.NS', 'HDFCBANK.NS',
            'BHARTIARTL.NS', 'DRREDDY.NS', 'SUNPHARMA.NS', 'IOC.NS', 'BPCL.NS',
            # Top US stocks
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'JPM', 'V', 'JNJ'
        ]
    
    print(f"\nInterval: {interval}")
    print(f"Stocks: {len(stocks_list)}")
    print(f"Workers: {max_workers}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Time period info
    period_info = {
        '1m': "60 days",
        '5m': "60 days",
        '15m': "60 days",
        '30m': "60 days",
        '1h': "10 years",
        '1d': "10 years"
    }
    print(f"Historical period: {period_info.get(interval, '10 years')}\n")
    
    completed = 0
    total_size = 0
    failed = 0
    
    # Download in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_intraday_data, symbol, interval): symbol 
            for symbol in stocks_list
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            symbol = futures[future]
            try:
                data, size = future.result()
                if data is not None:
                    saved_size = save_intraday_data(data, symbol, interval)
                    completed += 1
                    total_size += saved_size
                    size_mb = saved_size / (1024 * 1024)
                    print(f"  [{i}/{len(stocks_list)}] {symbol:15} | {size_mb:8.2f} MB")
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                print(f"  [{i}/{len(stocks_list)}] {symbol:15} | ERROR")
    
    # Summary
    print("\n" + "="*100)
    print("SUMMARY")
    print("="*100)
    
    total_gb = total_size / (1024**3)
    print(f"\n✓ Completed: {completed} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"\nTotal Size Added: {total_gb:.2f} GB")
    
    if completed > 0:
        print(f"Average per stock: {(total_size / completed) / (1024**2):.2f} MB")
    
    print(f"\nData location:")
    print(f"  data/intraday/{interval}/")


def show_expansion_impact(base_size_gb=10):
    """Show impact of adding different timeframes"""
    print("\n" + "="*100)
    print("EXPANSION IMPACT ANALYSIS")
    print("="*100)
    
    print(f"\nAssuming current dataset: {base_size_gb} GB\n")
    
    print("Adding intraday data for 100 stocks:")
    print("  " + "-"*70)
    print(f"  {'Interval':<15} | {'Per Stock':<15} | {'100 Stocks':<15} | {'Total':<15}")
    print("  " + "-"*70)
    
    scenarios = [
        ('1-hour', 0.08, 'Recommended: 10+ years'),
        ('15-minute', 0.25, 'Medium detail'),
        ('5-minute', 0.50, 'High detail'),
        ('1-minute', 2.00, '⚠️ Very large! 60 days only'),
    ]
    
    for interval, per_stock_gb, notes in scenarios:
        total_100_gb = per_stock_gb * 100
        final_gb = base_size_gb + total_100_gb
        print(f"  {interval:<15} | {per_stock_gb:>8.2f} GB        | {total_100_gb:>8.2f} GB        | {final_gb:>8.2f} GB       {notes}")
    
    print("  " + "-"*70)
    
    print("\nRecommendation for 20-25 GB target:")
    print("  1. Start with hourly data for top 100 stocks (+8 GB)")
    print("  2. Then add 5-minute data for top 50 stocks (+2.5 GB)")
    print("  3. Combine with technical indicators (+0.5-1 GB)")
    print("  4. Add sentiment data (+1-2 GB)")


if __name__ == "__main__":
    show_expansion_impact()
    
    print("\n" + "="*100)
    print("USAGE EXAMPLES")
    print("="*100)
    print("""
# Download 1-hour candles for top stocks (RECOMMENDED)
python download_intraday_data.py --interval=1h --stocks=top100

# Download 5-minute data for specific stocks
python download_intraday_data.py --interval=5m --stocks=TCS.NS,INFY.NS,RELIANCE.NS

# Download minute-level data (⚠️ will be VERY LARGE)
python download_intraday_data.py --interval=1m --stocks=AAPL,MSFT,GOOGL

Current estimates:
  - 1h data for 100 stocks: ~8 GB
  - 5m data for 100 stocks: ~25 GB alone (too much!)
  - Combination approach: 20-25 GB total dataset

Recommended final dataset composition:
  ├─ Daily OHLCV (200+ stocks, 11 years) = ~5-8 GB
  ├─ Hourly candles (100 stocks, 10 years) = ~8 GB
  ├─ Technical indicators = ~0.5-1 GB
  ├─ Sentiment data = ~1-2 GB
  └─ Macro indicators = ~0.5-1 GB
  
  TOTAL: 15-20 GB ✓
""")
    print("="*100)
