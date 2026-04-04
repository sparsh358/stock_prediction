"""
MEGA DATASET EXPLORER - Analyze & Optimize Your Big Data
Shows dataset composition, storage breakdown, and recommendations
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import json


def get_directory_size(path):
    """Calculate total size of directory"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total_size += os.path.getsize(filepath)
    return total_size


def analyze_mega_dataset():
    """Analyze the mega dataset structure and composition"""
    print("\n" + "="*100)
    print("MEGA DATASET EXPLORER - DETAILED ANALYSIS")
    print("="*100)
    
    data_dir = Path('data')
    
    if not data_dir.exists():
        print("\n⚠️  'data' directory not found. Run download_mega_dataset.py first!")
        return
    
    # Collect statistics
    total_size = get_directory_size(data_dir)
    total_size_gb = total_size / (1024**3)
    
    print(f"\nDataset Location: {data_dir.absolute()}")
    print(f"Total Size: {total_size_gb:.2f} GB")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Analyze by subdirectory (market)
    print("="*100)
    print("BREAKDOWN BY MARKET")
    print("="*100)
    
    markets = {}
    
    # Market subdirectories
    for market_dir in data_dir.iterdir():
        if market_dir.is_dir():
            market_name = market_dir.name.upper()
            csv_files = list(market_dir.glob('*.csv'))
            
            if csv_files:
                market_size = get_directory_size(market_dir)
                market_size_gb = market_size / (1024**3)
                
                # Analyze first file to get row count
                first_file = csv_files[0]
                try:
                    df = pd.read_csv(first_file, nrows=1)
                    total_rows = sum(len(pd.read_csv(f, engine='c')) for f in csv_files)
                except:
                    total_rows = 0
                
                markets[market_name] = {
                    'stocks': len(csv_files),
                    'size_gb': market_size_gb,
                    'rows': total_rows,
                    'avg_file_size_mb': (market_size / len(csv_files)) / (1024**2)
                }
                
                print(f"\n{market_name}:")
                print(f"  Stocks: {len(csv_files)}")
                print(f"  Size: {market_size_gb:.2f} GB")
                print(f"  Total Rows: {total_rows:,}")
                print(f"  Avg per stock: {(market_size / len(csv_files)) / (1024**2):.2f} MB")
                print(f"  Files: {', '.join([f.stem for f in csv_files[:3]])}")
                if len(csv_files) > 3:
                    print(f"            ... +{len(csv_files)-3} more")
    
    # Summary statistics
    print("\n" + "="*100)
    print("SUMMARY STATISTICS")
    print("="*100)
    
    total_stocks = sum(m['stocks'] for m in markets.values())
    total_rows = sum(m['rows'] for m in markets.values())
    growth_target = 20  # GB
    
    print(f"\nTotal Stocks: {total_stocks}")
    print(f"Total Rows: {total_rows:,}")
    print(f"Current Size: {total_size_gb:.2f} GB")
    print(f"Target Size: {growth_target} GB")
    
    if total_size_gb > 0:
        remaining_gb = growth_target - total_size_gb
        if remaining_gb > 0:
            additional_stocks = int((remaining_gb / (total_size_gb / max(total_stocks, 1))) * total_stocks)
            print(f"\nTo reach {growth_target} GB:")
            print(f"  Additional space needed: {remaining_gb:.2f} GB")
            print(f"  Estimated additional stocks: ~{additional_stocks}")
        else:
            print(f"\n✅ Already exceeded {growth_target} GB target!")
    
    # On-disk format analysis
    print("\n" + "="*100)
    print("DATA COMPRESSION & OPTIMIZATION")
    print("="*100)
    
    print("\nCurrent format: CSV (uncompressed)")
    print("\nCompression potential:")
    print(f"  Gzip compression (zipfile): {total_size_gb * 0.25:.2f} GB (75% reduction)")
    print(f"  Parquet (optimized for ML): {total_size_gb * 0.35:.2f} GB (65% reduction)")
    print(f"  HDF5 (structured storage): {total_size_gb * 0.40:.2f} GB (60% reduction)")
    
    print("\nFor big data ML, consider:")
    print("  1. Convert to Parquet format for faster reading")
    print("  2. Use compression when archiving old data")
    print("  3. Create smaller views for prototyping")


def get_storage_requirements():
    """Calculate storage needed for different expansions"""
    print("\n" + "="*100)
    print("STORAGE REQUIREMENTS FOR EXPANSION")
    print("="*100)
    
    # Current average
    avg_stock_size_mb = 5  # Approximate
    
    print("\nEstimated sizes:")
    print("  " + "-"*60)
    print("  Configuration                        | Size      | Growth   | Time*")
    print("  " + "-"*60)
    
    configs = [
        ("Current (19 stocks)", 19, 1),
        ("Small (100 stocks)", 100, 1),
        ("Medium (300 stocks)", 300, 1),
        ("Large (500 stocks)", 500, 1),
        ("Large + Hourly (500 stocks)", 500, 4),
        ("Large + Daily+Hourly+Sentiment (500)", 500, 6),
    ]
    
    for config, stocks, multiplier in configs:
        size_gb = (stocks * avg_stock_size_mb * multiplier) / 1024
        time_hours = (stocks * multiplier) / 50  # 50 stocks/hour download speed
        
        status = "✓" if size_gb >= 20 else " "
        print(f"  {config:35} | {size_gb:6.2f} GB | {multiplier}x data | {time_hours:.1f}h")
    
    print("  " + "-"*60)
    print("  * Download time with 8 parallel workers")


def create_implementation_plan():
    """Create step-by-step implementation plan"""
    print("\n" + "="*100)
    print("STEP-BY-STEP IMPLEMENTATION PLAN - 20-25 GB DATASET")
    print("="*100)
    
    plan = """
PHASE 1: GLOBAL STOCK MARKETS (Estimated: ~5-8 GB, 2-3 hours)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 1: Download 200+ global stocks
  Command: python download_mega_dataset.py
  Output:
    - data/india/*.csv        (75 stocks)
    - data/usa/*.csv          (57 stocks) 
    - data/global/*.csv       (20 stocks)
  Result: ~5-8 GB

PHASE 2: EXPAND STOCK UNIVERSE (Estimated: +5-7 GB, 2-3 hours)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 2a: Add emerging market stocks
  - Brazil (IBOVESPA): 30+ stocks
  - Russia, Mexico, South Africa
  - Total: +50 stocks

Step 2b: Add commodities & indices
  - Oil, Gold, Copper, Silver (commodity futures)
  - S&P 500, Nifty 50, Sensex, FTSE 100, DAX, Nikkei
  - Total: +25 indices

Step 2c: Continue with more Indian stocks
  - Mid-cap & small-cap stocks
  - Total: +75 more Indian stocks

Result: +150-200 additional stocks = +5-7 GB
Total so far: ~10-15 GB

PHASE 3: INTRADAY DATA (Estimated: +3-5 GB, 3-4 hours)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 3: Add hourly candles for top 100 stocks
  Command: python download_intraday_data.py --interval=hourly --stocks=100
  Resolution: 1 hour candles (11 years)
  
  Alternative (more data):
  - Add 5-minute candles: +2-3 GB per 100 stocks
  - Add 1-minute candles: +5-8 GB per 100 stocks (warning: massive!)

Result: +3-5 GB
Total so far: ~13-20 GB

PHASE 4: ADVANCED FEATURES (Estimated: +3-8 GB)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Step 4a: Generate technical indicators
  Command: python generate_technical_indicators.py
  Indicators (per stock):
    - RSI, MACD, Bollinger Bands, ATR
    - Moving averages (20, 50, 200 day)
    - Volume indicators
  Size: +0.5-1 GB

Step 4b: Add sentiment scores
  Command: python add_sentiment_scores.py
  Data sources:
    - News sentiment (NewsAPI)
    - Social media sentiment (Twitter)
    - Analyst ratings
  Size: +1-2 GB

Step 4c: Add macroeconomic indicators
  Command: python add_macro_indicators.py
  Indicators:
    - Interest rates, inflation, GDP
    - Currency exchange rates
    - Market volatility indices
  Size: +0.5-1 GB

Step 4d: Optional - Add options data
  (Only if needed for advanced trading strategies)
  Size: +2-3 GB

Result: +3-8 GB
FINAL TOTAL: 20-28 GB ✓✓✓

MAINTENANCE & MONITORING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Monitor progress:
  python explore_mega_dataset.py          # See current stats
  python verify_update_yahoo_prices.py    # Update prices daily/weekly
  
Data refresh schedule:
  - Daily stock prices: Update daily (append-only)
  - Weekly technicals: Regenerate weekly
  - Monthly sentiment: Update as needed
  - Quarterly macro data: Update quarterly
"""
    
    print(plan)


if __name__ == "__main__":
    analyze_mega_dataset()
    get_storage_requirements()
    create_implementation_plan()
    
    print("\n" + "="*100)
    print("NEXT STEPS")
    print("="*100)
    print("""
Ready to start building your 20-25 GB dataset?

Immediate actions:
  1. python download_mega_dataset.py           # Start Phase 1
  2. python explore_mega_dataset.py            # Monitor progress
  3. Check expansion_strategies.py for detailed planning
  
Questions answered:
  ✓ How to expand to 20-25 GB?
  ✓ Where to store the data?
  ✓ How long will it take?
  ✓ What are the best strategies?
  ✓ How to maintain and update?
""")
    print("="*100)
