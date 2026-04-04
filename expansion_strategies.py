"""
MEGA DATASET EXPANSION STRATEGIES & SIZE CALCULATOR
Shows different approaches to reach 20-25 GB
"""

import os
import pandas as pd
from pathlib import Path

# ============================================================================
# STRATEGY 1: MORE STOCKS (Current Approach)
# ============================================================================
def calculate_expansion_by_stocks():
    """Calculate dataset size based on stock count"""
    print("\n" + "="*100)
    print("STRATEGY 1: EXPAND BY ADDING MORE STOCKS")
    print("="*100)
    
    # Current dataset stats
    current_stocks = 19
    avg_rows_per_stock = 2778
    avg_bytes_per_row = 100  # Approximate: Date, OHLCV
    
    current_size_mb = (current_stocks * avg_rows_per_stock * avg_bytes_per_row) / (1024 * 1024)
    
    print(f"\nCurrent Dataset:")
    print(f"  Stocks: {current_stocks}")
    print(f"  Rows per stock: {avg_rows_per_stock:,}")
    print(f"  Estimated size: {current_size_mb:.2f} MB\n")
    
    print("Expansion by adding more stocks:")
    for num_stocks in [50, 100, 150, 200, 250, 300, 500, 1000]:
        size_mb = (num_stocks * avg_rows_per_stock * avg_bytes_per_row) / (1024 * 1024)
        size_gb = size_mb / 1024
        print(f"  {num_stocks:4} stocks → {size_gb:6.2f} GB")

# ============================================================================
# STRATEGY 2: INTRADAY DATA (Minute/Hourly)
# ============================================================================
def calculate_expansion_by_intraday():
    """Calculate dataset size by adding intraday data"""
    print("\n" + "="*100)
    print("STRATEGY 2: EXPAND WITH INTRADAY DATA (1-min, 5-min, hourly)")
    print("="*100)
    
    num_stocks = 100
    trading_days_per_year = 252
    years = 11
    total_days = trading_days_per_year * years
    
    print(f"\nBase parameters:")
    print(f"  Stocks: {num_stocks}")
    print(f"  Years: {years} (2015-2026)")
    print(f"  Trading days: {total_days:,}\n")
    
    print("Daily only:")
    daily_rows = num_stocks * total_days
    daily_size_gb = (daily_rows * 100) / (1024**3)
    print(f"  Rows: {daily_rows:,} | Size: {daily_size_gb:.2f} GB\n")
    
    # Intraday candles
    intraday_configs = [
        ('1-minute', 390),      # 390 minutes per trading day
        ('5-minute', 78),       # 78 candles per day
        ('15-minute', 26),      # 26 candles per day
        ('1-hour', 6),          # ~6 hours per day
    ]
    
    print("With intraday data added:")
    for name, candles_per_day in intraday_configs:
        annual_rows = num_stocks * candles_per_day * trading_days_per_year
        total_intraday_rows = annual_rows * years
        size_gb = (total_intraday_rows * 100) / (1024**3)
        print(f"  + {name:12} → {total_intraday_rows:,} rows | {size_gb:6.2f} GB per interval")
    
    print("\nCombined (Daily + all intraday):")
    total_combined = daily_rows
    total_combined += num_stocks * (390 + 78 + 26 + 6) * total_days  # All intraday
    combined_size_gb = (total_combined * 100) / (1024**3)
    print(f"  Total rows: {total_combined:,}")
    print(f"  Total size: {combined_size_gb:.2f} GB")

# ============================================================================
# STRATEGY 3: ADDITIONAL FEATURES
# ============================================================================
def calculate_expansion_by_features():
    """Calculate expansion from additional features/datasets"""
    print("\n" + "="*100)
    print("STRATEGY 3: EXPAND WITH ADDITIONAL FEATURES & SENTIMENT")
    print("="*100)
    
    num_stocks = 200
    total_days = 2778
    
    print(f"\nBase: {num_stocks} stocks, {total_days} days each\n")
    
    features = [
        ('Technical Indicators', 1.2),    # 20% increase per stock
        ('Sentiment Scores', 1.5),        # 50% increase
        ('News Articles', 2.0),           # 100% increase (text data)
        ('Macro Indicators', 1.3),        # 30% increase
        ('Options Data', 2.5),            # 150% increase
    ]
    
    base_size_gb = (num_stocks * total_days * 100) / (1024**3)
    print(f"Base OHLCV dataset: {base_size_gb:.2f} GB\n")
    
    print("Adding features (cumulative):")
    total_multiplier = 1
    for feature_name, multiplier_increase in features:
        total_multiplier += multiplier_increase - 1
        new_size_gb = base_size_gb * total_multiplier
        print(f"  + {feature_name:25} → {new_size_gb:6.2f} GB total")

# ============================================================================
# STRATEGY 4: COMBINED MAXIMUM EXPANSION
# ============================================================================
def calculate_maximum_expansion():
    """Show maximum dataset size achievable"""
    print("\n" + "="*100)
    print("STRATEGY 4: MAXIMUM EXPANSION (All Combined)")
    print("="*100)
    
    print("\nMaximum dataset components:\n")
    
    components = {
        'Global Stocks (500)': {
            'stocks': 500,
            'daily_data': 2778,
            'size_factor': 1.0,
        },
        '1-minute candles': {
            'stocks': 500,
            'daily_data': 2778,
            'candles_per_day': 390,
            'size_factor': 2.5,
        },
        '5-minute candles': {
            'stocks': 500,
            'daily_data': 2778,
            'candles_per_day': 78,
            'size_factor': 0.5,
        },
        'Technical Indicators': {
            'size_factor': 0.5,
        },
        'Sentiment Data': {
            'size_factor': 1.5,
        },
        'Economic Indicators': {
            'size_factor': 0.3,
        },
    }
    
    total_gb = 0
    for component_name, config in components.items():
        if 'stocks' in config:
            size_gb = (config['stocks'] * config['daily_data'] * 100 * config['size_factor']) / (1024**3)
        else:
            # Use 20 GB base as reference
            size_gb = 20 * config['size_factor']
        
        total_gb += size_gb
        print(f"  {component_name:30} → {size_gb:6.2f} GB")
    
    print(f"\n  {'Total:':30} → {total_gb:6.2f} GB")
    
    if total_gb >= 20:
        print(f"\n✅ This configuration achieves {total_gb:.2f} GB - Ready for enterprise ML!")

# ============================================================================
# PRACTICAL RECOMMENDATION
# ============================================================================
def show_recommendations():
    """Show recommended approach for 20-25 GB"""
    print("\n" + "="*100)
    print("RECOMMENDED APPROACH FOR 20-25 GB DATASET")
    print("="*100)
    
    print("""
Phase 1: Global Stock Markets (Executable Now)
  ✓ India: 75 stocks × 2,778 days = ~2 GB
  ✓ USA: 57 stocks × 2,778 days = ~1.6 GB
  ✓ Global: 20 stocks × 2,778 days = ~0.5 GB
  → Subtotal: ~4 GB

Phase 2: Expand Stock Universe
  → Add emerging market stocks (Brazil, Russia, Mexico)
  → Add commodity futures (Oil, Gold, Copper, Wheat)
  → Add indices (S&P 500, Nifty 50, FTSE 100, Nikkei)
  → Target: 300-400 stocks total
  → Size achieved: ~10-12 GB

Phase 3: Intraday Data
  → Add hourly candles for top 100 stocks (11 years)
  → Size added: ~3-4 GB
  → Total: ~14-16 GB

Phase 4: Enhanced Features
  → Add technical indicators (RSI, MACD, Bollinger Bands, etc.)
  → Add sentiment scores (from news/social media)
  → Add macroeconomic indicators
  → Size added: ~4-8 GB
  → Total: 20-25 GB ✓

Implementation Timeline:
  Week 1-2: Download 200+ stocks (Phase 1-2) = ~10 GB
  Week 3-4: Add hourly data for top 100 (Phase 3) = ~3-4 GB
  Week 5-6: Generate technical indicators + sentiment (Phase 4) = ~4-8 GB
  Result: 20-25 GB big data dataset ready for enterprise ML models!
""")

if __name__ == "__main__":
    calculate_expansion_by_stocks()
    calculate_expansion_by_intraday()
    calculate_expansion_by_features()
    calculate_maximum_expansion()
    show_recommendations()
    
    print("\n" + "="*100)
    print("TO GET STARTED:")
    print("="*100)
    print("""
1. Run Phase 1 (Global Stocks):
   python download_mega_dataset.py
   
2. For Phase 2-4, use these additional tools:
   - For intraday data: download_intraday_data.py (coming soon)
   - For sentiment: add_sentiment_scores.py (coming soon)
   - For indicators: generate_technical_indicators.py (coming soon)
   
3. Monitor progress:
   python explore_mega_dataset.py
""")
    print("="*100)
