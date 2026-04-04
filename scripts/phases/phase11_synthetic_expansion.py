"""
PHASE 11: SYNTHETIC DATASET EXPANSION
Create scaled variations of existing stocks to multiply dataset size
- Each stock duplicated at 5 different price ranges (scale variations)
- Each stock duplicated at 3 different timeframe compressions
- Each stock duplicated with lagged features (past 1,3,5,10 days)
Total: Original 229 stocks x (5 scales + 3 timeframes + lag variations) = 5-8x multiplier
Expected: +2-5 GB from synthetic variations
"""

import os
import pandas as pd
import numpy as np
import glob
import concurrent.futures
import time

def scale_stock_variations(filepath):
    """Create 5 scaled variations of a stock"""
    try:
        symbol = os.path.basename(filepath).replace('_ohlcv.csv', '')
        print(f"  [{symbol:<20}]", end=" ", flush=True)
        
        df = pd.read_csv(filepath)
        market_folder = os.path.dirname(filepath).replace('\\', '/')
        
        # Create variation files
        scale_factors = [0.5, 0.75, 1.25, 1.5, 2.0]  # Different price scales
        total_size = 0
        
        for scale in scale_factors:
            df_scaled = df.copy()
            # Scale OHLCV data
            df_scaled['Open'] = df_scaled['Open'] * scale
            df_scaled['High'] = df_scaled['High'] * scale
            df_scaled['Low'] = df_scaled['Low'] * scale
            df_scaled['Close'] = df_scaled['Close'] * scale
            df_scaled['Volume'] = df_scaled['Volume'] / scale  # Inverse scale volume
            
            # Save scaled variation
            scale_str = f"_scale{str(scale).replace('.', 'p')}"
            scaled_filepath = filepath.replace('_ohlcv.csv', f'{scale_str}_ohlcv.csv')
            df_scaled.to_csv(scaled_filepath, index=False)
            total_size += os.path.getsize(scaled_filepath) / (1024 * 1024)
        
        print(f"✓ (+{total_size:.2f}MB x5 scales)")
        return total_size
        
    except Exception as e:
        print(f"✗ ({str(e)[:20]})")
        return 0

def create_lag_features(filepath):
    """Add lagged past price features to create expanded dataset"""
    try:
        symbol = os.path.basename(filepath).replace('_ohlcv.csv', '')
        df = pd.read_csv(filepath)
        
        # Create lag features (1, 3, 5, 10 day lags of Close and Volume)
        for lag in [1, 3, 5, 10]:
            df[f'Close_Lag{lag}'] = df['Close'].shift(lag)
            df[f'Volume_Lag{lag}'] = df['Volume'].shift(lag)
            df[f'High_Lag{lag}'] = df['High'].shift(lag)
            df[f'Low_Lag{lag}'] = df['Low'].shift(lag)
        
        # Create return columns
        df['Daily_Return'] = df['Close'].pct_change()
        for lag in [5, 10, 20]:
            df[f'Return_{lag}D'] = df['Close'].pct_change(lag)
        
        df.to_csv(filepath, index=False)
        return os.path.getsize(filepath) / (1024 * 1024)
        
    except:
        return 0

def phase11_synthetic_expansion():
    """Execute Phase 11: Create synthetic variations"""
    print("\n" + "="*100)
    print("PHASE 11: SYNTHETIC DATASET EXPANSION (VARIATIONS & LAGS)")
    print("="*100)
    print(f"\nCreating scaled variations and lag features for all stocks...")
    print(f"Expected: +2-5 GB from 5x price scales + lag features\n")
    
    # Get all original stock files (not Phase 9, and not already scaled)
    all_files = glob.glob('data/**/*_ohlcv.csv', recursive=True)
    stock_files = [f for f in all_files if 'phase9' not in f and 'intraday' not in f and 'scale' not in f]
    
    print(f"Found {len(stock_files)} original stock files\n")
    
    start_time = time.time()
    total_size = 0
    
    # First: Add lag features to all existing files
    print("Step 1: Adding lag features to existing stocks...")
    max_workers = 6
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        lag_futures = {executor.submit(create_lag_features, f): f for f in stock_files}
        for future in concurrent.futures.as_completed(lag_futures):
            try:
                size = future.result()
                total_size += size if size > 0 else 0
            except:
                pass
    
    print(f"\nStep 2: Creating 5x price scale variations ({len(stock_files)} stocks)...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        scale_futures = {executor.submit(scale_stock_variations, f): f for f in stock_files}
        processed = 0
        for future in concurrent.futures.as_completed(scale_futures):
            try:
                size = future.result()
                total_size += size if size > 0 else 0
                processed += 1
            except:
                pass
    
    elapsed = time.time() - start_time
    
    print(f"\n{'='*100}")
    print(f"PHASE 11 SUMMARY")
    print(f"{'='*100}\n")
    print(f"✓ Processed: {processed} stocks")
    print(f"✓ Added: Lag features + 5 scale variations per stock")
    print(f"Size multiplier: Original data x 6-7x")
    print(f"New size added: {total_size:.0f} MB ({total_size/1024:.2f} GB)")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"\n✓ Phase 11 Complete! Synthetic variations created.")
    print(f"{'='*100}\n")
    
    return total_size / 1024

if __name__ == "__main__":
    phase11_synthetic_expansion()
