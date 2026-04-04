"""
Train Big Data Model on 20 Stocks
Scales from 10 stocks (4,220 rows) to 20 stocks (~16,000 rows)
"""

import pandas as pd
import numpy as np
import pickle
import warnings
from sklearn.model_selection import train_test_split, cross_val_score
from xgboost import XGBRegressor
import sys
import os

# Add to path
sys.path.insert(0, os.path.dirname(__file__))

from features_advanced import create_advanced_features, get_feature_list

warnings.filterwarnings('ignore')

# 20 stocks - both original and new
STOCKS_20 = [
    # Original 10
    'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'RELIANCE.NS', 'ICICIBANK.NS',
    'SBIN.NS', 'MARUTI.NS', 'HINDUNILVR.NS', 'AXISBANK.NS', 'HDFCBANK.NS',
    
    # New 10 from different sectors
    'BHARTIARTL.NS', 'DRREDDY.NS', 'SUNPHARMA.NS', 'IOC.NS', 'BPCL.NS',
    'ITC.NS', 'BRITANNIA.NS', 'LT.NS', 'DLF.NS', 'WIPRO.NS'
]

def process_stock_data(symbol):
    """Load and process a single stock"""
    csv_file = f'data/{symbol.replace(".", "_")}_ohlcv.csv'
    
    if not os.path.exists(csv_file):
        print(f"⚠️  {symbol}: File not found, skipping")
        return None
    
    try:
        df = pd.read_csv(csv_file, index_col=0)
        
        # Create features
        df = create_advanced_features(df)
        df.dropna(inplace=True)
        
        if len(df) == 0:
            print(f"⚠️  {symbol}: No valid features, skipping")
            return None
            
        return df
    except Exception as e:
        print(f"⚠️  {symbol}: Error processing ({str(e)[:30]}), skipping")
        return None

def train_big_data_model(stocks=None, test_size=0.2):
    """Train model on multiple stocks - BIG DATA approach"""
    
    if stocks is None:
        stocks = STOCKS_20
    
    print('\n' + '='*80)
    print('BIG DATA MODEL TRAINING')
    print('='*80)
    print(f'Target: {len(stocks)} stocks')
    print()
    
    # Load and combine all stocks
    print('[1/4] Loading stock data...')
    all_data = []
    successful = 0
    
    for i, symbol in enumerate(stocks, 1):
        print(f'  [{i}/{len(stocks)}] Processing {symbol}...', end=' ', flush=True)
        df = process_stock_data(symbol)
        
        if df is not None:
            all_data.append(df)
            successful += 1
            print(f'✅ ({len(df)} rows)')
        else:
            print('❌')
    
    if not all_data:
        print("❌ No stocks loaded!")
        return None
    
    print(f'\n✅ Loaded {successful} stocks')
    
    # Combine all stocks
    print(f'\n[2/4] Combining {len(all_data)} stocks...')
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.dropna(inplace=True)
    
    print(f'  Total rows: {len(combined_df):,}')
    print(f'  Total size: {combined_df.memory_usage(deep=True).sum() / 1024:.1f} KB')
    
    # Get features
    feature_cols = get_feature_list()
    X = combined_df[feature_cols].fillna(0)
    y = combined_df['target'].fillna(0)
    
    print(f'  Features: {len(feature_cols)}')
    print(f'  Samples: {len(X):,}')
    
    # Split data
    print(f'\n[3/4] Splitting data ({100-int(test_size*100)}/{int(test_size*100)} train/test)...')
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
    
    print(f'  Train: {len(X_train):,}')
    print(f'  Test:  {len(X_test):,}')
    
    # Train model
    print(f'\n[4/4] Training XGBoost model...')
    model = XGBRegressor(
        n_estimators=100,
        learning_rate=0.1,
        max_depth=6,
        random_state=42,
        verbosity=0
    )
    
    model.fit(X_train, y_train, verbose=False)
    
    # Evaluate
    train_r2 = model.score(X_train, y_train)
    test_r2 = model.score(X_test, y_test)
    
    # Cross validation
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
    
    print()
    print('='*80)
    print('MODEL PERFORMANCE')
    print('='*80)
    print(f'Train R²: {train_r2:.6f}')
    print(f'Test R²:  {test_r2:.6f} ✅')
    print(f'Cross-Val R²: {cv_scores.mean():.6f} ±{cv_scores.std():.6f}')
    
    # Feature importance
    feature_importance = sorted(
        zip(feature_cols, model.feature_importances_),
        key=lambda x: x[1],
        reverse=True
    )
    
    print('\nTop 10 Features:')
    for i, (feat, imp) in enumerate(feature_importance[:10], 1):
        print(f'  {i:2d}. {feat:20s} {imp:6.4f}')
    
    # Save model
    print('\n[Final] Saving model...')
    
    model_name = f'multistock_20stocks_xgb'
    model_path = f'models/{model_name}.pkl'
    features_path = f'models/{model_name}_features.pkl'
    metrics_path = f'models/{model_name}_metrics.pkl'
    
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    
    with open(features_path, 'wb') as f:
        pickle.dump(feature_cols, f)
    
    metrics = {
        'num_stocks': len(all_data),
        'total_samples': len(combined_df),
        'train_samples': len(X_train),
        'test_samples': len(X_test),
        'train_r2': train_r2,
        'test_r2': test_r2,
        'cv_r2_mean': cv_scores.mean(),
        'cv_r2_std': cv_scores.std(),
        'feature_importance': feature_importance,
        'created_at': pd.Timestamp.now().isoformat()
    }
    
    with open(metrics_path, 'wb') as f:
        pickle.dump(metrics, f)
    
    print(f'  ✅ Model saved: {model_path}')
    print(f'  ✅ Features saved: {features_path}')
    print(f'  ✅ Metrics saved: {metrics_path}')
    
    print('\n' + '='*80)
    print('BIG DATA TRAINING COMPLETE!')
    print('='*80)
    print(f'✅ Model: {model_name}')
    print(f'✅ Stocks: {len(all_data)}')
    print(f'✅ Data Points: {len(combined_df):,}')
    print(f'✅ Accuracy: {test_r2*100:.2f}%')
    print()
    
    return model, feature_cols, metrics

if __name__ == '__main__':
    import sys
    
    # Allow custom stock list
    if len(sys.argv) > 1:
        # Custom stocks: python script.py STOCK1 STOCK2 ...
        custom_stocks = sys.argv[1:]
        print(f"Using custom stocks: {custom_stocks}")
        train_big_data_model(stocks=custom_stocks)
    else:
        # Default: 20 stocks
        train_big_data_model(stocks=STOCKS_20)
