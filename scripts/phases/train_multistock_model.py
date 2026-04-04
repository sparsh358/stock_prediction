"""
Train Universal Model on ALL Indian Stocks
Creates a large-scale predictive model with thousands of data points
"""

import os
import pandas as pd
import numpy as np
import pickle
import warnings
from sklearn.model_selection import train_test_split, cross_val_score
from xgboost import XGBRegressor
import yfinance as yf
from features_advanced import create_advanced_features, get_feature_list
from indian_stocks_config import INDIAN_STOCKS, STARTER_STOCKS, HIGH_VOLUME_STOCKS
import time

warnings.filterwarnings('ignore')

def train_multistock_model(stocks=None, model_name='multistock', test_size=0.2):
    """
    Train a universal model on multiple stocks
    Creates a much more robust model with more diverse training data
    
    Args:
        stocks: List of stock symbols (default: STARTER_STOCKS)
        model_name: Name for saving model
        test_size: Train/test split ratio
    """
    
    if stocks is None:
        stocks = STARTER_STOCKS
    
    print("="*80)
    print(f"MULTI-STOCK UNIVERSAL MODEL TRAINING")
    print(f"Stocks: {len(stocks)} | Model Name: {model_name}")
    print("="*80)
    
    all_data = []
    download_time = 0
    feature_time = 0
    
    # Download and process data
    print(f"\n[1/4] Downloading and processing {len(stocks)} stocks...")
    
    for i, symbol in enumerate(stocks, 1):
        try:
            start = time.time()
            print(f"  [{i}/{len(stocks)}] {symbol}...", end=" ", flush=True)
            
            # Try to load from CSV first
            csv_file = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
            if os.path.exists(csv_file):
                data = pd.read_csv(csv_file)
                print(f"(cached) ", end="", flush=True)
            else:
                # Download fresh
                data = yf.download(symbol, start='2023-01-01', end='2026-04-01', progress=False)
                data = data.reset_index()
                # Save for future use
                os.makedirs('data', exist_ok=True)
                data.to_csv(csv_file, index=False)
            
            if data.empty:
                print(f"NO DATA")
                continue
            
            download_time += time.time() - start
            
            # Create advanced features
            start = time.time()
            features_df = create_advanced_features(data)
            features_df['symbol'] = symbol
            all_data.append(features_df)
            feature_time += time.time() - start
            
            print(f"OK ({len(features_df)} rows)")
        
        except Exception as e:
            print(f"ERROR: {str(e)[:40]}")
    
    if not all_data:
        print("No data downloaded!")
        return None
    
    # Combine all data
    print(f"\n[2/4] Combining data...")
    df_combined = pd.concat(all_data, ignore_index=True)
    print(f"  Total combined rows: {len(df_combined):,}")
    print(f"  Total combined size: {len(df_combined) * 20 / 1e6:.1f} MB")
    
    # Prepare features
    feature_cols = get_feature_list()
    X = df_combined[feature_cols].fillna(0)
    y = df_combined['target']
    
    print(f"  Features: {len(feature_cols)}")
    print(f"  Target range: {y.min():.4f} to {y.max():.4f}")
    
    # Split data
    print(f"\n[3/4] Splitting data (80% train, 20% test)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=42
    )
    print(f"  Train: {len(X_train):,} | Test: {len(X_test):,}")
    
    # Train model
    print(f"\n[4/4] Training XGBoost on {len(X_train):,} samples...")
    model = XGBRegressor(
        n_estimators=300,
        max_depth=10,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective='reg:pseudohubererror',
        random_state=42,
        n_jobs=-1,
        verbosity=0
    )
    
    model.fit(X_train, y_train)
    print("  Training complete!")
    
    # Evaluate
    print(f"\n{'='*80}")
    print("RESULTS")
    print(f"{'='*80}")
    
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='r2')
    
    print(f"Train R² Score:        {train_score:.4f}")
    print(f"Test R² Score:         {test_score:.4f}")
    print(f"Cross-Val R² (mean):   {cv_scores.mean():.4f} (±{cv_scores.std():.4f})")
    
    # Feature importance
    print(f"\n{'='*80}")
    print("TOP 15 IMPORTANT FEATURES")
    print(f"{'='*80}")
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for i, (idx, row) in enumerate(feature_importance.head(15).iterrows(), 1):
        print(f"{i:2}. {row['feature']:20} : {row['importance']:.4f}")
    
    # Save model
    print(f"\n{'='*80}")
    model_path = f"models/{model_name}_xgb.pkl"
    os.makedirs('models', exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"Model saved: {model_path}")
    
    with open(f"models/{model_name}_features.pkl", "wb") as f:
        pickle.dump(feature_cols, f)
    
    # Performance metrics
    metrics = {
        'train_r2': train_score,
        'test_r2': test_score,
        'cv_r2_mean': cv_scores.mean(),
        'cv_r2_std': cv_scores.std(),
        'total_samples': len(df_combined),
        'stocks': len(stocks),
        'features': len(feature_cols),
    }
    
    with open(f"models/{model_name}_metrics.pkl", "wb") as f:
        pickle.dump(metrics, f)
    
    print(f"Metrics saved: models/{model_name}_metrics.pkl")
    print(f"\nMetrics:")
    for k, v in metrics.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.4f}")
        else:
            print(f"  {k}: {v}")
    
    return model, feature_cols, metrics


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'starter':
            stocks = STARTER_STOCKS
            name = 'multistock_starter'
        elif sys.argv[1] == 'high-volume':
            stocks = HIGH_VOLUME_STOCKS
            name = 'multistock_high_volume'
        elif sys.argv[1] == 'all':
            stocks = list(INDIAN_STOCKS.keys())
            name = 'multistock_all'
        else:
            # Custom list
            stocks = sys.argv[1].split(',')
            name = f"multistock_custom_{len(stocks)}"
    else:
        stocks = STARTER_STOCKS
        name = 'multistock_starter'
    
    print(f"\nTraining on {len(stocks)} stocks...")
    print(f"Model: {name}\n")
    
    train_multistock_model(stocks, model_name=name)
