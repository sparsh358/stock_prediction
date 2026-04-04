"""
Model Performance Analyzer
Detailed metrics, benchmarks, and optimization recommendations
"""

import os
import pickle
import json
from pathlib import Path
from datetime import datetime
import numpy as np

def load_metrics(model_name):
    """Load saved metrics for a model"""
    metrics_path = f"models/{model_name}_metrics.pkl"
    
    if not os.path.exists(metrics_path):
        return None
    
    try:
        with open(metrics_path, 'rb') as f:
            return pickle.load(f)
    except:
        return None

def load_features(model_name):
    """Load feature names for a model"""
    features_path = f"models/{model_name}_features.pkl"
    
    if not os.path.exists(features_path):
        return None
    
    try:
        with open(features_path, 'rb') as f:
            return pickle.load(f)
    except:
        return None

def print_header(text):
    """Print formatted header"""
    print("\n" + "="*80)
    print(f"  {text}")
    print("="*80)

def print_section(header, items):
    """Print a section with items"""
    print(f"\n{header}")
    print("-" * 60)
    for label, value in items:
        if isinstance(value, float):
            print(f"  {label}: {value:.6f}")
        else:
            print(f"  {label}: {value}")

def analyze_model(model_name):
    """Analyze a specific model"""
    metrics = load_metrics(model_name)
    features = load_features(model_name)
    
    if not metrics:
        print(f"❌ Model not found: {model_name}")
        return
    
    print_header(f"MODEL ANALYSIS: {model_name}")
    
    # Basic info
    print_section("📊 MODEL INFORMATION", [
        ("Created", metrics.get('created_at', 'Unknown')),
        ("Stocks Used", metrics.get('num_stocks', 'Unknown')),
        ("Total Samples", metrics.get('total_samples', 'Unknown')),
        ("Train Samples", metrics.get('train_samples', 'Unknown')),
        ("Test Samples", metrics.get('test_samples', 'Unknown')),
    ])
    
    # Performance metrics
    metrics_items = [
        ("Train R² Score", metrics.get('train_r2', 0)),
        ("Test R² Score", metrics.get('test_r2', 0)),
        ("Cross-Val R² (mean)", metrics.get('cv_r2_mean', 0)),
        ("Cross-Val R² (std)", metrics.get('cv_r2_std', 0)),
        ("Train RMSE", metrics.get('train_rmse', 0)),
        ("Test RMSE", metrics.get('test_rmse', 0)),
    ]
    print_section("🎯 PERFORMANCE METRICS", metrics_items)
    
    # Interpretation
    test_r2 = metrics.get('test_r2', 0)
    train_r2 = metrics.get('train_r2', 0)
    
    print("\n📈 INTERPRETATION:")
    print(f"  Test R²: {test_r2:.4f}")
    
    if test_r2 > 0.95:
        print(f"  Status: ✅ EXCELLENT (>95% accuracy)")
    elif test_r2 > 0.90:
        print(f"  Status: ✅ GOOD (>90% accuracy)")
    elif test_r2 > 0.75:
        print(f"  Status: ⚠️  FAIR (>75% accuracy)")
    else:
        print(f"  Status: ❌ POOR (<75% accuracy)")
    
    # Overfitting check
    gap = train_r2 - test_r2
    print(f"\n  Overfitting gap: {gap:.4f}")
    if gap > 0.05:
        print(f"  ⚠️  Model may be overfitting (gap > 0.05)")
        print(f"     Recommendation: Add more training data or reduce model complexity")
    else:
        print(f"  ✅ Good generalization (gap < 0.05)")
    
    # Feature importance
    if metrics.get('feature_importance'):
        feature_imp = metrics.get('feature_importance')
        
        print("\n📊 TOP 10 FEATURES BY IMPORTANCE:")
        for i, (name, importance) in enumerate(feature_imp[:10], 1):
            bar_length = int(importance * 50)
            bar = "█" * bar_length
            print(f"  {i:2d}. {name:20s} {importance:6.4f} {bar}")
    
    # Recommendations
    print("\n💡 RECOMMENDATIONS:")
    
    if test_r2 < 0.90:
        print("  1. Scale to more stocks (more training data)")
        print("  2. Add more technical indicators (features_advanced.py)")
        print("  3. Tune XGBoost hyperparameters")
        print("  4. Check data quality for outliers")
    elif test_r2 < 0.95:
        print("  1. Continue scaling to 40+ stocks")
        print("  2. Consider ensemble methods")
    else:
        print("  ✅ Model is performing well!")
        print("  1. Consider deploying to production")
        print("  2. Set up automated daily predictions")
        print("  3. Monitor performance over time")
    
    if gap > 0.05:
        print("  4. Increase training data to reduce overfitting")

def compare_models():
    """Compare all available models"""
    print_header("MODEL COMPARISON")
    
    models_to_check = [
        'multistock_starter_xgb',
        'multistock_high_volume_xgb',
        'multistock_all_xgb',
        'universal_xgb_improved',
        'universal_xgb_model',
    ]
    
    results = []
    
    for model_name in models_to_check:
        metrics = load_metrics(model_name)
        if metrics:
            results.append({
                'name': model_name,
                'stocks': metrics.get('num_stocks', 0),
                'samples': metrics.get('total_samples', 0),
                'train_r2': metrics.get('train_r2', 0),
                'test_r2': metrics.get('test_r2', 0),
                'cv_r2_mean': metrics.get('cv_r2_mean', 0),
            })
    
    if not results:
        print("❌ No trained models found")
        return
    
    # Print comparison table
    print("\n📊 MODEL PERFORMANCE COMPARISON")
    print("-" * 100)
    print(f"{'Model':<35} {'Stocks':<10} {'Samples':<10} {'Train R²':<12} {'Test R²':<12} {'CV R²':<12}")
    print("-" * 100)
    
    for r in results:
        print(f"{r['name']:<35} {r['stocks']:<10} {r['samples']:<10} "
              f"{r['train_r2']:<12.6f} {r['test_r2']:<12.6f} {r['cv_r2_mean']:<12.6f}")
    
    # Find best model
    best = max(results, key=lambda x: x['test_r2'])
    print("\n✅ BEST MODEL: " + best['name'])
    print(f"   Test R²: {best['test_r2']:.6f}")
    print(f"   Stocks: {best['stocks']}")
    print(f"   Samples: {best['samples']}")
    
    # Scaling recommendation
    print("\n📈 SCALING RECOMMENDATIONS:")
    starter = next((r for r in results if 'starter' in r['name']), None)
    all_stocks = next((r for r in results if 'all_xgb' in r['name']), None)
    
    if starter and all_stocks:
        improvement = ((all_stocks['test_r2'] - starter['test_r2']) / starter['test_r2']) * 100
        print(f"  Scaling: starter → all")
        print(f"  Samples: {starter['samples']} → {all_stocks['samples']}")
        print(f"  R² change: {improvement:+.2f}%")
    
    if starter:
        print(f"\n  Current best: {starter['name']}")
        print(f"  Test R²: {starter['test_r2']:.6f}")
        print(f"  Command to scale: python master.py all")

def system_status():
    """Show complete system status"""
    print_header("SYSTEM STATUS")
    
    # Downloaded stocks
    print("\n📊 DOWNLOADED STOCKS:")
    data_dir = Path('data')
    csv_files = list(data_dir.glob('*.csv')) if data_dir.exists() else []
    
    if csv_files:
        total_rows = 0
        for csv_file in sorted(csv_files):
            try:
                with open(csv_file) as f:
                    rows = len(f.readlines()) - 1  # Exclude header
                    total_rows += rows
                    symbol = csv_file.stem.replace('_ohlcv', '')
                    print(f"  ✅ {symbol:<20} ({rows} rows)")
            except:
                pass
        
        print(f"\n  Total: {len(csv_files)} stocks, {total_rows} data points")
    else:
        print("  ❌ No stocks downloaded yet")
        print("  Run: python master.py starter")
    
    # Trained models
    print("\n🤖 TRAINED MODELS:")
    models_dir = Path('models')
    model_files = []
    
    if models_dir.exists():
        for pkl_file in models_dir.glob('*_xgb.pkl'):
            model_name = pkl_file.stem.replace('_xgb', '')
            metrics = load_metrics(model_name)
            if metrics:
                test_r2 = metrics.get('test_r2', 0)
                print(f"  ✅ {model_name:<35} (R²: {test_r2:.6f})")
                model_files.append(model_name)
    
    if not model_files:
        print("  ❌ No models trained yet")
        print("  Run: python master.py starter")
    
    # Predictions
    print("\n📈 PREDICTION RESULTS:")
    results_dir = Path('results')
    result_files = list(results_dir.glob('*.csv')) if results_dir.exists() else []
    
    if result_files:
        for csv_file in sorted(result_files)[-5:]:
            mod_time = datetime.fromtimestamp(csv_file.stat().st_mtime)
            print(f"  ✅ {csv_file.name:<40} ({mod_time.strftime('%Y-%m-%d %H:%M')})")
        
        if len(result_files) > 5:
            print(f"  ... and {len(result_files)-5} more results")
    else:
        print("  ❌ No predictions generated yet")
        print("  Run: python master.py starter")
    
    # Summary
    print("\n" + "="*80)
    print("STATUS SUMMARY")
    print("="*80)
    
    status = {
        'stocks_downloaded': len(csv_files),
        'models_trained': len(model_files),
        'predictions_generated': len(result_files),
    }
    
    if status['stocks_downloaded'] > 0 and status['models_trained'] > 0:
        print("✅ System is operational!")
        print(f"  - {status['stocks_downloaded']} stocks downloaded")
        print(f"  - {status['models_trained']} models trained")
        print(f"  - {status['predictions_generated']} prediction sets generated")
    elif status['stocks_downloaded'] > 0:
        print("⚠️  Data downloaded, models not trained yet")
        print("  Run: python master.py starter")
    else:
        print("❌ System not initialized")
        print("  Run: python master.py starter")

def show_help():
    """Show usage information"""
    print("""
PERFORMANCE ANALYZER - USAGE

python analyze.py compare      # Compare all trained models
python analyze.py status       # Show system status (stocks, models, results)
python analyze.py <model>      # Analyze specific model
                               # Examples: multistock_starter_xgb
                               #           multistock_high_volume_xgb
                               #           multistock_all_xgb
                               #           universal_xgb_improved
                               #           universal_xgb_model

EXAMPLES:

python analyze.py compare
  └─ Shows side-by-side comparison of all models
  └─ Helps decide which model to use
  └─ Shows scaling impact

python analyze.py status
  └─ Complete system status
  └─ Shows downloaded stocks
  └─ Shows trained models
  └─ Shows prediction history

python analyze.py multistock_starter_xgb
  └─ Detailed analysis of starter model
  └─ Performance metrics
  └─ Feature importance ranking
  └─ Overfitting detection
  └─ Optimization recommendations

OUTPUT INCLUDES:

1. Model Information
   - Creation date
   - Number of stocks
   - Sample count (train/test)

2. Performance Metrics
   - R² scores (train/test)
   - Cross-validation scores
   - RMSE values

3. Feature Importance
   - Top 10 most important features
   - Contribution percentage
   - Visual bars

4. Quality Assessment
   - Overfitting detection
   - Generalization quality
   - Next steps for improvement

5. Recommendations
   - Specific actions to improve accuracy
   - Scaling guidance
   - Production readiness
""")

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        show_help()
        sys.exit(0)
    
    command = sys.argv[1]
    
    if command == 'compare':
        compare_models()
    elif command == 'status':
        system_status()
    elif command in ['help', '-h', '--help']:
        show_help()
    else:
        # Treat as model name
        analyze_model(command)
