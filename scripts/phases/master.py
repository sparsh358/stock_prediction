"""
Master Script - Automated Big Data Pipeline
One-command scaling from 10 to 100+ stocks
"""

import os
import sys
import time
import subprocess
from indian_stocks_config import STARTER_STOCKS, HIGH_VOLUME_STOCKS, INDIAN_STOCKS

def run_command(cmd, description=""):
    """Run a command and report status"""
    if description:
        print(f"\n{'='*80}")
        print(f"{description}")
        print(f"{'='*80}\n")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=False)
        return result.returncode == 0
    except Exception as e:
        print(f"Error: {e}")
        return False

def pipeline_starter():
    """Run full pipeline for starter set (10 stocks)"""
    print("\n" + "="*80)
    print("STOCK PREDICTION PIPELINE - STARTER SET (10 STOCKS)")
    print("="*80)
    
    # Step 1: Download data
    run_command(
        "python bulk_download.py",
        "STEP 1/3: Download Data (10 stocks)"
    )
    
    # Step 2: Train model
    run_command(
        "python train_multistock_model.py starter",
        "STEP 2/3: Train Model (4,220 data points)"
    )
    
    # Step 3: Generate predictions
    run_command(
        "python batch_predict.py portfolio",
        "STEP 3/3: Generate Portfolio Predictions"
    )
    
    print("\n" + "="*80)
    print("STARTER PIPELINE COMPLETE!")
    print("="*80)
    print("✅ Model: models/multistock_starter_xgb.pkl")
    print("✅ Results: results/portfolio_summary_*.csv")
    print("✅ Next: Run 'python master.py high-volume' for top 10 stocks")

def pipeline_high_volume():
    """Run pipeline for high-volume stocks (top 10)"""
    print("\n" + "="*80)
    print("STOCK PREDICTION PIPELINE - HIGH VOLUME STOCKS")
    print("="*80)
    
    run_command(
        "python bulk_download.py high-volume",
        "STEP 1/3: Download High-Volume Stocks"
    )
    
    run_command(
        "python train_multistock_model.py high-volume",
        "STEP 2/3: Train High-Volume Model"
    )
    
    run_command(
        "python batch_predict.py portfolio",
        "STEP 3/3: Generate Predictions"
    )
    
    print("\n" + "="*80)
    print("HIGH-VOLUME PIPELINE COMPLETE!")
    print("✅ Model: models/multistock_high_volume_xgb.pkl")
    print("✅ Next: Run 'python master.py all' for 40+ stocks")

def pipeline_all():
    """Run pipeline for ALL stocks (40+)"""
    print("\n" + "="*80)
    print("STOCK PREDICTION PIPELINE - ALL STOCKS (40+)")
    print("="*80)
    print(f"This will download {len(INDIAN_STOCKS)} stocks and train a universal model")
    print("Estimated time: 15-30 minutes\n")
    
    response = input("Continue? (y/n): ").lower()
    if response != 'y':
        print("Cancelled.")
        return
    
    run_command(
        "python bulk_download.py all",
        "STEP 1/3: Download All Stocks (40+)"
    )
    
    run_command(
        "python train_multistock_model.py all",
        "STEP 2/3: Train Universal Model (16,000+ data points)"
    )
    
    run_command(
        "python batch_predict.py portfolio",
        "STEP 3/3: Generate Portfolio Analysis"
    )
    
    print("\n" + "="*80)
    print("COMPLETE PIPELINE FINISHED!")
    print("="*80)
    print("✅ Model: models/multistock_all_xgb.pkl")
    print("✅ Stocks: 40+")
    print("✅ Data Points: 16,000+")
    print("✅ Results: results/portfolio_summary_*.csv")

def pipeline_custom(stocks_str):
    """Run pipeline for custom stock list"""
    stocks = stocks_str.split(',')
    print("\n" + "="*80)
    print(f"CUSTOM PIPELINE - {len(stocks)} STOCKS")
    print("="*80)
    
    print(f"Stocks: {', '.join(stocks)}\n")
    
    # Download each stock
    for symbol in stocks:
        run_command(
            f"python bulk_download.py {symbol.strip()}",
            f"Downloading {symbol.strip()}"
        )
    
    # Train model
    stocks_arg = ','.join(s.strip() for s in stocks)
    run_command(
        f"python train_multistock_model.py {stocks_arg}",
        f"Training Model on {len(stocks)} Stocks"
    )
    
    # Predictions
    run_command(
        "python batch_predict.py portfolio",
        "Generating Predictions"
    )
    
    print("\n" + "="*80)
    print("CUSTOM PIPELINE COMPLETE!")
    print("="*80)

def show_status():
    """Show current system status"""
    print("\n" + "="*80)
    print("SYSTEM STATUS")
    print("="*80)
    
    # Check downloaded data
    print("\n📊 DOWNLOADED STOCKS:")
    count = 0
    for symbol in STARTER_STOCKS:
        csv_file = f"data/{symbol.replace('.', '_')}_ohlcv.csv"
        if os.path.exists(csv_file):
            size = os.path.getsize(csv_file)
            print(f"  ✅ {symbol} ({size/1024:.1f} KB)")
            count += 1
        else:
            print(f"  ❌ {symbol} (not downloaded)")
    
    print(f"\nTotal: {count}/{len(STARTER_STOCKS)} stocks downloaded")
    
    # Check models
    print("\n🤖 TRAINED MODELS:")
    models = [
        ('starter', 'Starter (10 stocks)'),
        ('high_volume', 'High Volume (10 stocks)'),
        ('all', 'All Stocks (40+)'),
    ]
    
    for model_id, label in models:
        model_path = f"models/multistock_{model_id}_xgb.pkl"
        if os.path.exists(model_path):
            print(f"  ✅ {label}")
        else:
            print(f"  ❌ {label}")
    
    # Check results
    print("\n📈 PREDICTION RESULTS:")
    results_dir = 'results'
    if os.path.exists(results_dir):
        files = os.listdir(results_dir)
        for f in sorted(files)[-5:]:
            print(f"  ✅ {f}")
        if len(files) > 5:
            print(f"  ... and {len(files)-5} more")
    else:
        print("  ❌ No predictions generated yet")

def show_usage():
    """Show usage information"""
    print("\n" + "="*80)
    print("MASTER PIPELINE - USAGE")
    print("="*80)
    print("""
python master.py starter              # Run pipeline for starter set (10 stocks)
python master.py high-volume          # Run pipeline for high-volume stocks
python master.py all                  # Run pipeline for ALL stocks (40+)
python master.py custom:TCS.NS,INFY.NS  # Custom stock list (comma-separated)
python master.py status               # Show current status
python master.py help                 # Show this help message

WHAT EACH DOES:

1. starter (10 stocks = 4,220 data points)
   - Downloads 10 benchmark stocks
   - Trains universal model
   - Generates portfolio predictions
   - Time: 5-10 minutes
   - Best for: Starting out

2. high-volume (10 stocks = 4,220 data points)
   - Downloads most actively traded stocks
   - More diverse model
   - Better for real trading
   - Time: 5-10 minutes
   - Best for: Proven stocks

3. all (40+ stocks = 16,000+ data points)
   - Downloads ALL available stocks
   - Comprehensive coverage
   - All sectors represented
   - Time: 15-30 minutes
   - Best for: Production

4. custom (Your choice)
   - Download specific stocks
   - Create targeted models
   - Sector-specific analysis
   - Time: Varies
   - Best for: Research

5. status
   - Shows what's downloaded
   - Shows what's trained
   - Shows prediction results
   - Time: 1 second
   - Use to: Check progress
""")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        show_usage()
        sys.exit(0)
    
    command = sys.argv[1]
    
    if command == 'starter':
        pipeline_starter()
    
    elif command == 'high-volume':
        pipeline_high_volume()
    
    elif command == 'all':
        pipeline_all()
    
    elif command.startswith('custom:'):
        stocks_str = command.replace('custom:', '')
        pipeline_custom(stocks_str)
    
    elif command == 'status':
        show_status()
    
    elif command in ['help', '-h', '--help']:
        show_usage()
    
    else:
        print(f"Unknown command: {command}")
        show_usage()
