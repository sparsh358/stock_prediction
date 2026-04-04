"""
Quick Reference - 20-Stock Big Data System
Commands to use and scale further
"""

import os

print("""
╔═══════════════════════════════════════════════════════════════════════════╗
║                   🚀 20-STOCK BIG DATA SYSTEM                            ║
║                      Quick Command Reference                              ║
╚═══════════════════════════════════════════════════════════════════════════╝

📊 CURRENT STATUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ✅ Downloaded Stocks:  20 (from multiple sectors)
  ✅ Data Points:        8,421 rows (~3 years per stock)
  ✅ Model Trained:      72.39% accuracy (R²)
  ✅ Predictions:        19 stocks analyzed
  ✅ Portfolio Result:   +0.38% avg return (52.6% bullish)


🎯 MAIN COMMANDS - USE THESE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 🔄 RE-TRAIN BIG DATA MODEL
   
   python train_20stocks_model.py
   
   └─ Retrains model on all 20 stocks
   └─ Use when: Adding new data, improving accuracy
   └─ Time: ~2-3 minutes
   └─ Output: Updated model pickle files


2. 📈 PREDICT FOR ALL 20 STOCKS
   
   python predict_20stocks.py
   
   └─ Generates predictions for all 20 stocks
   └─ Shows sentiment analysis
   └─ Identifies bullish/bearish opportunities
   └─ Time: ~60 seconds
   └─ Output: CSV file with all predictions


3. 🔍 ANALYZE MODEL PERFORMANCE
   
   python analyze.py compare
   
   └─ Compare all trained models
   └─ Shows: Starter vs Big Data performance
   └─ Helps understand improvements
   └─ Time: ~1 second


4. 📥 DOWNLOAD MORE STOCKS (Optional)
   
   python download_more_stocks.py
   
   └─ Downloads 10 more stocks from new sectors
   └─ Can run multiple times to add more
   └─ Time: ~60 seconds for 10 stocks


5. ⬆️  SCALE TO 40+ STOCKS
   
   python bulk_download.py all
   python train_multistock_model.py all
   python batch_predict.py portfolio
   
   └─ Downloads 40+ stocks
   └─ Trains on massive dataset (16,000+)
   └─ Full enterprise coverage
   └─ Time: 15-30 minutes total


🎓 UNDERSTANDING THE MODELS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Available Models:

  1. multistock_starter_xgb.pkl (10 stocks)
     ├─ Accuracy: 99.77% R²
     ├─ Data: 4,220 rows
     ├─ Pros: Very accurate on starter set
     ├─ Cons: May overfit, limited coverage
     └─ Use: Quick testing, limited portfolio

  2. multistock_20stocks_xgb.pkl (20 stocks) ⭐ CURRENT
     ├─ Accuracy: 72.39% R²
     ├─ Data: 8,421 rows
     ├─ Pros: Good generalization, diverse sectors
     ├─ Cons: Accuracy drops (expected & healthy)
     └─ Use: Production predictions, 20-stock portfolio

  3. multistock_all_xgb.pkl (40+ stocks) - Ready to train
     ├─ Accuracy: ~65-70% R² (estimated)
     ├─ Data: 16,000+ rows
     ├─ Pros: Best generalization, enterprise ready
     ├─ Cons: Lower accuracy (but most reliable)
     └─ Use: Full market coverage, production


📊 SECTOR COVERAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Your 20 Stocks Span These Sectors:

  IT (3):              TCS, INFY, WIPRO
  Banking (3):         ICICIBANK, SBIN, AXISBANK
  FMCG (3):           HINDUNILVR, ITC, BRITANNIA
  Pharma (2):         DRREDDY, SUNPHARMA
  Energy (2):         IOC, BPCL
  Auto (1):           MARUTI
  Telecom (1):        BHARTIARTL
  Infrastructure (1): LT
  Real Estate (1):    DLF

  Total: 9 sectors covered


📈 LATEST PREDICTIONS (20 Stocks)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

High-Confidence BULLISH (Technical + Sentiment Aligned):
  ✅ TCS.NS           +0.40%  (Sentiment: BULLISH)
  ✅ WIPRO.NS         +1.17%  (Sentiment: BULLISH)
  ✅ HINDUNILVR.NS    +0.62%  (Sentiment: BULLISH)

Strong BULLISH Signal:
  ✅ SBIN.NS          +1.17%
  ✅ MARUTI.NS        +1.17%
  ✅ AXISBANK.NS      +1.17%

Portfolio Average: +0.38% expected return
Overall Trend: MODERATELY BULLISH (52.6% signals)


🗂️ PROJECT STRUCTURE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

stock_prediction/
│
├─── SCRIPTS - Main Tools
│    ├─ download_more_stocks.py ✅ Download 10 more
│    ├─ train_20stocks_model.py ✅ Train big data
│    ├─ predict_20stocks.py ✅ Predict portfolio
│    ├─ master.py (automation entry)
│    └─ ... (8 other production scripts)
│
├─── DATA - Stock Information
│    ├─ data/TCS_NS_ohlcv.csv
│    ├─ data/INFY_NS_ohlcv.csv
│    ├─ data/... (20 stocks total)
│    └─ data/... (~8,421 rows combined)
│
├─── MODELS - Trained Models
│    ├─ multistock_20stocks_xgb.pkl ⭐ ACTIVE
│    ├─ multistock_20stocks_xgb_features.pkl
│    ├─ multistock_20stocks_xgb_metrics.pkl
│    └─ (Other model versions)
│
├─── RESULTS - Predictions
│    └─ portfolio_20stocks_*.csv
│
└─── DOCUMENTATION
     ├─ BIGDATA_SCALING_10to20.md ⭐ NEW
     ├─ README.md
     ├─ QUICKSTART.md
     └─ ... (other guides)


💡 TIPS & TRICKS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Keep Historical Results
   └─ All predictions saved to results/*.csv
   └─ Can track model changes over time

2. Import These Scripts
   └─ from predict_20stocks import predict_stock_20
   └─ from train_20stocks_model import train_big_data_model
   └─ Use in your own scripts

3. Monitor Model Performance
   └─ python analyze.py compare
   └─ Shows accuracy improvements as more data added

4. Schedule Predictions
   └─ Run python predict_20stocks.py daily
   └─ Export results to CSV
   └─ Build historical tracking


🔄 DAILY WORKFLOW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Morning Check:
  1. python predict_20stocks.py
  2. Review CSV results
  3. Check top bullish opportunities

Weekly Update:
  1. python train_20stocks_model.py
  2. python analyze.py compare
  3. python predict_20stocks.py

Monthly Scaling:
  1. python download_more_stocks.py (add 10 more)
  2. python train_20stocks_model.py (with new stocks)
  3. Compare results


🚀 NEXT PHASE - SCALING TO 40+ STOCKS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

When ready to scale further:

Step 1: Download all available stocks
  └─ python bulk_download.py all
  └─ Time: ~15 minutes

Step 2: Train on massive dataset
  └─ python train_multistock_model.py all
  └─ Time: ~5 minutes
  └─ Result: 16,000+ rows, 40+ stocks

Step 3: Generate enterprise predictions
  └─ python batch_predict.py portfolio
  └─ Time: ~60 seconds
  └─ Result: Full market coverage


📊 SUCCESS METRICS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Current System:
  ✅ 20 stocks downloaded
  ✅ 8,421 data points
  ✅ 72.39% model accuracy
  ✅ 19 stocks predicted
  ✅ 52.6% bullish signals
  ✅ 9 sectors covered


╔═══════════════════════════════════════════════════════════════════════════╗
║                    Ready to Scale Further? 🚀                            ║
║                                                                            ║
║  Next: python bulk_download.py all    (Get 40+ stocks)                   ║
║  Then: python train_multistock_model.py all  (Train enterprise model)    ║
║  Finally: python batch_predict.py portfolio  (Full market analysis)      ║
╚═══════════════════════════════════════════════════════════════════════════╝
""")
