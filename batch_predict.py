"""
Batch Prediction and Portfolio Analysis
Make predictions for multiple stocks and analyze sector performance
"""

import pandas as pd
import pickle
import os
from features_advanced import create_advanced_features, get_feature_list
from indian_stocks_config import INDIAN_STOCKS, SECTOR_GROUPS, STARTER_STOCKS
import yfinance as yf
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

# Load sentiment
try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

load_dotenv()
NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')

def get_news_sentiment(stock_symbol):
    """Get sentiment for a stock using NewsAPI + VADER"""
    
    if not NEWSAPI_KEY:
        return None
    
    company_name = INDIAN_STOCKS.get(stock_symbol, stock_symbol)
    
    to_date = datetime.now()
    from_date = to_date - timedelta(days=3)
    from_date_str = from_date.strftime('%Y-%m-%d')
    
    url = "https://newsapi.org/v2/everything"
    params = {
        'q': company_name,
        'from': from_date_str,
        'language': 'en',
        'sortBy': 'relevancy',
        'pageSize': 5,
        'apiKey': NEWSAPI_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        if response.status_code != 200:
            return None
        
        data = response.json()
        if data.get('totalResults', 0) == 0:
            return None
        
        sia = SentimentIntensityAnalyzer()
        articles = data.get('articles', [])[:5]
        sentiments = []
        
        for article in articles:
            text = f"{article['title']} {article.get('description', '')}"
            sentiment_scores = sia.polarity_scores(text)
            sentiments.append(sentiment_scores['compound'])
        
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0.0
        
        if avg_sentiment > 0.1:
            sentiment_label = 'BULLISH'
        elif avg_sentiment < -0.1:
            sentiment_label = 'BEARISH'
        else:
            sentiment_label = 'NEUTRAL'
        
        return {
            'sentiment': sentiment_label,
            'score': avg_sentiment,
            'article_count': len(articles)
        }
    except:
        return None

def predict_stock(stock_symbol, model_path='models/multistock_starter_xgb.pkl'):
    """Make prediction for a single stock"""
    
    try:
        # Download data
        data = yf.download(stock_symbol, start='2024-01-01', end='2026-04-01', progress=False)
        if data.empty:
            return None
        
        data = data.reset_index()
        
        # Create features
        df_featured = create_advanced_features(data)
        
        # Load model
        if not os.path.exists(model_path):
            return None
        
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        
        # Get features list
        feature_path = model_path.replace('.pkl', '').replace('_xgb', '_features.pkl')
        if os.path.exists(feature_path):
            with open(feature_path, "rb") as f:
                feature_cols = pickle.load(f)
        else:
            feature_cols = get_feature_list()
        
        # Make prediction
        latest = df_featured[feature_cols].iloc[-1:].fillna(0)
        predicted_return = model.predict(latest)[0]
        
        current_price = df_featured['Close'].iloc[-1]
        predicted_price = current_price * (1 + predicted_return)
        
        # Get sentiment
        sentiment = get_news_sentiment(stock_symbol)
        
        return {
            'symbol': stock_symbol,
            'company': INDIAN_STOCKS.get(stock_symbol, stock_symbol),
            'current_price': current_price,
            'predicted_price': predicted_price,
            'predicted_return': predicted_return,
            'change_amount': predicted_price - current_price,
            'change_percent': predicted_return * 100,
            'signal': 'BULLISH' if predicted_return > 0 else ('BEARISH' if predicted_return < 0 else 'NEUTRAL'),
            'sentiment': sentiment['sentiment'] if sentiment else 'N/A',
            'sentiment_score': sentiment['score'] if sentiment else 0,
        }
    
    except Exception as e:
        return None

def batch_predict(stocks=None, model_path='models/multistock_starter_xgb.pkl'):
    """Make predictions for multiple stocks"""
    
    if stocks is None:
        stocks = STARTER_STOCKS
    
    print("="*120)
    print("BATCH PREDICTIONS")
    print("="*120)
    print(f"Model: {model_path} | Stocks: {len(stocks)}\n")
    
    results = []
    
    for i, symbol in enumerate(stocks, 1):
        print(f"[{i}/{len(stocks)}] {symbol}...", end=" ", flush=True)
        pred = predict_stock(symbol, model_path)
        
        if pred:
            results.append(pred)
            signal = pred['signal']
            sentiment = pred['sentiment']
            change = pred['change_percent']
            alignment = "✓" if signal == sentiment else "✗" if sentiment != 'N/A' else "-"
            
            print(f"{signal:8} | Change: {change:+6.2f}% | Sentiment: {sentiment:8} {alignment}")
        else:
            print("FAILED")
    
    return pd.DataFrame(results)

def sector_analysis(model_path='models/multistock_starter_xgb.pkl'):
    """Analyze all sectors"""
    
    print("\n" + "="*120)
    print("SECTOR ANALYSIS")
    print("="*120 + "\n")
    
    sector_results = {}
    
    for sector, stocks in SECTOR_GROUPS.items():
        # Filter stocks to those we have data for
        available_stocks = [s for s in stocks if os.path.exists(f"data/{s.replace('.', '_')}_ohlcv.csv")]
        
        if not available_stocks:
            continue
        
        print(f"{sector}:")
        sector_data = []
        
        for symbol in available_stocks:
            pred = predict_stock(symbol, model_path)
            if pred:
                sector_data.append(pred)
        
        if sector_data:
            df_sector = pd.DataFrame(sector_data)
            avg_return = df_sector['change_percent'].mean()
            bullish_count = (df_sector['signal'] == 'BULLISH').sum()
            bearish_count = (df_sector['signal'] == 'BEARISH').sum()
            
            sector_results[sector] = {
                'avg_return': avg_return,
                'bullish': bullish_count,
                'bearish': bearish_count,
                'neutral': len(sector_data) - bullish_count - bearish_count,
                'stocks_analyzed': len(sector_data)
            }
            
            print(f"  Avg Return: {avg_return:+6.2f}% | Bullish: {bullish_count} | Bearish: {bearish_count}")
        
        print()
    
    return sector_results

def portfolio_summary(stocks=None):
    """Generate portfolio summary"""
    
    if stocks is None:
        stocks = STARTER_STOCKS
    
    df_results = batch_predict(stocks)
    
    if df_results.empty:
        print("No predictions generated")
        return
    
    print("\n" + "="*120)
    print("PORTFOLIO SUMMARY")
    print("="*120 + "\n")
    
    # Overall statistics
    total_bullish = (df_results['signal'] == 'BULLISH').sum()
    total_bearish = (df_results['signal'] == 'BEARISH').sum()
    avg_return = df_results['change_percent'].mean()
    
    print(f"Total Stocks Analyzed: {len(df_results)}")
    print(f"Bullish Signals:       {total_bullish} ({total_bullish/len(df_results)*100:.1f}%)")
    print(f"Bearish Signals:       {total_bearish} ({total_bearish/len(df_results)*100:.1f}%)")
    print(f"Average Expected Return: {avg_return:+.2f}%\n")
    
    # Top gainers
    print("TOP 5 BULLISH (Highest Expected Return):")
    top_gainers = df_results.nlargest(5, 'change_percent')
    for idx, row in top_gainers.iterrows():
        print(f"  {row['symbol']:15} : {row['change_percent']:+6.2f}% ({row['company'][:30]})")
    
    print("\nTOP 5 BEARISH (Lowest Expected Return):")
    top_losers = df_results.nsmallest(5, 'change_percent')
    for idx, row in top_losers.iterrows():
        print(f"  {row['symbol']:15} : {row['change_percent']:+6.2f}% ({row['company'][:30]})")
    
    # Save to CSV
    csv_path = f"results/portfolio_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    os.makedirs('results', exist_ok=True)
    df_results.to_csv(csv_path, index=False)
    print(f"\nResults saved: {csv_path}")
    
    return df_results

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'portfolio':
            portfolio_summary()
        elif sys.argv[1] == 'sector':
            sector_analysis()
        elif sys.argv[1].startswith('batch:'):
            stock_list = sys.argv[1].replace('batch:', '').split(',')
            batch_predict(stock_list)
        else:
            # Single prediction
            pred = predict_stock(sys.argv[1])
            if pred:
                for k, v in pred.items():
                    print(f"{k}: {v}")
    else:
        print("Generate portfolio summary for all starter stocks")
        portfolio_summary()
