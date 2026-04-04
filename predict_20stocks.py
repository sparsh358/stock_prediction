"""
Predict for all 20 stocks using the big data model
"""

import pandas as pd
import pickle
import os
import sys
from features_advanced import create_advanced_features, get_feature_list
import yfinance as yf
from dotenv import load_dotenv
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
import requests
from datetime import datetime, timedelta

# Download VADER if needed
try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    print("Installing VADER sentiment analyzer...")
    nltk.download('vader_lexicon', quiet=True)

load_dotenv()
NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')

COMPANY_NAMES = {
    'TCS.NS': 'Tata Consultancy Services',
    'INFY.NS': 'Infosys Limited',
    'WIPRO.NS': 'Wipro Limited',
    'RELIANCE.NS': 'Reliance Industries',
    'ICICIBANK.NS': 'ICICI Bank',
    'SBIN.NS': 'State Bank of India',
    'MARUTI.NS': 'Maruti Suzuki',
    'HINDUNILVR.NS': 'Hindustan Unilever',
    'AXISBANK.NS': 'Axis Bank',
    'HDFCBANK.NS': 'HDFC Bank',
    'BHARTIARTL.NS': 'Bharti Airtel',
    'DRREDDY.NS': 'Dr. Reddy\'s',
    'SUNPHARMA.NS': 'Sun Pharmaceutical',
    'IOC.NS': 'Indian Oil',
    'BPCL.NS': 'BPCL',
    'ITC.NS': 'ITC Limited',
    'BRITANNIA.NS': 'Britannia',
    'LT.NS': 'Larsen & Toubro',
    'DLF.NS': 'DLF Limited',
}

def get_news_sentiment(symbol):
    """Get sentiment using NewsAPI + VADER"""
    if not NEWSAPI_KEY:
        return None, []
    
    company_name = COMPANY_NAMES.get(symbol, symbol)
    
    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=3)
        from_date_str = from_date.strftime('%Y-%m-%d')
        
        url = "https://newsapi.org/v2/everything"
        params = {
            'q': company_name,
            'from': from_date_str,
            'sortBy': 'publishedAt',
            'language': 'en',
            'apiKey': NEWSAPI_KEY
        }
        
        response = requests.get(url, params=params, timeout=5)
        articles = response.json().get('articles', [])[:5]
        
        if not articles:
            return None, []
        
        sia = SentimentIntensityAnalyzer()
        scores = [sia.polarity_scores(a['title'])['compound'] for a in articles]
        avg_score = sum(scores) / len(scores)
        
        if avg_score > 0.1:
            sentiment = "BULLISH"
        elif avg_score < -0.1:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"
        
        return sentiment, articles[:3]
    except:
        return None, []

def predict_stock_20(symbol, model_path='models/multistock_20stocks_xgb.pkl'):
    """Predict for a single stock using 20-stock model"""
    
    try:
        # Load data
        csv_file = f'data/{symbol.replace(".", "_")}_ohlcv.csv'
        if not os.path.exists(csv_file):
            return None, None, None
        
        df = pd.read_csv(csv_file, index_col=0)
        
        # Create features
        df = create_advanced_features(df)
        df.dropna(inplace=True)
        
        if len(df) == 0:
            return None, None, None
        
        # Load model
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        # Get latest features
        feature_cols = get_feature_list()
        latest = df[feature_cols].iloc[-1:].fillna(0)
        
        # Predict
        prediction = model.predict(latest)[0]
        current_price = df['Close'].iloc[-1]
        price_change_pct = (prediction * 100)
        
        # Determine signal
        if price_change_pct > 0.2:
            signal = "BULLISH"
        elif price_change_pct < -0.2:
            signal = "BEARISH"
        else:
            signal = "NEUTRAL"
        
        return price_change_pct, signal, current_price
    except Exception as e:
        return None, None, None

def batch_predict_20stocks():
    """Predict for all 20 stocks"""
    
    STOCKS_20 = [
        'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'RELIANCE.NS', 'ICICIBANK.NS',
        'SBIN.NS', 'MARUTI.NS', 'HINDUNILVR.NS', 'AXISBANK.NS', 'HDFCBANK.NS',
        'BHARTIARTL.NS', 'DRREDDY.NS', 'SUNPHARMA.NS', 'IOC.NS', 'BPCL.NS',
        'ITC.NS', 'BRITANNIA.NS', 'LT.NS', 'DLF.NS', 'WIPRO.NS'
    ]
    
    print('\n' + '='*100)
    print('PREDICTIONS - 20 STOCKS BIG DATA MODEL')
    print('='*100)
    
    results = []
    bullish_count = 0
    bearish_count = 0
    total_return = 0
    
    unique_stocks = list(dict.fromkeys(STOCKS_20))  # Remove duplicates
    
    print('\\nSymbol         Signal    Current Price   Predicted Price   Change %   Sentiment  Align')
    print('-' * 100)
    
    for i, symbol in enumerate(unique_stocks, 1):
        
        change_pct, signal, price = predict_stock_20(symbol)
        
        if change_pct is None:
            print(f'{symbol:12s}  ❌ SKIP')
            continue
        
        # Get sentiment
        sentiment, articles = get_news_sentiment(symbol)
        
        # Count signals
        if signal == "BULLISH":
            bullish_count += 1
        else:
            bearish_count += 1
        
        total_return += change_pct
        
        # Alignment check
        align = "✓" if (signal == "BULLISH" and sentiment == "BULLISH") or \
                       (signal == "BEARISH" and sentiment == "BEARISH") else "✗"
        
        predicted_price = price * (1 + change_pct/100)
        
        result = {
            'symbol': symbol,
            'current_price': price,
            'predicted_price': predicted_price,
            'change_pct': change_pct,
            'signal': signal,
            'sentiment': sentiment if sentiment else 'NONE',
            'align': align
        }
        results.append(result)
        
        print(f'{symbol:12s}  {signal:8s}   ₹{price:10.2f}      ₹{predicted_price:10.2f}      {change_pct:+6.2f}%   {sentiment or "N/A":8s}  {align}')
    
    # Summary
    print('\n' + '='*100)
    print('PORTFOLIO SUMMARY - 20 STOCKS')
    print('='*100)
    
    print(f'\nTotal Stocks Analyzed: {len(results)}')
    print(f'Bullish Signals: {bullish_count} ({100*bullish_count/len(results) if results else 0:.1f}%)')
    print(f'Bearish Signals: {bearish_count} ({100*bearish_count/len(results) if results else 0:.1f}%)')
    print(f'Average Expected Return: {total_return/len(results) if results else 0:+.2f}%')    
    # Show detailed table
    print('\\n' + '='*100)
    print('DETAILED PRICE ANALYSIS - ALL STOCKS')
    print('='*100)
    print('\\nSymbol         Signal      Current Price   Predicted Price   Change %   Company')
    print('-' * 100)
    for r in sorted(results, key=lambda x: x['change_pct'], reverse=True):
        company = COMPANY_NAMES.get(r['symbol'], r['symbol'])
        print(f"{r['symbol']:12s}  {r['signal']:8s}   ₹{r['current_price']:10.2f}      ₹{r['predicted_price']:10.2f}      {r['change_pct']:+6.2f}%   {company}")    
    # Top opportunities
    print('\n📈 TOP 5 BULLISH (Highest Expected Return):')
    print('   Symbol               Current Price   Predicted Price   Change %   Company')
    print('   ' + '-' * 95)
    bullish = sorted([r for r in results if r['signal'] == 'BULLISH'], 
                     key=lambda x: x['change_pct'], reverse=True)[:5]
    for r in bullish:
        print(f"  ✅ {r['symbol']:12s}     ₹{r['current_price']:10.2f}     ₹{r['predicted_price']:10.2f}     {r['change_pct']:+6.2f}%   {COMPANY_NAMES.get(r['symbol'], r['symbol'])}")    
    
    # Top concerns
    print('\n📉 TOP 5 BEARISH (Lowest Expected Return):')
    print('   Symbol               Current Price   Predicted Price   Change %   Company')
    print('   ' + '-' * 95)
    bearish = sorted([r for r in results if r['signal'] == 'BEARISH'], 
                     key=lambda x: x['change_pct'])[:5]
    for r in bearish:
        print(f"  ❌ {r['symbol']:12s}     ₹{r['current_price']:10.2f}     ₹{r['predicted_price']:10.2f}     {r['change_pct']:+6.2f}%   {COMPANY_NAMES.get(r['symbol'], r['symbol'])}")
    print('\n[Final] Saving results...')
    df_results = pd.DataFrame(results)
    
    # Reorder columns for better readability
    df_results = df_results[['symbol', 'current_price', 'predicted_price', 'change_pct', 'signal', 'sentiment', 'align']]
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_path = f'results/portfolio_20stocks_{timestamp}.csv'
    
    # Create results directory if needed
    os.makedirs('results', exist_ok=True)
    
    df_results.to_csv(csv_path, index=False)
    print(f'  ✅ Results saved: {csv_path}')
    print(f'  ✅ Rows: {len(df_results)} stocks')
    print(f'  ✅ Columns: symbol, current_price, predicted_price, change_pct, signal, sentiment, align')
    
    print('\n' + '='*100)
    print('✅ BIG DATA ANALYSIS COMPLETE')
    print('='*100)

if __name__ == '__main__':
    batch_predict_20stocks()
