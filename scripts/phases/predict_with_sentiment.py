import pandas as pd
import pickle
import sys
import io
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

# Fix encoding for Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Download VADER lexicon
try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

# Load environment variables
load_dotenv()
NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')

# Company name and symbol mappings
COMPANY_NAMES = {
    'TCS.NS': 'Tata Consultancy Services',
    'INFY.NS': 'Infosys Limited',
    'WIPRO.NS': 'Wipro Limited',
    'RELIANCE.NS': 'Reliance Industries',
    'HINDUNILVR.NS': 'Hindustan Unilever',
    'LT.NS': 'Larsen & Toubro',
    'MARUTI.NS': 'Maruti Suzuki India',
    'AAPL': 'Apple Inc',
    'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Alphabet Inc',
}

COMPANY_SYMBOLS = {
    'TCS.NS': 'TCS',
    'INFY.NS': 'INFY',
    'WIPRO.NS': 'WIPRO',
    'RELIANCE.NS': 'RIL',
    'HINDUNILVR.NS': 'HINDUNILVR',
    'LT.NS': 'LT',
    'MARUTI.NS': 'MARUTI',
}

def create_normalized_features(df):
    """Create normalized features for prediction"""
    
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df = df.tail(500)
    
    df['ma_10'] = df['Close'].rolling(10).mean()
    df['ma_20'] = df['Close'].rolling(20).mean()
    df['ma_50'] = df['Close'].rolling(50).mean()
    
    df['ma_10_ratio'] = df['ma_10'] / df['Close']
    df['ma_20_ratio'] = df['ma_20'] / df['Close']
    df['ma_50_ratio'] = df['ma_50'] / df['Close']
    
    df['lag_1'] = df['Close'].shift(1)
    df['lag_2'] = df['Close'].shift(2)
    df['lag_3'] = df['Close'].shift(3)
    
    df['lag_1_ratio'] = df['lag_1'] / df['Close']
    df['lag_2_ratio'] = df['lag_2'] / df['Close']
    df['lag_3_ratio'] = df['lag_3'] / df['Close']
    
    df['returns'] = df['Close'].pct_change()
    df['returns_2'] = df['Close'].pct_change(2)
    df['returns_3'] = df['Close'].pct_change(3)
    
    df['volatility'] = df['returns'].rolling(10).std()
    df['target'] = df['Close'].pct_change().shift(-1)
    
    df.dropna(inplace=True)
    return df

def get_news_sentiment(stock_symbol):
    """Fetch news sentiment using NewsAPI + VADER sentiment analysis"""
    
    if not NEWSAPI_KEY:
        print("Warning: NEWSAPI_KEY not configured. Skipping sentiment analysis.")
        return None
    
    company_name = COMPANY_NAMES.get(stock_symbol, stock_symbol)
    
    # Calculate date range
    to_date = datetime.now()
    from_date = to_date - timedelta(days=3)
    from_date_str = from_date.strftime('%Y-%m-%d')
    
    url = "https://newsapi.org/v2/everything"
    params = {
        'q': company_name,
        'from': from_date_str,
        'language': 'en',
        'sortBy': 'relevancy',
        'pageSize': 10,
        'apiKey': NEWSAPI_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=5)
        
        if response.status_code != 200:
            return None
        
        data = response.json()
        
        if data.get('totalResults', 0) == 0:
            return {'sentiment': 'NEUTRAL', 'score': 0.0, 'article_count': 0}
        
        # Initialize VADER sentiment analyzer
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
        
    except Exception as e:
        print(f"Error fetching sentiment: {e}")
        return None

def predict_with_sentiment(stock_symbol, model_path="models/universal_xgb_model.pkl"):
    """Predict price with sentiment analysis"""
    
    csv_file = f"data/{stock_symbol.replace('.', '_')}_ohlcv.csv"
    company_name = COMPANY_NAMES.get(stock_symbol, "Unknown Company")
    
    print(f"\n{'='*80}")
    print(f"PRICE PREDICTION WITH SENTIMENT ANALYSIS")
    print(f"{'='*80}")
    print(f"\nCompany:              {company_name}")
    print(f"Stock Symbol:         {stock_symbol}")
    
    try:
        # Load price prediction
        df = pd.read_csv(csv_file)
        df_featured = create_normalized_features(df)
        
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        
        feature_cols = [
            'ma_10_ratio', 'ma_20_ratio', 'ma_50_ratio',
            'lag_1_ratio', 'lag_2_ratio', 'lag_3_ratio',
            'returns', 'returns_2', 'returns_3', 'volatility'
        ]
        
        latest = df_featured[feature_cols].iloc[-1:].fillna(0)
        predicted_return = model.predict(latest)[0]
        
        current_price = df_featured['Close'].iloc[-1]
        predicted_price = current_price * (1 + predicted_return)
        change = predicted_price - current_price
        
        # Get sentiment
        sentiment_data = get_news_sentiment(stock_symbol)
        
        # Display results
        print(f"\n{'—'*80}")
        print("PRICE PREDICTION (Technical Analysis):")
        print(f"{'—'*80}")
        print(f"Current price:        {current_price:.2f}")
        print(f"Predicted next close: {predicted_price:.2f}")
        print(f"Expected change:      {change:.2f} ({predicted_return*100:+.2f}%)")
        
        if predicted_return > 0:
            price_signal = "BULLISH (UP)"
        elif predicted_return < 0:
            price_signal = "BEARISH (DOWN)"
        else:
            price_signal = "NEUTRAL"
        
        print(f"Price Signal:         {price_signal}")
        
        # Display sentiment
        if sentiment_data:
            print(f"\n{'—'*80}")
            print("NEWS SENTIMENT (Market Sentiment):")
            print(f"{'—'*80}")
            print(f"Sentiment:            {sentiment_data['sentiment']}")
            print(f"Sentiment Score:      {sentiment_data['score']:.3f}")
            print(f"Recent Articles:      {sentiment_data['article_count']}")
            
            # Combined recommendation
            print(f"\n{'—'*80}")
            print("COMBINED RECOMMENDATION:")
            print(f"{'—'*80}")
            
            if price_signal.split()[0] == sentiment_data['sentiment']:
                confidence = "HIGH CONFIDENCE"
                emoji = "💪"
            else:
                confidence = "CONFLICTING SIGNALS"
                emoji = "⚠️"
            
            print(f"{emoji} {confidence}")
            print(f"Price Signal:   {price_signal}")
            print(f"News Sentiment: {sentiment_data['sentiment']}")
        
        print(f"\n{'='*80}\n")
        
        return {
            'company': company_name,
            'symbol': stock_symbol,
            'current': current_price,
            'predicted': predicted_price,
            'change': change,
            'change_pct': predicted_return * 100,
            'price_signal': price_signal,
            'sentiment': sentiment_data
        }
        
    except FileNotFoundError:
        print(f"Error: {csv_file} not found")
        return None
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        # Show predictions for default stocks
        default_stocks = ['TCS.NS', 'INFY.NS']
        
        for stock in default_stocks:
            predict_with_sentiment(stock)
        
        print("\nUsage for specific stock: python predict_with_sentiment.py <stock_symbol>")
        print("Example: python predict_with_sentiment.py WIPRO.NS")
    else:
        stock_symbol = sys.argv[1]
        predict_with_sentiment(stock_symbol)
