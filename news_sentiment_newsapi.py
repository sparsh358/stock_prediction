"""
News Sentiment Analysis using NewsAPI + VADER
Fetches news articles and performs sentiment analysis
"""

import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

# Download VADER lexicon
try:
    nltk.data.find('sentiment/vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon')

# Load environment variables
load_dotenv()
NEWSAPI_KEY = os.getenv('NEWSAPI_KEY')

# Company mapping
COMPANY_NAMES = {
    'TCS.NS': ('Tata Consultancy Services', 'TCS'),
    'INFY.NS': ('Infosys Limited', 'INFY'),
    'WIPRO.NS': ('Wipro Limited', 'WIPRO'),
    'RELIANCE.NS': ('Reliance Industries', 'RELIANCE'),
    'AAPL': ('Apple Inc.', 'AAPL'),
    'MSFT': ('Microsoft Corporation', 'MSFT'),
    'GOOGL': ('Alphabet Inc.', 'GOOGL'),
}

def get_company_search_term(symbol):
    """Get company name for news search"""
    if symbol in COMPANY_NAMES:
        return COMPANY_NAMES[symbol][0]
    return symbol

def get_news_sentiment_newsapi(stock_symbol, days=3):
    """
    Fetch news from NewsAPI and perform sentiment analysis using VADER
    
    Args:
        stock_symbol: Stock symbol (e.g., 'TCS.NS', 'AAPL')
        days: Number of days to look back for news
    
    Returns:
        dict: Contains articles, sentiment scores, and classification
    """
    
    if not NEWSAPI_KEY:
        print("Error: NEWSAPI_KEY not found in .env file")
        return None
    
    company_name = get_company_search_term(stock_symbol)
    
    # Calculate date range
    to_date = datetime.now()
    from_date = to_date - timedelta(days=days)
    from_date_str = from_date.strftime('%Y-%m-%d')
    
    # Fetch news from NewsAPI
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
        response = requests.get(url, params=params, timeout=10)
        
        if response.status_code != 200:
            print(f"API Error: {response.status_code}")
            print(f"Response: {response.json()}")
            return None
        
        data = response.json()
        
        if data['totalResults'] == 0:
            print(f"No articles found for {company_name}")
            return None
        
        # Initialize VADER sentiment analyzer
        sia = SentimentIntensityAnalyzer()
        
        # Process articles
        articles = data['articles'][:5]  # Top 5 articles
        sentiments = []
        
        for article in articles:
            # Analyze headline and description
            text = f"{article['title']} {article['description'] or ''}"
            sentiment_scores = sia.polarity_scores(text)
            sentiments.append({
                'headline': article['title'],
                'source': article['source']['name'],
                'url': article['url'],
                'published': article['publishedAt'],
                'sentiment_score': sentiment_scores['compound'],  # -1 to +1
                'positive': sentiment_scores['pos'],
                'negative': sentiment_scores['neg'],
                'neutral': sentiment_scores['neu']
            })
        
        # Calculate average sentiment
        avg_sentiment = sum(s['sentiment_score'] for s in sentiments) / len(sentiments) if sentiments else 0
        
        # Classify sentiment
        if avg_sentiment > 0.1:
            sentiment_classification = "BULLISH"
        elif avg_sentiment < -0.1:
            sentiment_classification = "BEARISH"
        else:
            sentiment_classification = "NEUTRAL"
        
        return {
            'symbol': stock_symbol,
            'company': company_name,
            'articles': sentiments,
            'average_sentiment': avg_sentiment,
            'classification': sentiment_classification,
            'article_count': len(sentiments),
            'date_range': f"{from_date_str} to {to_date.strftime('%Y-%m-%d')}"
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {str(e)}")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None


def display_sentiment_report(sentiment_data):
    """Display formatted sentiment analysis report"""
    
    if not sentiment_data:
        return
    
    print("\n" + "="*80)
    print("NEWS SENTIMENT ANALYSIS (NewsAPI + VADER)")
    print("="*80)
    print(f"\nCompany:           {sentiment_data['company']}")
    print(f"Symbol:            {sentiment_data['symbol']}")
    print(f"Analysis Period:   {sentiment_data['date_range']}")
    print(f"Articles Analyzed: {sentiment_data['article_count']}")
    
    print("\n" + "-"*80)
    print("SENTIMENT SUMMARY")
    print("-"*80)
    print(f"Average Sentiment Score: {sentiment_data['average_sentiment']:.3f}")
    print(f"Classification:          {sentiment_data['classification']}")
    
    if sentiment_data['classification'] == 'BULLISH':
        signal = "POSITIVE (UP)"
    elif sentiment_data['classification'] == 'BEARISH':
        signal = "NEGATIVE (DOWN)"
    else:
        signal = "NEUTRAL"
    
    print(f"Market Signal:           {signal}")
    
    print("\n" + "-"*80)
    print("TOP ARTICLES")
    print("-"*80)
    
    for i, article in enumerate(sentiment_data['articles'], 1):
        print(f"\n{i}. {article['headline'][:70]}")
        print(f"   Source: {article['source']}")
        print(f"   Sentiment: {article['sentiment_score']:+.3f} ", end="")
        
        if article['sentiment_score'] > 0.1:
            print("[POSITIVE]")
        elif article['sentiment_score'] < -0.1:
            print("[NEGATIVE]")
        else:
            print("[NEUTRAL]")
        
        print(f"   Published: {article['published'][:10]}")
        print(f"   URL: {article['url'][:60]}...")
    
    print("\n" + "="*80)


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    else:
        print("Usage: python news_sentiment_newsapi.py <symbol>")
        print("Example: python news_sentiment_newsapi.py TCS.NS")
        sys.exit(1)
    
    print(f"Fetching news for {symbol} using NewsAPI + VADER...")
    result = get_news_sentiment_newsapi(symbol)
    display_sentiment_report(result)
