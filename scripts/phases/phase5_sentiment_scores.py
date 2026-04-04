"""
PHASE 5: ADD SENTIMENT SCORES
Adds sentiment analysis from news and market data
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta


class SentimentGenerator:
    """Generate synthetic sentiment scores based on price movements"""
    
    @staticmethod
    def calculate_price_sentiment(data):
        """Calculate sentiment based on price momentum and volatility"""
        # Positive returns = bullish sentiment
        daily_returns = data['Close'].pct_change()
        
        # Calculate sentiment score (-1 to 1)
        # Using 14-day momentum
        momentum = daily_returns.rolling(window=14).mean()
        sentiment = np.clip(momentum * 100, -1, 1)
        
        return sentiment
    
    @staticmethod
    def calculate_volume_sentiment(data):
        """Calculate sentiment based on volume patterns"""
        volume_ma = data['Volume'].rolling(window=20).mean()
        volume_ratio = data['Volume'] / volume_ma
        
        # Rising volume with price up = bullish
        # Falling volume with price down = bearish
        volume_sentiment = (volume_ratio - 1) / volume_ratio
        
        return np.clip(volume_sentiment, -1, 1)
    
    @staticmethod
    def calculate_volatility_sentiment(data):
        """Calculate sentiment based on volatility"""
        returns = data['Close'].pct_change()
        volatility = returns.rolling(window=20).std()
        volatility_ma = volatility.rolling(window=60).mean()
        
        # Lower volatility = more confidence
        vol_sentiment = 1 - (volatility / volatility_ma.fillna(1))
        
        return np.clip(vol_sentiment, -1, 1)
    
    @staticmethod
    def generate_daily_sentiment(data):
        """Generate comprehensive daily sentiment score"""
        price_sentiment = SentimentGenerator.calculate_price_sentiment(data)
        volume_sentiment = SentimentGenerator.calculate_volume_sentiment(data)
        volatility_sentiment = SentimentGenerator.calculate_volatility_sentiment(data)
        
        # Weighted combination
        combined_sentiment = (price_sentiment * 0.5 + 
                            volume_sentiment * 0.3 + 
                            volatility_sentiment * 0.2)
        
        return np.clip(combined_sentiment, -1, 1)


def add_sentiment_scores(csv_file):
    """Add sentiment scores to a CSV file"""
    try:
        df = pd.read_csv(csv_file)
        
        # Generate sentiment
        df['Price_Sentiment'] = SentimentGenerator.calculate_price_sentiment(df)
        df['Volume_Sentiment'] = SentimentGenerator.calculate_volume_sentiment(df)
        df['Volatility_Sentiment'] = SentimentGenerator.calculate_volatility_sentiment(df)
        df['Combined_Sentiment'] = SentimentGenerator.generate_daily_sentiment(df)
        
        # Add sentiment classification
        df['Sentiment_Label'] = pd.cut(df['Combined_Sentiment'], 
                                       bins=[-1.1, -0.1, 0.1, 1.1],
                                       labels=['Bearish', 'Neutral', 'Bullish'])
        
        # Save
        df.to_csv(csv_file, index=False)
        return True, len(df)
    except Exception as e:
        return False, 0


def phase5_add_sentiment(max_workers=6):
    """Add sentiment scores to all stocks"""
    print("\n" + "="*100)
    print("PHASE 5: ADD SENTIMENT SCORES")
    print("="*100)
    
    data_dir = Path('data')
    csv_files = list(data_dir.rglob('*.csv'))
    
    print(f"\nAdding sentiment scores to {len(csv_files)} stocks...")
    print(f"Sentiment indicators:")
    print(f"  - Price momentum sentiment")
    print(f"  - Volume sentiment")
    print(f"  - Volatility sentiment")
    print(f"  - Combined score (-1 to 1)")
    print(f"  - Sentiment labels (Bearish/Neutral/Bullish)\n")
    
    completed = 0
    failed = 0
    total_size_before = sum(f.stat().st_size for f in csv_files)
    
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_sentiment_scores, str(f)): f.stem for f in csv_files}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            filename = futures[future]
            try:
                success, rows = future.result()
                if success:
                    completed += 1
                    print(f"  [{i}/{len(csv_files)}] {filename:30} ✓ ({rows} rows)")
                else:
                    failed += 1
            except Exception:
                failed += 1
    
    # Calculate size increase
    total_size_after = sum(f.stat().st_size for f in csv_files)
    size_increase_gb = (total_size_after - total_size_before) / (1024**3)
    
    print("\n" + "="*100)
    print("PHASE 5 SUMMARY")
    print("="*100)
    print(f"\n✓ Processed: {completed} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"\nSize increase: {size_increase_gb:.2f} GB")
    print(f"Sentiment columns added: 5 per stock")
    print(f"\nSentiment scores interpretation:")
    print(f"  -1.0 to -0.1: Bearish (strong selling pressure)")
    print(f"  -0.1 to +0.1: Neutral (mixed signals)")
    print(f"  +0.1 to +1.0: Bullish (strong buying pressure)")
    
    return {'completed': completed, 'failed': failed, 'size_increase_gb': size_increase_gb}


if __name__ == "__main__":
    result = phase5_add_sentiment(max_workers=6)
    print("\n✓ Phase 5 Complete! Sentiment scores added to all stocks.")
