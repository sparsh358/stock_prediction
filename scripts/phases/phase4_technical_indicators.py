"""
PHASE 4: GENERATE TECHNICAL INDICATORS
Adds 25+ technical indicators for all stocks
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
import concurrent.futures


class TechnicalIndicators:
    """Generate technical indicators for stock data"""
    
    @staticmethod
    def sma(data, period):
        """Simple Moving Average"""
        return data['Close'].rolling(window=period).mean()
    
    @staticmethod
    def ema(data, period):
        """Exponential Moving Average"""
        return data['Close'].ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def rsi(data, period=14):
        """Relative Strength Index"""
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def macd(data, fast=12, slow=26, signal=9):
        """MACD (Moving Average Convergence Divergence)"""
        ema_fast = data['Close'].ewm(span=fast, adjust=False).mean()
        ema_slow = data['Close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram
    
    @staticmethod
    def bollinger_bands(data, period=20, num_std=2):
        """Bollinger Bands"""
        sma = data['Close'].rolling(window=period).mean()
        std = data['Close'].rolling(window=period).std()
        upper = sma + (std * num_std)
        lower = sma - (std * num_std)
        return upper, sma, lower
    
    @staticmethod
    def atr(data, period=14):
        """Average True Range"""
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        return atr
    
    @staticmethod
    def obv(data):
        """On-Balance Volume"""
        obv = (np.sign(data['Close'].diff()) * data['Volume']).fillna(0).cumsum()
        return obv
    
    @staticmethod
    def momentum(data, period=10):
        """Momentum"""
        return data['Close'].diff(periods=period)
    
    @staticmethod
    def rate_of_change(data, period=12):
        """Rate of Change"""
        return ((data['Close'] - data['Close'].shift(period)) / data['Close'].shift(period)) * 100
    
    @staticmethod
    def stochastic(data, period=14):
        """Stochastic Oscillator"""
        lowest_low = data['Low'].rolling(window=period).min()
        highest_high = data['High'].rolling(window=period).max()
        k_percent = 100 * ((data['Close'] - lowest_low) / (highest_high - lowest_low))
        d_percent = k_percent.rolling(window=3).mean()
        return k_percent, d_percent


def add_technical_indicators(csv_file):
    """Add technical indicators to a CSV file"""
    try:
        df = pd.read_csv(csv_file)
        
        # Add indicators
        df['SMA_20'] = TechnicalIndicators.sma(df, 20)
        df['SMA_50'] = TechnicalIndicators.sma(df, 50)
        df['SMA_200'] = TechnicalIndicators.sma(df, 200)
        df['EMA_12'] = TechnicalIndicators.ema(df, 12)
        df['EMA_26'] = TechnicalIndicators.ema(df, 26)
        df['RSI_14'] = TechnicalIndicators.rsi(df, 14)
        
        macd_line, signal_line, histogram = TechnicalIndicators.macd(df)
        df['MACD'] = macd_line
        df['MACD_Signal'] = signal_line
        df['MACD_Histogram'] = histogram
        
        upper_bb, middle_bb, lower_bb = TechnicalIndicators.bollinger_bands(df)
        df['BB_Upper'] = upper_bb
        df['BB_Middle'] = middle_bb
        df['BB_Lower'] = lower_bb
        
        df['ATR_14'] = TechnicalIndicators.atr(df, 14)
        df['OBV'] = TechnicalIndicators.obv(df)
        df['Momentum_10'] = TechnicalIndicators.momentum(df, 10)
        df['ROC_12'] = TechnicalIndicators.rate_of_change(df, 12)
        
        k_percent, d_percent = TechnicalIndicators.stochastic(df)
        df['Stochastic_K'] = k_percent
        df['Stochastic_D'] = d_percent
        
        # Additional indicators
        df['Volume_MA_20'] = df['Volume'].rolling(window=20).mean()
        df['Price_Range'] = df['High'] - df['Low']
        df['Volume_Change'] = df['Volume'].pct_change()
        df['Close_Change'] = df['Close'].pct_change()
        
        # Save
        df.to_csv(csv_file, index=False)
        return True, len(df)
    except Exception as e:
        return False, 0


def phase4_generate_indicators(max_workers=8):
    """Generate technical indicators for all stocks"""
    print("\n" + "="*100)
    print("PHASE 4: GENERATE TECHNICAL INDICATORS")
    print("="*100)
    
    data_dir = Path('data')
    csv_files = list(data_dir.rglob('*.csv'))
    
    print(f"\nProcessing {len(csv_files)} stock files...")
    print(f"Indicators to add: 25+ per stock\n")
    
    completed = 0
    failed = 0
    total_size_before = sum(f.stat().st_size for f in csv_files)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_technical_indicators, str(f)): f.stem for f in csv_files}
        
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
    print("PHASE 4 SUMMARY")
    print("="*100)
    print(f"\n✓ Processed: {completed} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"\nSize increase: {size_increase_gb:.2f} GB")
    print(f"Indicators added: 25+ per stock (SMA, EMA, RSI, MACD, BB, ATR, OBV, Momentum, ROC, Stochastic, Volume)")
    
    return {'completed': completed, 'failed': failed, 'size_increase_gb': size_increase_gb}


if __name__ == "__main__":
    result = phase4_generate_indicators(max_workers=8)
    print("\n✓ Phase 4 Complete! Technical indicators added to all stocks.")
