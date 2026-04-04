"""
Enhanced Feature Engineering with Advanced Technical Indicators
Includes RSI, MACD, Bollinger Bands, Volume Analysis
"""

import pandas as pd
import numpy as np

def calculate_rsi(prices, period=14):
    """Calculate Relative Strength Index"""
    try:
        prices = np.array(prices, dtype=float)
        deltas = np.diff(prices)
        seed = deltas[:period+1]
        ups = seed[seed >= 0]
        downs = -seed[seed < 0]
        
        up = ups.sum() / period
        down = downs.sum() / period
        
        if down == 0:
            return np.full_like(prices, 100.0 if up > 0 else 50.0)
        
        rs = up / down
        rsi = np.zeros_like(prices, dtype=float)
        rsi[:period] = 100. - 100. / (1. + rs)
        
        for i in range(period, len(prices)):
            delta = deltas[i-1]
            if delta > 0:
                upval = delta
                downval = 0.
            else:
                upval = 0.
                downval = -delta
            
            up = (up * (period - 1) + upval) / period
            down = (down * (period - 1) + downval) / period
            
            if down == 0:
                rs = 100 if upval > 0 else 0
            else:
                rs = up / down
            rsi[i] = 100. - 100. / (1. + rs)
        
        return rsi
    except Exception as e:
        return np.full_like(np.array(prices), 50.0)

def calculate_macd(prices, fast=12, slow=26, signal=9):
    """Calculate MACD (Moving Average Convergence Divergence)"""
    ema_fast = pd.Series(prices).ewm(span=fast).mean()
    ema_slow = pd.Series(prices).ewm(span=slow).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal).mean()
    histogram = macd - signal_line
    return macd.values, signal_line.values, histogram.values

def calculate_bollinger_bands(prices, period=20, std_dev=2):
    """Calculate Bollinger Bands"""
    sma = pd.Series(prices).rolling(period).mean()
    std = pd.Series(prices).rolling(period).std()
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    bb_position = (pd.Series(prices) - lower_band) / (upper_band - lower_band)
    return upper_band.values, sma.values, lower_band.values, bb_position.values

def calculate_volume_features(df):
    """Calculate volume-based features"""
    # Volume change
    volume_change = df['Volume'].pct_change()
    
    # On-Balance Volume (OBV)
    obv = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()
    
    # Volume Moving Average
    volume_ma = df['Volume'].rolling(20).mean()
    
    # Volume Rate of Change
    vroc = volume_change.rolling(14).mean()
    
    return {
        'volume_change': volume_change,
        'obv': obv,
        'volume_ma': volume_ma,
        'vroc': vroc
    }

def create_advanced_features(df):
    """
    Create advanced technical indicators for better predictions
    
    Input: df with OHLCV data
    Output: df with 15+ technical features
    """
    
    df = df.copy().reset_index(drop=True)
    
    # Handle multi-level column names from yfinance
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    # Ensure numeric types
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce').fillna(0)
    df['High'] = pd.to_numeric(df['High'], errors='coerce')
    df['Low'] = pd.to_numeric(df['Low'], errors='coerce')
    
    # Keep last 500 rows for features
    df = df.tail(500)
    
    # ===== BASIC MOVING AVERAGES =====
    df['ma_10'] = df['Close'].rolling(10).mean()
    df['ma_20'] = df['Close'].rolling(20).mean()
    df['ma_50'] = df['Close'].rolling(50).mean()
    
    # ===== MOMENTUM INDICATORS =====
    # RSI (Relative Strength Index)
    df['rsi_14'] = calculate_rsi(df['Close'].values, 14)
    df['rsi_21'] = calculate_rsi(df['Close'].values, 21)
    
    # MACD
    macd, signal, histogram = calculate_macd(df['Close'].values)
    df['macd'] = macd
    df['macd_signal'] = signal
    df['macd_histogram'] = histogram
    
    # ===== VOLATILITY INDICATORS =====
    # Bollinger Bands
    upper_bb, mid_bb, lower_bb, bb_pos = calculate_bollinger_bands(df['Close'].values)
    df['bb_upper'] = upper_bb
    df['bb_middle'] = mid_bb
    df['bb_lower'] = lower_bb
    df['bb_position'] = bb_pos  # 0-1 scale, position within bands
    
    # ATR (Average True Range)
    high_low = df['High'] - df['Low']
    high_close = abs(df['High'] - df['Close'].shift())
    low_close = abs(df['Low'] - df['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = ranges.max(axis=1)
    df['atr_14'] = true_range.rolling(14).mean()
    
    # ===== VOLUME INDICATORS =====
    vol_features = calculate_volume_features(df)
    df['volume_change'] = vol_features['volume_change']
    df['obv'] = vol_features['obv']
    df['volume_ma'] = vol_features['volume_ma']
    df['vroc'] = vol_features['vroc']
    
    # ===== PRICE ACTION =====
    # Returns and lags (existing)
    df['returns'] = df['Close'].pct_change()
    df['returns_2'] = df['Close'].pct_change(2)
    df['returns_3'] = df['Close'].pct_change(3)
    df['lag_1'] = df['Close'].shift(1)
    df['lag_2'] = df['Close'].shift(2)
    df['lag_3'] = df['Close'].shift(3)
    
    # Normalized versions
    df['ma_10_ratio'] = df['ma_10'] / df['Close']
    df['ma_20_ratio'] = df['ma_20'] / df['Close']
    df['ma_50_ratio'] = df['ma_50'] / df['Close']
    df['lag_1_ratio'] = df['lag_1'] / df['Close']
    
    # ===== VOLATILITY =====
    df['volatility'] = df['returns'].rolling(10).std()
    df['volatility_20'] = df['returns'].rolling(20).std()
    
    # ===== TREND STRENGTH =====
    # ADX-like indicator (trend strength)
    df['plus_di'] = ((df['High'] - df['High'].shift(1)) > (df['Low'].shift(1) - df['Low'])).astype(int)
    df['minus_di'] = ((df['Low'].shift(1) - df['Low']) > (df['High'] - df['High'].shift(1))).astype(int)
    df['trend_strength'] = (df['plus_di'] - df['minus_di']).rolling(14).mean()
    
    # ===== TARGET =====
    df['target'] = df['Close'].pct_change().shift(-1)
    
    # Drop rows with NaN
    df = df.dropna()
    
    return df


def get_feature_list():
    """Return list of all features used for modeling"""
    return [
        'ma_10_ratio', 'ma_20_ratio', 'ma_50_ratio',
        'lag_1_ratio',
        'returns', 'returns_2', 'returns_3',
        'volatility', 'volatility_20',
        'rsi_14', 'rsi_21',
        'macd', 'macd_signal', 'macd_histogram',
        'bb_position', 'atr_14',
        'volume_change', 'obv', 'volume_ma',
        'trend_strength'
    ]


if __name__ == '__main__':
    # Example usage
    import yfinance as yf
    
    data = yf.download('TCS.NS', start='2024-01-01', end='2026-04-01', progress=False)
    data = data.reset_index()
    
    features_df = create_advanced_features(data)
    print(f"Total rows: {len(features_df)}")
    print(f"Total features: {len(get_feature_list())}")
    print(f"\nFeatures:\n{features_df.columns.tolist()}")
    print(f"\nFirst row:\n{features_df.head(1)}")
