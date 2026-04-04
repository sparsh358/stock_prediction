import pandas as pd

def create_features(df):
    df = df.copy()


    df = df.tail(500)


    # 🔥 Convert columns to numeric
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df['close'] = df['Close']
    df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
    df['High'] = pd.to_numeric(df['High'], errors='coerce')
    df['Low'] = pd.to_numeric(df['Low'], errors='coerce')
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')
    df['high_low_diff'] = df['High'] - df['Low']

    # Moving averages
    df['ma_10'] = df['Close'].rolling(10).mean()
    df['ma_20'] = df['Close'].rolling(20).mean()
    df['ma_50'] = df['Close'].rolling(50).mean()

    # adding lags 
    
    df['lag_1'] = df['Close'].shift(1)
    df['lag_2'] = df['Close'].shift(2)
    df['lag_3'] = df['Close'].shift(3)

    # Returns
    df['returns'] = df['Close'].pct_change()

    # volatility
    df['volatility'] = df['returns'].rolling(10).std()


    # Target
    df['target'] = df['Close'].shift(-1)

    df.dropna(inplace=True)

    return df


if __name__ == "__main__":
    df = pd.read_csv("data/sample_ohlcv.csv")
    df = create_features(df)

    df.to_csv("data/featured_data.csv", index=False)
    print("Features created!")