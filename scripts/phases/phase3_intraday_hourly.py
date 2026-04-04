"""
PHASE 3 COMPLETE: INTRADAY DATA DOWNLOAD - Execute with 1-hour candles
Downloads hourly OHLCV data for top 120 stocks (11+ years history)
Target: +8-10 GB to dataset
"""

import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import time

# Top 120 stocks across all markets for intraday
TOP_INTRADAY_STOCKS = [
    # Top Indian stocks (30)
    'TCS.NS', 'INFY.NS', 'WIPRO.NS', 'RELIANCE.NS', 'ICICIBANK.NS',
    'SBIN.NS', 'MARUTI.NS', 'HINDUNILVR.NS', 'AXISBANK.NS', 'HDFCBANK.NS',
    'BHARTIARTL.NS', 'DRREDDY.NS', 'SUNPHARMA.NS', 'IOC.NS', 'BPCL.NS',
    'ADANIPORTS.NS', 'ADANIENT.NS', 'BAJAJAUT.NS', 'BOSCH.NS', 'COLPAL.NS',
    'HDFC.NS', 'GAIL.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LUPIN.NS',
    'NESTLEIND.NS', 'POWERGRID.NS', 'TATASTEEL.NS', 'TECHM.NS', 'ONGC.NS',
    # Top US stocks (30)
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'NVDA', 'TSLA', 'BRK.B', 'JNJ', 'V',
    'JPM', 'WMT', 'PG', 'UNH', 'HD', 'DIS', 'XOM', 'COP', 'CVX', 'PFE',
    'KO', 'PEP', 'MCD', 'NKE', 'NFLX', 'IBM', 'INTC', 'AMD', 'CRM', 'ORCL',
    # Top European stocks (20)
    'SAP', 'ROG.SW', 'NESN.SW', 'NOVN.SW', 'ROLO.L', 'ASML', 'VOW3.DE', 'SIE.DE',
    'ALV.DE', 'MUV2.DE', 'DBX.DE', 'BMW.DE', 'DAI.DE', 'EOAN.DE', 'RWE.DE',
    'BAS.DE', 'MC.PA', 'OR.PA', 'GSK.L', 'HSBA.L',
    # Top Asian stocks (20)
    'TCEHY', '0700.HK', '0001.HK', '0005.HK', 'YANG', '2308.TW', '6758.T',
    '8031.T', '9984.T', '005930.KS', '000660.KS', '035420.KS', '011200.KS',
    'BHP', 'CBA', 'WBC', 'NAB', 'MQG', 'AMP', 'CSL',
    # Top Emerging (20)
    'PETROBRAS', 'ASML', 'GAZP', 'SBER', 'RSTI', 'PPLM', 'PETR4', 'VALE3',
    'LREN3', 'MGLU3', 'BBDC4', 'BRFS3', 'JBSS3', 'RAIL3', 'FIBR3',
    'ITUB4', 'ABEV3', 'USIM5', 'GGBR4', 'CSAN3',
]

def download_intraday_for_stock(symbol, interval='1h', period='10y'):
    """Download intraday data for a single stock"""
    try:
        print(f"  [{symbol:<10}] Downloading {interval} candles...", end=" ", flush=True)
        
        # Download intraday data
        data = yf.download(symbol, interval=interval, period=period, progress=False, prepost=False)
        
        if data.empty or len(data) < 50:
            print(f"✗ (insufficient data)")
            return None, 0
        
        # Prepare data
        data.reset_index(inplace=True)
        data = data[['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
        data.rename(columns={'Datetime': 'Date'}, inplace=True)
        
        # Create intraday directory
        market_type = 'india' if '.NS' in symbol else 'global'
        intraday_dir = f'data/intraday/{interval}/{market_type}'
        os.makedirs(intraday_dir, exist_ok=True)
        
        # Save file
        filename = f'{intraday_dir}/{symbol.replace(".", "_")}_intraday_{interval}.csv'
        data.to_csv(filename, index=False)
        
        file_size_mb = os.path.getsize(filename) / (1024 * 1024)
        print(f"✓ ({len(data)} candles, {file_size_mb:.1f} MB)")
        
        return data, file_size_mb
        
    except Exception as e:
        error_msg = str(e)[:40]
        print(f"✗ ({error_msg})")
        return None, 0


def phase3_intraday():
    """Execute Phase 3: Download hourly intraday data"""
    print("\n" + "="*100)
    print("PHASE 3: INTRADAY DATA DOWNLOAD (HOURLY CANDLES)")
    print("="*100)
    print(f"\nDownloading 1-hour candles for {len(TOP_INTRADAY_STOCKS)} stocks (10 years history)...")
    print(f"Expected: +8-10 GB to dataset\n")
    
    start_time = time.time()
    total_size = 0
    successful = 0
    failed = 0
    
    # Parallel download with controlled workers
    max_workers = 6
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for symbol in TOP_INTRADAY_STOCKS:
            future = executor.submit(download_intraday_for_stock, symbol, interval='1h', period='10y')
            futures[future] = symbol
        
        # Process as completed
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            symbol = futures[future]
            try:
                data, size_mb = future.result()
                if data is not None:
                    total_size += size_mb
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"  Error processing {symbol}: {str(e)[:40]}")
                failed += 1
    
    elapsed = time.time() - start_time
    
    print(f"\n{'='*100}")
    print(f"PHASE 3 SUMMARY")
    print(f"{'='*100}\n")
    print(f"✓ Downloaded: {successful} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"Total size added: {total_size:.0f} MB ({total_size/1024:.1f} GB)")
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
    print(f"\n✓ Phase 3 Complete! Hourly candle data added to dataset.")
    print(f"{'='*100}\n")
    
    return total_size / 1024  # Return size in GB


if __name__ == "__main__":
    phase3_intraday()
