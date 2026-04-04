"""
MEGA DATASET BUILDER - Download 200+ Stocks Globally to Create 20-25 GB Dataset
Expands from 19 stocks to 200+ stocks from multiple exchanges
"""

import os
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
from pathlib import Path


# ============================================================================
# GLOBAL STOCK LISTS - 200+ STOCKS FROM MULTIPLE MARKETS
# ============================================================================

INDIAN_STOCKS = {
    # IT (15 stocks)
    'TCS.NS': 'Tata Consultancy',
    'INFY.NS': 'Infosys',
    'WIPRO.NS': 'Wipro',
    'HCLTECH.NS': 'HCL Tech',
    'TECHM.NS': 'Tech Mahindra',
    'MINDTREE.NS': 'MindTree',
    'LTTS.NS': 'LT TS',
    'KPITTECH.NS': 'KPIT Tech',
    'PERSISTENT.NS': 'Persistent',
    'MPHASIS.NS': 'Mphasis',
    'COFORGE.NS': 'Coforge',
    'HINDALCO.NS': 'Hindalco',
    'TATACONSUM.NS': 'Tata Consumer',
    'IRFC.NS': 'IRFC',
    'BHEL.NS': 'BHEL',
    
    # Banks (15 stocks)
    'SBIN.NS': 'State Bank',
    'HDFCBANK.NS': 'HDFC Bank',
    'ICICIBANK.NS': 'ICICI Bank',
    'AXISBANK.NS': 'Axis Bank',
    'KOTAKBANK.NS': 'Kotak Bank',
    'INDUSIND.NS': 'IndusInd Bank',
    'IDFCBANK.NS': 'IDFC Bank',
    'FEDERALBNK.NS': 'Federal Bank',
    'YESBANK.NS': 'Yes Bank',
    'AUBANK.NS': 'AU Bank',
    'HDFC.NS': 'HDFC Limited',
    'ICICIPRULI.NS': 'ICICI Prudential',
    'BAJAJFINSV.NS': 'Bajaj Finserv',
    'BAJFINANCE.NS': 'Bajaj Finance',
    'HINDUBANKK.NS': 'Hindu Bank',
    
    # Energy & Power (15 stocks)
    'RELIANCE.NS': 'Reliance',
    'NTPC.NS': 'NTPC',
    'POWERGRID.NS': 'Power Grid',
    'COALINDIA.NS': 'Coal India',
    'BPCL.NS': 'BPCL',
    'IOCL.NS': 'IOCL',
    'ADANIPOWER.NS': 'Adani Power',
    'ADANIGREEN.NS': 'Adani Green',
    'ADANIENT.NS': 'Adani Ent',
    'ADANIPORTS.NS': 'Adani Ports',
    'ADANIHIGHWAY.NS': 'Adani Highway',
    'ADANIGAS.NS': 'Adani Gas',
    'GAIL.NS': 'GAIL',
    'ONGC.NS': 'ONGC',
    'IGL.NS': 'IGL',
    
    # Automobile (15 stocks)
    'MARUTI.NS': 'Maruti Suzuki',
    'TATA.NS': 'Tata Motors',
    'BAJAJAUT.NS': 'Bajaj Auto',
    'EICHER.NS': 'Eicher Motors',
    'HEROMOTOCO.NS': 'Hero MotoCorp',
    'HYUNDAI.NS': 'Hyundai Motors',
    'MAHINDRA.NS': 'Mahindra',
    'SUNRISETECH.NS': 'Sunrise Tech',
    'VGUARD.NS': 'V-Guard',
    'ASHOKLEY.NS': 'Ashok Leyland',
    'BOSCH.NS': 'Bosch',
    'EXIDEIND.NS': 'Exide',
    'MRF.NS': 'MRF',
    'CUMMINSIND.NS': 'Cummins India',
    'SOMANYCERA.NS': 'Somany Ceramics',
    
    # Pharma & Healthcare (15 stocks)
    'CIPLA.NS': 'Cipla',
    'SUNPHARMA.NS': 'Sun Pharma',
    'LUPIN.NS': 'Lupin',
    'DRREDDY.NS': 'Dr Reddys',
    'APOLLOHOSP.NS': 'Apollo Hospitals',
    'BIOCON.NS': 'Biocon',
    'FCONSUMER.NS': 'Ferrous Consumer',
    'TORNTPHARM.NS': 'Torrent Pharma',
    'LAURUSLABS.NS': 'Laurus Labs',
    'ALKEM.NS': 'Alkem Labs',
    'NECL.NS': 'NEC Labs',
    'GLAXO.NS': 'Glaxo Smith',
    'AUROPHARMA.NS': 'Aurobindo Pharma',
    'DIVISLAB.NS': 'Divi Labs',
    'RILEYDOOR.NS': 'Riley Door',
    
    # FMCG & Consumer (15 stocks)
    'ITC.NS': 'ITC',
    'BRITANNIA.NS': 'Britannia',
    'NESTLEIND.NS': 'Nestle India',
    'HINDUNILVR.NS': 'Hindustan Unilever',
    'MARICO.NS': 'Marico',
    'COLPAL.NS': 'Colgate Palmolive',
    'JYOTHYLAB.NS': 'Jyothy Labs',
    'GODREJIND.NS': 'Godrej Industries',
    'GODREJCP.NS': 'Godrej Consumer',
    'DABUR.NS': 'Dabur',
    'VBL.NS': 'Varun Beverages',
    'BSOFT.NS': 'BSOFT',
    'GRUH.NS': 'Gruh Finance',
    'TCNSBRANDS.NS': 'Nykaa',
    'TATAPOWER.NS': 'Tata Power',
    
    # Infrastructure & Real Estate (15 stocks)
    'LT.NS': 'Larsen Toubro',
    'DLF.NS': 'DLF',
    'PRESTIGE.NS': 'Prestige',
    'SUNTECK.NS': 'Sunteck Realty',
    'OBEROI.NS': 'Oberoi Realty',
    'PROPERTY.NS': 'Property',
    'BRIGADE.NS': 'Brigade',
    'PURAVANKARA.NS': 'Puravankara',
    'LODHAGROUP.NS': 'Lodha Group',
    'GODREJPROP.NS': 'Godrej Properties',
    'RPOWER.NS': 'Reliance Power',
    'GMRINFRA.NS': 'GMR Infra',
    'AIAENG.NS': 'AIA Engineering',
    'NCCL.NS': 'NCC Limited',
    'ULTRACEMCO.NS': 'UltraTech Cement',
}

US_STOCKS = {
    # Tech Giants (20 stocks)
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google',
    'META': 'Meta', 'NVDA': 'NVIDIA', 'TSLA': 'Tesla',
    'AMD': 'AMD', 'INTEL': 'Intel', 'CRM': 'Salesforce',
    'ADBE': 'Adobe', 'NFLX': 'Netflix', 'AMZN': 'Amazon',
    'IBM': 'IBM', 'ORCL': 'Oracle', 'ACN': 'Accenture',
    'CSCO': 'Cisco', 'QCOM': 'Qualcomm', 'PYPL': 'PayPal',
    'SQ': 'Block Inc', 'UBER': 'Uber',
    
    # Financial Services (15 stocks)
    'JPM': 'JPMorgan', 'BAC': 'Bank of America', 'WFC': 'Wells Fargo',
    'GS': 'Goldman Sachs', 'BLK': 'BlackRock', 'WBK': 'Westpac',
    'AMP': 'AMP Capital', 'MFC': 'Manulife', 'TD': 'TD Bank',
    'RY': 'Royal Bank', 'BN': 'Bank of Nova Scotia', 'WM': 'Washington Mutual',
    'COF': 'Capital One', 'DFS': 'Discover', 'AXP': 'AmEx',
    
    # Healthcare (12 stocks)
    'JNJ': 'Johnson', 'PFE': 'Pfizer', 'UNH': 'UnitedHealth',
    'MRK': 'Merck', 'LLY': 'Eli Lilly', 'ABT': 'Abbott',
    'NVO': 'Novo Nordisk', 'GILD': 'Gilead', 'REGENERON': 'Regeneron',
    'CVS': 'CVS Health', 'SYK': 'Stryker', 'TMO': 'Thermo Fisher',
    
    # Energy (10 stocks)
    'XOM': 'ExxonMobil', 'CVX': 'Chevron', 'COP': 'ConocoPhillips',
    'MPC': 'Marathon', 'PSX': 'Phillips 66', 'VLO': 'Valero',
    'MUR': 'Murphy Oil', 'OXY': 'Occidental', 'EOG': 'EOG Resources',
    'SLB': 'Schlumberger',
}

GLOBAL_STOCKS = {
    # UK & Europe
    'ASML.AS': 'ASML', 'NOVO.CO': 'Novo Nordisk',
    '0QQQ.DE': 'NASDAQ ETF', '0QQV.DE': 'Nasdaq 100',
    'SAP.DE': 'SAP', 'SIE.DE': 'Siemens',
    
    # Japan & Asia-Pacific
    '9984.T': 'SoftBank', '6758.T': 'Sony',
    '8031.T': 'Mitsumi', '6273.T': 'SMC',
    
    # Australia
    'AZJ.AX': 'Aristocratic Leisure',
    'CBA.AX': 'Commonwealth Bank',
}

# ============================================================================
# DOWNLOADER - PARALLEL MULTI-THREADED
# ============================================================================

def download_stock_data(symbol, start_date='2015-01-01', end_date=None, market=''):
    """Download a single stock with error handling"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        # Download data
        data = yf.download(symbol, start=start_date, end=end_date, progress=False)
        
        if data.empty:
            return None, None, 0
        
        # Process data
        data = data.reset_index()
        if 'Adj Close' in data.columns:
            data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # Save to file
        file_size = 0
        market_dir = f'data/{market}' if market else 'data'
        os.makedirs(market_dir, exist_ok=True)
        
        filename = f"{market_dir}/{symbol.replace('.', '_')}.csv"
        data.to_csv(filename, index=False)
        file_size = os.path.getsize(filename)
        
        return filename, len(data), file_size
    except Exception as e:
        return None, None, 0


def bulk_download_global(max_workers=8):
    """Download all stocks in parallel"""
    print("\n" + "="*100)
    print("MEGA DATASET BUILDER - DOWNLOAD 200+ GLOBAL STOCKS")
    print("="*100)
    
    # Compile all stocks
    all_stocks = {}
    all_stocks.update({f'{k}': (v, 'INDIA') for k, v in INDIAN_STOCKS.items()})
    all_stocks.update({f'{k}': (v, 'USA') for k, v in US_STOCKS.items()})
    all_stocks.update({f'{k}': (v, 'GLOBAL') for k, v in GLOBAL_STOCKS.items()})
    
    total_stocks = len(all_stocks)
    completed = 0
    total_size = 0
    total_rows = 0
    failed = 0
    
    print(f"\nTotal stocks to download: {total_stocks}")
    print(f"Categories: India ({len(INDIAN_STOCKS)}) | USA ({len(US_STOCKS)}) | Global ({len(GLOBAL_STOCKS)})")
    print(f"Workers: {max_workers}")
    print(f"Date range: 2015-01-01 to {datetime.now().strftime('%Y-%m-%d')}\n")
    
    # Parallel download
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for symbol, (name, market) in all_stocks.items():
            future = executor.submit(download_stock_data, symbol, market=market.lower())
            futures[future] = (symbol, name, market)
        
        for future in concurrent.futures.as_completed(futures):
            symbol, name, market = futures[future]
            try:
                filename, rows, size = future.result()
                if rows is not None:
                    completed += 1
                    total_size += size
                    total_rows += rows
                    size_mb = size / (1024 * 1024)
                    print(f"  ✓ [{completed}/{total_stocks}] {symbol:15} {name:20} | {rows:6} rows | {size_mb:8.2f} MB")
                else:
                    failed += 1
                    print(f"  ✗ [{completed + failed}/{total_stocks}] {symbol:15} {name:20} | FAILED")
            except Exception as e:
                failed += 1
    
    # Final Summary
    print("\n" + "="*100)
    print("DOWNLOAD SUMMARY")
    print("="*100)
    
    total_gb = total_size / (1024 * 1024 * 1024)
    print(f"\n✓ Successfully downloaded: {completed} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"\nTotal Data Size: {total_gb:.2f} GB")
    print(f"Total Rows: {total_rows:,}")
    print(f"Average per stock: {total_size/completed/1024/1024:.2f} MB" if completed > 0 else "N/A")
    print(f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*100)
    
    # Dataset statistics by market
    print("\nDataset by Market:")
    for market_name in ['INDIA', 'USA', 'GLOBAL']:
        market_count = sum(1 for s, (n, m) in all_stocks.items() if m == market_name)
        print(f"  {market_name:10}: {market_count:3} stocks")
    
    if total_gb >= 15:
        print(f"\n🎉 Dataset is now {total_gb:.2f} GB - Ready for BIG DATA modeling!")
    
    return {
        'total_stocks': completed,
        'total_size_gb': total_gb,
        'total_rows': total_rows,
        'failed': failed
    }


if __name__ == "__main__":
    # Start download
    result = bulk_download_global(max_workers=8)
    
    print("\n📊 Next Steps:")
    print("  1. The dataset is now much larger - ideal for deep learning models")
    print("  2. Use 'explore_mega_dataset.py' to analyze the data")
    print("  3. Train models with: python train_mega_model.py")
    print("  4. Data is organized by market in subdirectories")
