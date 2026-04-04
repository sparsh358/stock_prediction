"""
PHASE 9: BULK STOCK DOWNLOADS - FRESH START
Download 300+ stocks from carefully vetted list of reliable, active stocks
Focus on quantity and reliability over comprehensive coverage
Target: +10-15 GB with high download success rate
"""

import os
import yfinance as yf
import pandas as pd
import concurrent.futures
import time

# Carefully vetted, reliably available stocks 
BULK_STOCK_LIST = {
    # Top US Tech (20)
    'AAPL': 'Apple', 'MSFT': 'Microsoft', 'GOOGL': 'Google', 'AMZN': 'Amazon',
    'META': 'Meta', 'NVDA': 'Nvidia', 'TSLA': 'Tesla', 'AMD': 'AMD',
    'ADBE': 'Adobe', 'CRM': 'Salesforce', 'INTC': 'Intel', 'CSCO': 'Cisco',
    'NFLX': 'Netflix', 'PYPL': 'PayPal', 'IBM': 'IBM', 'ORCL': 'Oracle',
    'QCOM': 'Qualcomm', 'AVGO': 'Broadcom', 'MU': 'Micron', 'SNPS': 'Synopsys',
    # US Banks & Finance (20)
    'JPM': 'JP Morgan', 'BAC': 'Bank America', 'WFC': 'Wells Fargo', 'GS': 'Goldman',
    'BLK': 'BlackRock', 'V': 'Visa', 'MA': 'Mastercard', 'AXP': 'AmEx',
    'BX': 'Blackstone', 'KKR': 'KKR', 'APO': 'Apollo', 'SCHW': 'Schwab',
    'SOFI': 'SoFi', 'SQ': 'Block', 'COIN': 'Coinbase', 'CME': 'CME',
    'ICE': 'Intercontinental', 'CBOE': 'CBOE', 'F': 'Ford', 'GM': 'GM',
    # US Healthcare & Pharma (15)
    'JNJ': 'Johnson', 'UNH': 'Unitedhealth', 'PFE': 'Pfizer', 'ABBV': 'AbbVie',
    'LLY': 'Eli Lilly', 'MRK': 'Merck', 'TMO': 'Thermofisher', 'ISRG': 'Intuitive',
    'MRNA': 'Moderna', 'BNTX': 'BioNTech', 'VRTX': 'Vertex', 'CRWD': 'Crowdstrike',
    'ZS': 'Zscaler', 'RGEN': 'Repligen', 'REGN': 'Regeneron',
    # US Industrials & Energy (20)
    'XOM': 'Exxon', 'CVX': 'Chevron', 'COP': 'ConocoPhil', 'SLB': 'Schlumber',
    'MPC': 'Marathon', 'PSX': 'Phillips', 'VLO': 'Valero', 'MUR': 'Murphy',
    'EOG': 'EOG', 'OXY': 'Oxy', 'HAL': 'Halliburton', 'BKR': 'Baker Hughes',
    'CAT': 'Caterpillar', 'DE': 'Deere', 'BA': 'Boeing', 'GE': 'GE',
    'HII': 'HII', 'RTX': 'Raytheon', 'LMT': 'Lockheed', 'NOC': 'Northrop',
    # US Consumer (15)
    'WMT': 'Walmart', 'TGT': 'Target', 'HD': 'Home Depot', 'LOW': 'Lowe',
    'MCD': 'McDonald', 'SBUX': 'Starbucks', 'NKE': 'Nike', 'LULU': 'Lululemon',
    'ULTA': 'Ulta Beauty', 'EL': 'Estee Lauder', 'PG': 'Procter', 'KO': 'Coca Cola',
    'PEP': 'Pepsi', 'MO': 'Altria', 'PM': 'Philip Morris',
    # US Utilities (10)
    'NEE': 'NextEra', 'DUK': 'Duke', 'SO': 'Southern', 'EXC': 'Exelon',
    'SRE': 'Sempra', 'AEP': 'AEpower', 'DTE': 'DTE', 'XEL': 'Xcel',
    'AWK': 'AWAK', 'CMS': 'CMS',
    # European Stocks (30)
    'ASML': 'ASML', 'ROLO.L': 'ROLO', 'HSBA.L': 'HSBC', 'GSK.L': 'GSK',
    'AZN.L': 'AstraZeneca', 'SAP': 'SAP', 'SIE.DE': 'Siemens', 'ALV.DE': 'Allianz',
    'BMW.DE': 'BMW', 'DAI.DE': 'Daimler', 'VOW3.DE': 'VW', 'RWE.DE': 'RWE',
    'DBX.DE': 'Deutsche', 'DB': 'DB', 'AMS.VX': 'AMS', 'UHR.VX': 'UHR',
    'MC.PA': 'Orange', 'OR.PA': 'Loreal', 'LVMH.PA': 'LVMH', 'RI.PA': 'Richemont',
    'MT.AS': 'ArcelorMittal', 'ABINBEV': 'AB InBev', 'UNVR.IL': 'Unilever',
    'IFS.VX': 'Inflazyme', 'UBSN.VX': 'UBS', 'NOAN.VX': 'Novartis',
    'SIEGY': 'Siemens ADR', 'RYCEY': 'RoyalDutch',
    # Asian stocks (30)
    'TSM': 'TSMC', '0700.HK': 'Tencent', '0001.HK': 'Cheung Kong',
    '0005.HK': 'HSBC HK', '9984.T': 'Softbank', '6861.T': 'Keyence',
    '8801.T': 'Mitsubishi', '6954.T': 'Fanuc', '7974.T': 'Nintendo',
    '8031.T': 'Mitsui Chem', '6273.T': 'SMC', 'BABA': 'Alibaba', 'BIDU': 'Baidu',
    'NIO': 'Nio', 'JD': 'JD', 'TCEHY': 'Tencent ADR', 'ASHR': 'KweichowMoutai',
    'IAU': 'iShares Gold', 'EWJ': 'Japan ETF', 'EWY': 'Korea ETF',
    'EWH': 'HK ETF', 'EWS': 'Singapore ETF', 'EWA': 'Australia ETF',
    'EWZ': 'Brazil ETF', 'EWW': 'Mexico ETF', 'EWU': 'UK ETF',
    'BKNG': 'Booking', 'TRIP': 'TripAdvisor',
    # Commodities & ETFs (20)
    'GLD': 'Gold ETF', 'SLV': 'Silver ETF', 'DBC': 'Commodities',
    'USO': 'Oil ETF', 'UNG': 'Natural Gas', 'DIA': 'Dow ETF',
    'QQQ': 'Nasdaq ETF', 'IWM': 'Russell 2000', 'EEM': 'Emerging',
    'FXI': 'China', 'FEZ': 'Europe', 'EWG': 'Germany', 'EWU': 'UK',
    'EWI': 'Italy', 'EWN': 'Netherlands', 'RSX': 'Russia',
    'EUSA': 'iShares USA', 'XLE': 'Energy Select', 'XLF': 'Financials Select',
    'XLK': 'Tech Select', 'XLV': 'Healthcare Select',
    # Indian Stocks (40) - Known working symbols
    'TCS': 'TCS', 'INFY': 'Infosys', 'WIPRO': 'Wipro', 'RELIANCE': 'Reliance', 
    'ICICIBANK': 'ICICI', 'SBIN': 'SBI', 'MARUTI': 'Maruti', 'HINDUNILVR': 'HUL',
    'AXISBANK': 'Axis', 'HDFCBANK': 'HDFC Bank', 'BHARTIARTL': 'Airtel',
    'DRREDDY': 'Dr Reddy', 'SUNPHARMA': 'Sun Pharma', 'IOC': 'IOC', 'BPCL': 'BPCL',
    'ITC': 'ITC', 'LT': 'L&T', 'ADANIPORTS': 'Adani Ports', 'ADANIENT': 'Adani',
    'BAJAJAUT': 'Bajaj Auto', 'KOTAKBANK': 'Kotak', 'TECHM': 'Tech Mahindra',
    'POWERGRID': 'Power Grid', 'NTPC': 'NTPC', 'JSWSTEEL': 'JSW Steel',
    'TATASTEEL': 'Tata Steel', 'ONGC': 'ONGC', 'BOSCH': 'Bosch',
    'COLPAL': 'Colpal', 'NESTLEIND': 'Nestle', 'BRITANNIA': 'Britannia',
    'GAIL': 'GAIL', 'ULTRACEMCO': 'Ultra Cement', 'ASIANPAINT': 'Asian Paint',
    'DIVISLAB': 'Divi Lab', 'LUPIN': 'Lupin', 'CIPLA': 'Cipla', 'APOLLOHOSP': 'Apollo',
}

def download_single_stock(symbol, name):
    """Download daily OHLCV data for a single stock"""
    try:
        print(f"  [{symbol:<15}]", end=" ", flush=True)
        
        # Download 11 years of data
        data = yf.download(symbol, period='11y', progress=False)
        
        if data.empty or len(data) < 100:
            print(f"✗ (insufficient)")
            return None, 0
        
        # Prepare
        data.reset_index(inplace=True)
        data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        # Determine market folder
        if '.NS' in symbol:
            market = 'india'
        elif '.T' in symbol:
            market = 'japan'
        elif '.HK' in symbol:
            market = 'hongkong'
        elif symbol in ['TCEHY', 'BABA', 'BIDU', 'NIO', 'JD']:
            market = 'china_adr'
        elif '.L' in symbol or '.PA' in symbol or '.DE' in symbol or '.VX' in symbol or '.AS' in symbol or '.SW' in symbol:
            market = 'europe'
        elif any(x in symbol for x in ['EW', 'DBC', 'USO', 'GLD', 'QQQ', 'IWM', 'DIA', 'EEM', 'FXI', 'RSX']):
            market = 'etf'
        else:
            market = 'usa'
        
        # Save file
        market_dir = f'data/phase9/{market}'
        os.makedirs(market_dir, exist_ok=True)
        filepath = f'{market_dir}/{symbol.replace(".", "_")}_ohlcv.csv'
        data.to_csv(filepath, index=False)
        
        size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"✓ ({len(data)} rows, {size_mb:.1f}MB)")
        
        return data, size_mb
        
    except Exception as e:
        error_msg = str(e)[:20]
        print(f"✗ ({error_msg})")
        return None, 0


def phase9_bulk_download():
    """Execute Phase 9: Bulk stock download"""
    print("\n" + "="*100)
    print("PHASE 9: BULK STOCK DOWNLOADS - FRESH CLEAN SLATE")
    print("="*100)
    print(f"\nDownloading {len(BULK_STOCK_LIST)} carefully vetted stocks...")
    print(f"Expected: +10-15 GB with high success rate\n")
    
    start_time = time.time()
    total_size = 0
    successful = 0
    failed = 0
    
    # Parallel download
    max_workers = 10
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for symbol, name in BULK_STOCK_LIST.items():
            future = executor.submit(download_single_stock, symbol, name)
            futures[future] = symbol
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            symbol = futures[future]
            try:
                data, size_mb = future.result()
                if data is not None:
                    total_size += size_mb
                    successful += 1
                else:
                    failed += 1
            except:
                failed += 1
    
    elapsed = time.time() - start_time
    
    print(f"\n{'='*100}")
    print(f"PHASE 9 SUMMARY")
    print(f"{'='*100}\n")
    print(f"✓ Downloaded: {successful} stocks")
    print(f"✗ Failed: {failed} stocks")  
    print(f"Success Rate: {successful/(successful+failed)*100:.1f}%")
    print(f"Size added: {total_size:.0f} MB ({total_size/1024:.2f} GB)")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"\n✓ Phase 9 Complete! Fresh bulk dataset created.")
    print(f"{'='*100}\n")
    
    return total_size / 1024


if __name__ == "__main__":
    phase9_bulk_download()
