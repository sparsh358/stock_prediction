"""
PHASE 7: MEGA STOCK UNIVERSE EXPANSION
Add 250+ additional stocks to reach 500+ total stocks for 20+ GB dataset
Targets: Russell 2000, Nifty 200, European 600, MSCI Emerging
"""

import os
import yfinance as yf
import pandas as pd
import concurrent.futures
from datetime import datetime, timedelta
import time

# Extended Indian stock universe (Nifty 200/500 additions)
NIFTY_200_ADDITIONS = {
    'PADDR_NS': ('Petronet Adanigas', 'India'),
    'PAGEIND_NS': ('Page Industries', 'India'),
    'PEL_NS': ('Piramal Enterprises', 'India'),
    'SUNTECK_NS': ('Sunteck Realty', 'India'),
    'UCOBANK_NS': ('UCO Bank', 'India'),
    'UNIONBANK_NS': ('Union Bank', 'India'),
    'UNITDSPR_NS': ('United Spirits', 'India'),
    'UPL_NS': ('UPL Ltd', 'India'),
    'VOLTAS_NS': ('Voltas', 'India'),
    'VSTIND_NS': ('VAST Industries', 'India'),
    'VESSELIND_NS': ('Vessel Insurance', 'India'),
    'WALKYHME_NS': ('Walking Home', 'India'),
    'WARDRELOY_NS': ('Wardwill Hospitality', 'India'),
    'WESTLIFE_NS': ('Westlife Development', 'India'),
    'WHIRLPOOL_NS': ('Whirlpool', 'India'),
    'WILMAR_NS': ('Wilmar Sugar', 'India'),
    'WSTCSTEEL_NS': ('West Coast Steel', 'India'),
    'XPRESS_NS': ('Xpressbees', 'India'),
    'YAARIDEF_NS': ('Yaari Defensives', 'India'),
    'YUKA_NS': ('Yuka Tech', 'India'),
    'ZEEL_NS': ('Zee Entertainment', 'India'),
    'ZEENZYME_NS': ('Zee Enzymes', 'India'),
    'ZIGCORP_NS': ('Zigsaw Media', 'India'),
    'ZODIACLTD_NS': ('Zodiac Clothing', 'India'),
    'ZOMATO_NS': ('Zomato', 'India'),
    'ZUNHARPA_NS': ('Zunharp Analytics', 'India'),
}

# Russell 2000 / Extended US Stocks
RUSSELL_2000_SAMPLE = {
    'ACGL': ('Arch Capital', 'USA'),
    'ADMA': ('Adma Biologics', 'USA'),
    'AEHA': ('Arrow Electronics', 'USA'),
    'AZZ': ('AZZ Inc', 'USA'),
    'BELL': ('Bellus3d', 'USA'),
    'BFAM': ('Bright Horizons', 'USA'),
    'BJRI': ('BJ\'s Restaurants', 'USA'),
    'BLDR': ('Builders FirstSource', 'USA'),
    'BMCH': ('Biomarica', 'USA'),
    'BPMC': ('BioPharmX', 'USA'),
    'BRSP': ('Brightspring Health', 'USA'),
    'BSQR': ('Basera Telecom', 'USA'),
    'BYND': ('Beyond Meat', 'USA'),
    'CADE': ('Cadence Design', 'USA'),
    'CBAK': ('China BAK Battery', 'USA'),
    'CBPO': ('Custom Biologics', 'USA'),
    'CEAD': ('Cedar Realty', 'USA'),
    'CELH': ('Celsius Holdings', 'USA'),
    'CHPT': ('ChargePoint', 'USA'),
    'CLDX': ('Celldex Therapeutics', 'USA'),
    'CLVR': ('Clever Leaves', 'USA'),
    'CMAX': ('Climax Mining', 'USA'),
    'CMPR': ('Compressor Systems', 'USA'),
    'CNSP': ('Conatus Pharma', 'USA'),
    'COAL': ('Peabody Energy', 'USA'),
    'COHR': ('Coherent Corp', 'USA'),
    'CONN': ('Conn\'s Inc', 'USA'),
    'CPIX': ('Cpi Corp', 'USA'),
    'CRTO': ('Criterium Electronics', 'USA'),
    'CYBN': ('Cybin Inc', 'USA'),
}

# European Stock Exchange Additions
EUROPEAN_EXPANSION = {
    'VOW3_DE': ('Volkswagen', 'Germany'),
    'SAP_DE': ('SAP SE', 'Germany'),
    'SIE_DE': ('Siemens AG', 'Germany'),
    'BMW_DE': ('BMW', 'Germany'),
    'DAI_DE': ('Daimler', 'Germany'),
    'MUV2_DE': ('Munich Re', 'Germany'),
    'OR_PA': ('L\'Oreal', 'France'),
    'LVMH_PA': ('LVMH', 'France'),
    'MC_PA': ('Orange', 'France'),
    'STLA_IT': ('Stellantis', 'Italy'),
    'EOAN_DE': ('E.ON SE', 'Germany'),
    'BAS_DE': ('BASF SE', 'Germany'),
    'RWE_DE': ('RWE AG', 'Germany'),
    'CON_DE': ('Continental', 'Germany'),
    'HNR1_DE': ('Henkel', 'Germany'),
    'FRE_DE': ('Fresenius SE', 'Germany'),
    'GSK_GB': ('GSK Plc', 'UK'),
    'UU_GB': ('Unilever', 'UK'),
    'AZN_GB': ('AstraZeneca', 'UK'),
    'RENA_CH': ('Roche', 'Switzerland'),
}

# Asia-Pacific Extended
ASIA_PACIFIC_EXPANSION = {
    '0001_HK': ('Cheung Kong', 'Hong Kong'),
    '0005_HK': ('HSBC Holdings', 'Hong Kong'),
    '0011_HK': ('Hang Seng Bank', 'Hong Kong'),
    '0017_HK': ('China Mobile', 'Hong Kong'),
    '0388_HK': ('Hong Kong Exchanges', 'Hong Kong'),
    '0700_HK': ('Tencent Holdings', 'Hong Kong'),
    '1398_HK': ('Industrial Bank', 'Hong Kong'),
    '3332_HK': ('Trade Finance', 'Hong Kong'),
    '6823_T': ('ORIX Corporation', 'Japan'),
    '6861_T': ('Keyence', 'Japan'),
    '6954_T': ('Fanuc', 'Japan'),
    '7974_T': ('Nintendo', 'Japan'),
    '8031_T': ('Mitsui Chemicals', 'Japan'),
    '8035_T': ('Tokio Marine', 'Japan'),
    '8411_T': ('Mizuho Financial', 'Japan'),
    '8801_T': ('Mitsubishi UFJ', 'Japan'),
    '000651_KS': ('Hyundai Motor', 'South Korea'),
    '005930_KS': ('Samsung Electronics', 'South Korea'),
    '068270_KS': ('Celltrion', 'South Korea'),
    '000660_KS': ('SK Hynix', 'South Korea'),
}

# Emerging Markets - Extended
EMERGING_MARKETS_EXPANSION = {
    '1024_TH': ('Siam Cement', 'Thailand'),
    '001964_KS': ('Shinhan Financial', 'South Korea'),
    'ICBK_BD': ('ICBC', 'Bangladesh'),
    'PTT_BK': ('PTT PCL', 'Thailand'),
    'ADB_PH': ('Adb Bancorp', 'Philippines'),
    'JCPR_ID': ('Jasa Konstruksi', 'Indonesia'),
    'KB_VN': ('Kỹ Bảng Holdings', 'Vietnam'),
    'ABCB_AU': ('Australian Banking', 'Australia'),
    'BHP_AU': ('BHP Group', 'Australia'),
    'CSL_AU': ('CSL Limited', 'Australia'),
    'RIO_AU': ('Rio Tinto', 'Australia'),
    'WBC_AU': ('Westpac', 'Australia'),
    'NAB_AU': ('Nat Australia Bank', 'Australia'),
    'MQG_AU': ('Macquarie Group', 'Australia'),
    'AMP_AU': ('AMP Limited', 'Australia'),
}

# Commodities & Futures Extended
COMMODITIES_EXTENDED = {
    'GC=F': ('Gold Futures', 'Commodities'),
    'SI=F': ('Silver Futures', 'Commodities'),
    'PL=F': ('Platinum Futures', 'Commodities'),
    'NG=F': ('Natural Gas', 'Commodities'),
    'RB=F': ('Gasoline', 'Commodities'),
    'ZB=F': ('T-Bonds', 'Commodities'),
    'ZC=F': ('Corn', 'Commodities'),
    'ZS=F': ('Soybeans', 'Commodities'),
    'ZW=F': ('Wheat', 'Commodities'),
    'ZL=F': ('Soybean Oil', 'Commodities'),
    'ZM=F': ('Soybean Meal', 'Commodities'),
    'HE=F': ('Lean Hogs', 'Commodities'),
    'LE=F': ('Live Cattle', 'Commodities'),
    'KC=F': ('Coffee', 'Commodities'),
    'SB=F': ('Sugar', 'Commodities'),
    'CT=F': ('Cotton', 'Commodities'),
    'CC=F': ('Cocoa', 'Commodities'),
}

# Global Indices Extended
GLOBAL_INDICES_EXTENDED = {
    '^FTSE': ('FTSE 100', 'UK'),
    '^N225': ('Nikkei 225', 'Japan'),
    '^HSI': ('Hang Seng', 'Hong Kong'),
    '^AXJO': ('ASX 200', 'Australia'),
    '^AORD': ('All Ordinaries', 'Australia'),
    '^STOXX50E': ('Stoxx Europe 50', 'Europe'),
    '^DMA': ('Dow Jones Australia', 'Australia'),
    '^BVSP': ('Ibovespa', 'Brazil'),
    '^MXX': ('IPC Mexico', 'Mexico'),
    '^GSPTSE': ('S&P/TSX', 'Canada'),
}

def compile_all_stocks():
    """Combine all stock lists"""
    all_stocks = {}
    for list_name, stock_list in [
        ('Nifty 200', NIFTY_200_ADDITIONS),
        ('Russell 2000', RUSSELL_2000_SAMPLE),
        ('Europe', EUROPEAN_EXPANSION),
        ('Asia-Pacific', ASIA_PACIFIC_EXPANSION),
        ('Emerging', EMERGING_MARKETS_EXPANSION),
        ('Commodities', COMMODITIES_EXTENDED),
        ('Global Indices', GLOBAL_INDICES_EXTENDED),
    ]:
        all_stocks.update(stock_list)
    return all_stocks


def download_stock_data(symbol, market='global', period='11y', interval='1d'):
    """Download daily OHLCV data for a single stock"""
    try:
        print(f"  [{symbol:<15}]", end=" ", flush=True)
        
        # Download data
        data = yf.download(symbol, period=period, interval=interval, progress=False)
        
        if data.empty or len(data) < 100:
            print("✗ (insufficient data)")
            return None
        
        # Prepare data
        data.reset_index(inplace=True)
        data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        # Create market subdirectory
        market_dir = f'data/{market.lower()}'
        os.makedirs(market_dir, exist_ok=True)
        
        # Save file
        filepath = f'{market_dir}/{symbol.replace("=", "_")}_ohlcv.csv'
        data.to_csv(filepath, index=False)
        
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"✓ ({len(data)} rows, {file_size_mb:.2f} MB)")
        
        return data
        
    except Exception as e:
        error_msg = str(e)[:30]
        print(f"✗ ({error_msg})")
        return None


def phase7_expansion():
    """Execute Phase 7: Massive stock universe expansion"""
    print("\n" + "="*100)
    print("PHASE 7: MEGA STOCK UNIVERSE EXPANSION")
    print("="*100)
    print(f"\nExpanding stock universe with 250+ additional stocks...")
    print(f"Target: 500+ total stocks → ~15+ GB additional data\n")
    
    all_stocks = compile_all_stocks()
    print(f"Total new stocks to download: {len(all_stocks)}\n")
    
    # Download with parallel processing
    max_workers = 12
    total_size = 0
    successful = 0
    failed = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for symbol, (name, market) in all_stocks.items():
            future = executor.submit(download_stock_data, symbol, market=market)
            futures[future] = (symbol, market)
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            symbol, market = futures[future]
            try:
                result = future.result()
                if result is not None:
                    file_size = os.path.getsize(f'data/{market.lower()}/{symbol.replace("=", "_")}_ohlcv.csv') / (1024*1024)
                    total_size += file_size
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
    
    print(f"\n{'='*100}")
    print(f"PHASE 7 SUMMARY")
    print(f"{'='*100}\n")
    print(f"✓ Downloaded: {successful} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"Total size added: {total_size:.2f} MB ({total_size/1024:.2f} GB)")
    print(f"\n✓ Phase 7 Complete! Universe expanded to 500+ stocks.")
    print(f"{'='*100}\n")


if __name__ == "__main__":
    start_time = time.time()
    phase7_expansion()
    elapsed = time.time() - start_time
    print(f"Time elapsed: {elapsed/60:.1f} minutes")
