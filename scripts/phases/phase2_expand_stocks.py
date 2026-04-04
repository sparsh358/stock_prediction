"""
PHASE 2: EXPAND TO 300+ GLOBAL STOCKS
Adding emerging markets, more Indian stocks, commodities, and indices
"""

import os
import yfinance as yf
import pandas as pd
from datetime import datetime
import concurrent.futures


# ============================================================================
# EXPANDED GLOBAL STOCK LISTS - 300+ STOCKS
# ============================================================================

ADDITIONAL_INDIAN_STOCKS = {
    # IT & Tech (20 more)
    'KPITTECH.NS': 'KPIT Tech', 'PERSISTENT.NS': 'Persistent', 'LTTS.NS': 'LT TS',
    'EMAMILTD.NS': 'Emami', 'SONATSOFTW.NS': 'Sonata Software', 'INFIBEAM.NS': 'Infibeam',
    'HCLTECH.NS': 'HCL Tech', 'L&TFH.NS': 'L&T Finance', 'PDINDIA.NS': 'PDI',
    'MYWISH.NS': 'My Wish', 'TATATECH.NS': 'Tata Tech', 'CCOMPRES.NS': 'Compressor',
    'SEGMENT.NS': 'Segment', 'ITTINFRA.NS': 'ITT Infra', 'MINDHOUSE.NS': 'Mind House',
    'DIGISPICE.NS': 'DigiSpice', 'ZENSARTECH.NS': 'Zensar Tech', 'QWEST.NS': 'Qwest',
    'MYPHARMA.NS': 'My Pharma', 'ONMOBILE.NS': 'OnMobile',
    
    # Pharma & Healthcare (25 more)
    'FCONSUMER.NS': 'FC Healthcare', 'NECL.NS': 'NEC Labs', 'JKL.NS': 'JK Lakshmi',
    'ERIS.NS': 'Eris', 'PAXLABS.NS': 'Paxlabs', 'CTL.NS': 'Ctl', 'LSOMED.NS': 'LS Med',
    'VCCV.NS': 'VCC', 'PANACEA.NS': 'Panacea', 'APOLLOTYRE.NS': 'Apollo Tyre',
    'JMFINANCIAL.NS': 'JM Financial', 'NETWORK18.NS': 'Network 18', 'RELIGARE.NS': 'Religare',
    'ABAN.NS': 'Aban', 'SUDARSHAN.NS': 'Sudarshan', 'ANUP.NS': 'Anup',
    'ALLSEC.NS': 'All Sec', 'SUNDRMSFLM.NS': 'Sundram Film', 'GSLTECH.NS': 'GSL Tech',
    'ARVINDFARM.NS': 'Arvind', 'MAHLOG.NS': 'Mahlog', 'NHPC.NS': 'NHPC',
    'WABCOINDIA.NS': 'WAB', 'MEDPLUS.NS': 'Med Plus', 'XPRO.NS': 'Xpro',
    
    # FMCG & Consumer (20 more)
    'VARADHAC.NS': 'Varadha', 'COSMOFFIN.NS': 'Cosmo', 'COCHINSHIP.NS': 'Cochin',
    'INDIGO.NS': 'IndiGo', 'SPICEJET.NS': 'SpiceJet', 'GO-AIR.NS': 'Go Air',
    'PAYTM.NS': 'Paytm', 'ZOMATO.NS': 'Zomato', 'IRCTC.NS': 'IRCTC',
    'VAHAN.NS': 'Vahan', 'VIRUSONLINE.NS': 'Virus', 'JIYASAVING.NS': 'Jiya',
    'INOXWIND.NS': 'Inox Wind', 'NEWTECH.NS': 'New Tech', 'VISTRA.NS': 'Vistra',
    'NEWSHAPE.NS': 'New Shape', 'CHHIMANLAL.NS': 'Chiml', 'GMMINDIA.NS': 'GMM',
    'ISCOINDIA.NS': 'ISCI', 'SHILPIT.NS': 'Shilpit',
    
    # Banks & Finance (15 more)
    'BANDHANBNK.NS': 'Bandhan Bank', 'RMHBANK.NS': 'RMH Bank', 'AIRTELPAYMTS.NS': 'Airtel Payments',
    'BAJAJFINSV.NS': 'Bajaj Finserv', 'BAJFINANCE.NS': 'Bajaj Finance', 'ICICIMF.NS': 'ICICI MF',
    'ABCDL.NS': 'ABCDL', 'SUNDRMF.NS': 'Sundrum', 'HSBAKINDIA.NS': 'HSBC',
    'CITIBANK.NS': 'Citi Bank', 'DEUTSCHBA.NS': 'Deutsche', 'SBIIN.NS': 'SBI',
    'INDIACC.NS': 'IndiaCC', 'ICCBANK.NS': 'ICCB', 'FBL.NS': 'FBL',
    
    # Auto (15 more)
    'TATAMOTORS.NS': 'Tata Motors', 'HYUNDAI.NS': 'Hyundai', 'EICHER.NS': 'Eicher',
    'VGUARD.NS': 'V-Guard', 'ASHOKLEY.NS': 'Ashok Leyland', 'APOLLOHOSP.NS': 'Apollo',
    'TORNTPHARM.NS': 'Torrent', 'BOSCH.NS': 'Bosch', 'CUMMINSIND.NS': 'Cummins',
    'MRF.NS': 'MRF', 'EXIDEIND.NS': 'Exide', 'SUNRISETECH.NS': 'Sunrise',
    'SOMANYCERA.NS': 'Somany', 'GOLDTECH.NS': 'Gold Tech', 'GODREG.NS': 'Godrej',
    
    # Energy & Power (15 more)
    'JSWSTEEL.NS': 'JSW Steel', 'TATASTEEL.NS': 'Tata Steel', 'HINDALCO.NS': 'Hindalco',
    'GAIL.NS': 'GAIL', 'IGL.NS': 'IGL', 'NTPC.NS': 'NTPC', 'POWERGRID.NS': 'Power Grid',
    'COALINDIA.NS': 'Coal India', 'RPOWER.NS': 'Reliance Power', 'TATAPOWER.NS': 'Tata Power',
    'JSWENERGY.NS': 'JSW Energy', 'ADANIPOWER.NS': 'Adani Power', 'ADANIGREEN.NS': 'Adani Green',
    'ADANIGAS.NS': 'Adani Gas', 'ADANIPORTS.NS': 'Adani Ports', 'ADANIENT.NS': 'Adani Ent',
    
    # Infrastructure & Real Estate (15 more)
    'DLF.NS': 'DLF', 'PRESTIGE.NS': 'Prestige', 'OBEROIREALTY.NS': 'Oberoi',
    'LODHAGROUP.NS': 'Lodha', 'GODREJPROP.NS': 'Godrej Prop', 'SUNTECK.NS': 'Sunteck',
    'BRIGADE.NS': 'Brigade', 'AIAENG.NS': 'AIA Eng', 'GMRINFRA.NS': 'GMR Infra',
    'IRFC.NS': 'IRFC', 'NCCL.NS': 'NCC', 'ULTRACEMCO.NS': 'UltraTech Cement',
    'SHREECEM.NS': 'Shree Cement', 'JKCEMENT.NS': 'JK Cement', 'AMBUJA.NS': 'Ambuja',
}

EMERGING_MARKETS = {
    # Brazil (IBOVESPA)
    'PETR4.SA': 'Petrobras', 'VALE3.SA': 'Vale', 'ITUB4.SA': 'Itau Unibanco',
    'BBDC4.SA': 'Banco Bradesco', 'UGPA3.SA': 'Ultrapar', 'MGLU3.SA': 'Magalu',
    'LREN3.SA': 'Lojas Renner', 'RADL3.SA': 'Radial', 'MOVI3.SA': 'Movida',
    'WEGE3.SA': 'WEG',
    
    # Russia (MOEX)
    'GAZP.ME': 'Gazprom', 'SBER.ME': 'Sberbank', 'VTBR.ME': 'VTB',
    'TATN.ME': 'Tatneft', 'NLMK.ME': 'NLMK', 'GMKN.ME': 'Poly Metals',
    'MTSS.ME': 'MTS', 'ROLO.ME': 'Rosneft',
    
    # Mexico (BMV)
    'GCARSO.MX': 'Grupo Carso', 'WALMEX.MX': 'Walmart Mexico',
    'ALPEK.MX': 'Alpek', 'GRUMA.MX': 'Gruma',
    
    # Thailand (SET)
    'ADVANC.BK': 'Advanced Info', 'PTT.BK': 'Thai Oil', 'CPALL.BK': 'CP All',
    
    # Indonesia (IDX)
    'BBCA.JK': 'Bank Central Asia', 'BMRI.JK': 'Bank Mandiri',
    
    # Philippines (PSE)
    'MBT.PH': 'Mabuhay Telecom', 'AEV.PH': 'Alliance Ventures',
}

COMMODITIES_FUTURES = {
    'GC=F': 'Gold', 'CL=F': 'Crude Oil (WTI)', 'NG=F': 'Natural Gas',
    'HG=F': 'Copper', 'SI=F': 'Silver', 'ZS=F': 'Soybeans',
    'ZW=F': 'Wheat', 'ZC=F': 'Corn', 'CO=F': 'Cocoa',
    'CT=F': 'Cotton', 'DXY=F': 'Dollar Index', 'TRX=F': 'Treasury Bond',
}

GLOBAL_INDICES = {
    '^GSPC': 'S&P 500', '^IXIC': 'NASDAQ', '^DJI': 'Dow Jones',
    '^NSEI': 'Nifty 50', '^BSESN': 'Sensex', '^NSEOIT': 'Nifty IT',
    '^NSEBANK': 'Nifty Bank', '^FTSE': 'FTSE 100', '^GDAXI': 'DAX',
    '^FCHI': 'CAC 40', '^STOXX50E': 'STOXX 50', '^N225': 'Nikkei 225',
    '^HANG': 'Hang Seng', '^AXJO': 'ASX 200', '^AORD': 'All Ordinaries',
    '^TWII': 'TAIEX', '^JKSE': 'Jakarta', '^STI': 'Singapore Straits',
    'BVSP': 'Bovespa', '^MERV': 'Merval',
}

MORE_US_STOCKS = {
    'J': 'Jacobs', 'AZO': 'AutoZone', 'NKE': 'Nike', 'GE': 'General Electric',
    'BA': 'Boeing', 'UBER': 'Uber', 'LYFT': 'Lyft', 'DISH': 'Dish',
    'MU': 'Micron', 'LRCX': 'Lam Research', 'ASML': 'ASML', 'ARM': 'ARM',
    'MSTR': 'MicroStrategy', 'SNOW': 'Snowflake', 'DDOG': 'Datadog',
    'ZM': 'Zoom', 'CRWD': 'CrowdStrike', 'OKTA': 'Okta', 'TWLO': 'Twilio',
}

EUROPEAN_STOCKS = {
    'SAP.DE': 'SAP', 'SIE.DE': 'Siemens', 'ALV.DE': 'Allianz',
    'MUV2.DE': 'Munich Re', 'DBX.DE': 'Deutsche Boerse', 'EOAN.DE': 'EON',
    'FLT.L': 'Flutter', 'AZN.L': 'AstraZeneca', 'HSBA.L': 'HSBC',
    'UNAC.L': 'Unilever', 'ULVR.L': 'Unilever PLC', 'DGE.L': 'Diageo',
}

ASIAN_STOCKS = {
    'ASML.AS': 'ASML', '0QQQ.DE': 'NASDAQ ETF', '0QQV.DE': 'NASDAQ 100',
    'CBA.AX': 'CBA', '6758.T': 'Sony', '9984.T': 'SoftBank',
    '8031.T': 'Mitsumi', '6273.T': 'SMC', 'AZJ.AX': 'Aristocratic',
}


def fetch_all_stocks():
    """Compile all stocks for Phase 2"""
    all_stocks = {}
    
    # Add all new stocks
    all_stocks.update({f'{k}': (v, 'INDIA') for k, v in ADDITIONAL_INDIAN_STOCKS.items()})
    all_stocks.update({f'{k}': (v, 'EMERGING') for k, v in EMERGING_MARKETS.items()})
    all_stocks.update({f'{k}': (v, 'COMMODITIES') for k, v in COMMODITIES_FUTURES.items()})
    all_stocks.update({f'{k}': (v, 'INDICES') for k, v in GLOBAL_INDICES.items()})
    all_stocks.update({f'{k}': (v, 'USA') for k, v in MORE_US_STOCKS.items()})
    all_stocks.update({f'{k}': (v, 'EUROPE') for k, v in EUROPEAN_STOCKS.items()})
    all_stocks.update({f'{k}': (v, 'ASIA') for k, v in ASIAN_STOCKS.items()})
    
    return all_stocks


def download_stock_data(symbol, start_date='2015-01-01', end_date=None, market=''):
    """Download a single stock"""
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    try:
        data = yf.download(symbol, start=start_date, end=end_date, progress=False)
        if data.empty:
            return None, 0
        
        data = data.reset_index()
        if 'Adj Close' in data.columns:
            data = data[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        
        market_dir = f'data/{market.lower()}' if market else 'data'
        os.makedirs(market_dir, exist_ok=True)
        
        filename = f"{market_dir}/{symbol.replace('.', '_')}.csv"
        data.to_csv(filename, index=False)
        file_size = os.path.getsize(filename)
        
        return len(data), file_size
    except Exception as e:
        return None, 0


def phase2_download(max_workers=8):
    """Download Phase 2: 200+ additional stocks"""
    print("\n" + "="*100)
    print("PHASE 2: EXPAND TO 300+ GLOBAL STOCKS")
    print("="*100)
    
    all_stocks = fetch_all_stocks()
    total_stocks = len(all_stocks)
    
    print(f"\nDownloading {total_stocks} new stocks...")
    print(f"Categories:")
    print(f"  India: {len(ADDITIONAL_INDIAN_STOCKS)}")
    print(f"  Emerging: {len(EMERGING_MARKETS)}")
    print(f"  Commodities: {len(COMMODITIES_FUTURES)}")
    print(f"  Indices: {len(GLOBAL_INDICES)}")
    print(f"  USA: {len(MORE_US_STOCKS)}")
    print(f"  Europe: {len(EUROPEAN_STOCKS)}")
    print(f"  Asia: {len(ASIAN_STOCKS)}\n")
    
    completed = 0
    total_size = 0
    failed = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_stock_data, symbol, market=market): (symbol, name, market)
            for symbol, (name, market) in all_stocks.items()
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            symbol, name, market = futures[future]
            try:
                rows, size = future.result()
                if rows is not None:
                    completed += 1
                    total_size += size
                    size_mb = size / (1024 * 1024)
                    print(f"  [{i}/{total_stocks}] {symbol:15} {name:25} | {rows:6} rows | {size_mb:6.2f} MB")
                else:
                    failed += 1
            except Exception:
                failed += 1
    
    # Summary
    print("\n" + "="*100)
    print("PHASE 2 SUMMARY")
    print("="*100)
    
    total_gb = total_size / (1024**3)
    print(f"\n✓ Downloaded: {completed} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"\nPhase 2 Size: {total_gb:.2f} GB")
    print(f"Average per stock: {(total_size / max(completed, 1)) / (1024**2):.2f} MB")
    print(f"\nCumulative size (Phase 1 + Phase 2): ~{0.03 + total_gb:.2f} GB")
    
    return {'completed': completed, 'failed': failed, 'size_gb': total_gb}


if __name__ == "__main__":
    result = phase2_download(max_workers=8)
    print("\n📊 Phase 2 Complete!")
    print("Ready for Phase 3: Download hourly data (optional)")
