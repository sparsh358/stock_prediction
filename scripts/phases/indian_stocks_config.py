"""
Indian Stock Database Configuration
Supports 25+ major Indian stocks across all sectors
"""

# Comprehensive India Stock List (NSE)
INDIAN_STOCKS = {
    # IT Services
    'TCS.NS': 'Tata Consultancy Services',
    'INFY.NS': 'Infosys Limited',
    'WIPRO.NS': 'Wipro Limited',
    'HCLTECH.NS': 'HCL Technologies',
    'TECHM.NS': 'Tech Mahindra',
    
    # Banks
    'SBIN.NS': 'State Bank of India',
    'HDFCBANK.NS': 'HDFC Bank',
    'ICICIBANK.NS': 'ICICI Bank',
    'AXISBANK.NS': 'Axis Bank',
    'KOTAKBANK.NS': 'Kotak Mahindra Bank',
    
    # Energy & Power
    'RELIANCE.NS': 'Reliance Industries',
    'NTPC.NS': 'NTPC Limited',
    'POWERGRID.NS': 'Power Grid Corporation',
    'COALINDIA.NS': 'Coal India Limited',
    'BPCL.NS': 'Bharat Petroleum',
    
    # Automobile
    'TATA.NS': 'Tata Motors',
    'MARUTI.NS': 'Maruti Suzuki India',
    'BAJAJ-AUTO.NS': 'Bajaj Auto',
    'EICHER.NS': 'Eicher Motors',
    'HEROMOTOCO.NS': 'Hero MotoCorp',
    
    # Pharma
    'CIPLA.NS': 'Cipla Limited',
    'SUNPHARMA.NS': 'Sun Pharmaceutical',
    'LUPIN.NS': 'Lupin Limited',
    'APOLLOHOSP.NS': 'Apollo Hospitals',
    'BIOCON.NS': 'Biocon Limited',
    
    # FMCG & Consumer
    'ITC.NS': 'ITC Limited',
    'NESTLEIND.NS': 'Nestle India',
    'BRITANNIA.NS': 'Britannia Industries',
    'HINDUNILVR.NS': 'Hindustan Unilever',
    'MARICO.NS': 'Marico Limited',
    
    # Infrastructure
    'LT.NS': 'Larsen & Toubro',
    'DLF.NS': 'DLF Limited',
    'SUNTECK.NS': 'Sunteck Realty',
    
    # Cement
    'SHREECEM.NS': 'Shree Cement',
    'JKCEMENT.NS': 'JK Cement',
    'AMBUJA.NS': 'Ambuja Cements',
    
    # Metals & Mining
    'HINDALCO.NS': 'Hindalco Industries',
    'JSTEEL.NS': 'JSW Steel',
    'TATASTEEL.NS': 'Tata Steel',
}

# Sector grouping for analysis
SECTOR_GROUPS = {
    'IT Services': ['TCS.NS', 'INFY.NS', 'WIPRO.NS', 'HCLTECH.NS', 'TECHM.NS'],
    'Banking': ['SBIN.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'AXISBANK.NS', 'KOTAKBANK.NS'],
    'Energy': ['RELIANCE.NS', 'NTPC.NS', 'POWERGRID.NS', 'COALINDIA.NS', 'BPCL.NS'],
    'Automobile': ['TATA.NS', 'MARUTI.NS', 'BAJAJ-AUTO.NS', 'EICHER.NS', 'HEROMOTOCO.NS'],
    'Pharma': ['CIPLA.NS', 'SUNPHARMA.NS', 'LUPIN.NS', 'APOLLOHOSP.NS', 'BIOCON.NS'],
    'FMCG': ['ITC.NS', 'NESTLEIND.NS', 'BRITANNIA.NS', 'HINDUNILVR.NS', 'MARICO.NS'],
    'Infrastructure': ['LT.NS', 'DLF.NS', 'SUNTECK.NS'],
    'Cement': ['SHREECEM.NS', 'JKCEMENT.NS', 'AMBUJA.NS'],
    'Metals': ['HINDALCO.NS', 'JSTEEL.NS', 'TATASTEEL.NS'],
}

# High volume stocks (most actively traded)
HIGH_VOLUME_STOCKS = [
    'TCS.NS', 'INFY.NS', 'RELIANCE.NS', 'HDFCBANK.NS', 'ICICIBANK.NS',
    'WIPRO.NS', 'SBIN.NS', 'MARUTI.NS', 'TATA.NS', 'AXISBANK.NS'
]

# First tier stocks to start with
STARTER_STOCKS = [
    'TCS.NS',      # IT
    'INFY.NS',     # IT
    'WIPRO.NS',    # IT
    'RELIANCE.NS', # Energy
    'HDFCBANK.NS',  # Banking
    'ICICIBANK.NS',  # Banking
    'SBIN.NS',     # Banking
    'MARUTI.NS',   # Auto
    'TATA.NS',     # Auto
    'HINDUNILVR.NS', # FMCG
]

if __name__ == '__main__':
    print(f"Total stocks: {len(INDIAN_STOCKS)}")
    for sector, stocks in SECTOR_GROUPS.items():
        print(f"{sector}: {len(stocks)} stocks")
    print(f"\nHigh volume: {len(HIGH_VOLUME_STOCKS)} stocks")
    print(f"Starter set: {len(STARTER_STOCKS)} stocks")
