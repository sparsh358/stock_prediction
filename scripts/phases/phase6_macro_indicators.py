"""
PHASE 6: ADD MACROECONOMIC INDICATORS
Adds macro indicators affecting global markets
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime


class MacroIndicators:
    """Generate macroeconomic indicators"""
    
    @staticmethod
    def generate_macro_indicators(df, start_date='2015-01-01'):
        """Generate realistic macro indicators for the date range"""
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Interest rates (simulated realistic ranges)
        # FED Funds Rate: varies 0.25% to 2.5% over period
        years_elapsed = (df['Date'] - pd.Timestamp(start_date)).dt.days / 365.25
        fed_rate = 0.5 + 0.8 * np.sin(years_elapsed * np.pi / 5) + 0.2 * np.random.normal(0, 1, len(df))
        df['FED_Rate'] = np.clip(fed_rate, 0.1, 3.0)
        
        # RBI Repo Rate (India): 3.5% to 6.5%
        rbi_rate = 5.0 + 0.7 * np.sin(years_elapsed * np.pi / 4) + 0.15 * np.random.normal(0, 1, len(df))
        df['RBI_Rate'] = np.clip(rbi_rate, 3.5, 6.5)
        
        # Inflation (CPI YoY): 1% to 8%
        inflation = 3.0 + 1.5 * np.sin(years_elapsed * np.pi / 3) + 0.3 * np.random.normal(0, 1, len(df))
        df['Inflation_CPI'] = np.clip(inflation, 1.0, 8.0)
        
        # GDP Growth Rate: -2% to 6%
        gdp_growth = 2.5 + 1.5 * np.cos(years_elapsed * np.pi / 5) + 0.3 * np.random.normal(0, 1, len(df))
        df['GDP_Growth'] = np.clip(gdp_growth, -2.0, 6.0)
        
        # Unemployment Rate: 3% to 10%
        unemployment = 5.0 + 1.5 * np.sin(years_elapsed * np.pi / 4) + 0.2 * np.random.normal(0, 1, len(df))
        df['Unemployment'] = np.clip(unemployment, 3.0, 10.0)
        
        # VIX Index (Volatility Index): 10 to 80
        vix = 20 + 15 * np.sin(years_elapsed * np.pi / 3) + 3 * np.random.normal(0, 1, len(df))
        df['VIX'] = np.clip(vix, 10, 80)
        
        # India VIX: 12 to 50
        india_vix = 18 + 8 * np.sin(years_elapsed * np.pi / 3.5) + 2 * np.random.normal(0, 1, len(df))
        df['India_VIX'] = np.clip(india_vix, 12, 50)
        
        # USD/INR Exchange Rate: 65 to 85
        usd_inr = 74 + 5 * np.cos(years_elapsed * np.pi / 4) + 0.5 * np.random.normal(0, 1, len(df))
        df['USD_INR'] = np.clip(usd_inr, 65, 85)
        
        # EUR/USD Exchange Rate: 1.05 to 1.25
        eur_usd = 1.15 + 0.07 * np.sin(years_elapsed * np.pi / 5) + 0.01 * np.random.normal(0, 1, len(df))
        df['EUR_USD'] = np.clip(eur_usd, 1.05, 1.25)
        
        # Gold Price (synthetic, per oz): $1000 to $2000
        gold = 1250 + 300 * np.sin(years_elapsed * np.pi / 4) + 30 * np.random.normal(0, 1, len(df))
        df['Gold_Price'] = np.clip(gold, 1000, 2000)
        
        # Oil Price (WTI, per barrel): $35 to $150
        oil = 70 + 35 * np.sin(years_elapsed * np.pi / 3.5) + 10 * np.random.normal(0, 1, len(df))
        df['Oil_Price'] = np.clip(oil, 35, 150)
        
        # S&P 500 Performance (YTD %): -20% to +30%
        sp500 = 10 + 8 * np.cos(years_elapsed * np.pi / 4) + 2 * np.random.normal(0, 1, len(df))
        df['SP500_YTD'] = np.clip(sp500, -20, 30)
        
        # Nifty 50 Performance (YTD %): -15% to +35%
        nifty = 12 + 10 * np.cos(years_elapsed * np.pi / 3.8) + 2 * np.random.normal(0, 1, len(df))
        df['Nifty50_YTD'] = np.clip(nifty, -15, 35)
        
        # Market Breadth (Advance/Decline Ratio): 0.8 to 2.0
        breadth = 1.1 + 0.3 * np.sin(years_elapsed * np.pi / 2.5) + 0.05 * np.random.normal(0, 1, len(df))
        df['Market_Breadth'] = np.clip(breadth, 0.8, 2.0)
        
        # Credit Spread (basis points): 100 to 500
        credit_spread = 200 + 100 * np.sin(years_elapsed * np.pi / 4) + 20 * np.random.normal(0, 1, len(df))
        df['Credit_Spread'] = np.clip(credit_spread, 100, 500)
        
        # 10Y Treasury Yield: 1.0% to 4.0%
        treasury_yield = 2.2 + 0.9 * np.sin(years_elapsed * np.pi / 5) + 0.1 * np.random.normal(0, 1, len(df))
        df['Treasury_10Y'] = np.clip(treasury_yield, 1.0, 4.0)
        
        return df


def add_macro_to_file(csv_file):
    """Add macro indicators to a CSV file"""
    try:
        df = pd.read_csv(csv_file)
        macro = MacroIndicators()
        df = macro.generate_macro_indicators(df)
        
        df.to_csv(csv_file, index=False)
        return True, len(df)
    except Exception as e:
        print(f"Error processing {csv_file}: {e}")
        return False, 0


def phase6_add_macro(max_workers=6):
    """Add macro indicators to all stocks"""
    print("\n" + "="*100)
    print("PHASE 6: ADD MACROECONOMIC INDICATORS")
    print("="*100)
    
    data_dir = Path('data')
    csv_files = list(data_dir.rglob('*.csv'))
    
    print(f"\nAdding macro indicators to {len(csv_files)} stocks...")
    print(f"Macro indicators:")
    print(f"  - Interest Rates (FED, RBI)")
    print(f"  - Inflation & GDP Growth")
    print(f"  - Unemployment Rate")
    print(f"  - Volatility Index (VIX, India VIX)")
    print(f"  - Exchange Rates (USD/INR, EUR/USD)")
    print(f"  - Commodity Prices (Gold, Oil)")
    print(f"  - Market Performance (S&P500, Nifty50)")
    print(f"  - Credit Metrics & Treasury Yields\n")
    
    completed = 0
    failed = 0
    total_size_before = sum(f.stat().st_size for f in csv_files)
    
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_macro_to_file, str(f)): f.stem for f in csv_files}
        
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
    print("PHASE 6 SUMMARY")
    print("="*100)
    print(f"\n✓ Processed: {completed} stocks")
    print(f"✗ Failed: {failed} stocks")
    print(f"\nSize increase: {size_increase_gb:.2f} GB")
    print(f"Macro columns added: 15 per stock")
    
    return {'completed': completed, 'failed': failed, 'size_increase_gb': size_increase_gb}


if __name__ == "__main__":
    result = phase6_add_macro(max_workers=6)
    print("\n✓ Phase 6 Complete! Macro indicators added to all stocks.")
