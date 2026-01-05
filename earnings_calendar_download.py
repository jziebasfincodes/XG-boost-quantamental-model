import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = "YOUR_API_KEY"
BASE_URL = "https://financialmodelingprep.com/stable"
MAX_WORKERS = 16

TICKERS = {
    Your Ticker Here 
}

ALL_TICKERS = []
for sector, tickers in TICKERS.items():
    ALL_TICKERS.extend(tickers)
ALL_TICKERS = list(set(ALL_TICKERS))

print(f"\n{'='*100}")
print(f"EARNINGS & DIVIDENDS HISTORICAL DATA DOWNLOAD")
print(f"Total tickers: {len(ALL_TICKERS)}")
print(f"{'='*100}\n")

def get_earnings_history(ticker):
    """Get all historical earnings dates"""
    url = f"{BASE_URL}/earnings?symbol={ticker}&limit=1000&apikey={API_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if data else None
        return None
    except:
        return None

def get_dividends_history(ticker):
    """Get all historical dividends"""
    url = f"{BASE_URL}/dividends?symbol={ticker}&limit=1000&apikey={API_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if data else None
        return None
    except:
        return None

def process_ticker(ticker):
    """Get earnings and dividends for a ticker"""
    
    # Get earnings
    earnings = get_earnings_history(ticker)
    time.sleep(0.15)
    
    # Get dividends
    dividends = get_dividends_history(ticker)
    time.sleep(0.15)
    
    earnings_count = len(earnings) if earnings else 0
    dividends_count = len(dividends) if dividends else 0
    
    if earnings_count > 0 or dividends_count > 0:
        print(f"  ✓ {ticker}: {earnings_count} earnings, {dividends_count} dividends")
    else:
        print(f"  ✗ {ticker}: No data")
    
    return {
        'ticker': ticker,
        'earnings': earnings,
        'dividends': dividends
    }

def download_all():
    """Download everything"""
    
    all_earnings = []
    all_dividends = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t): t for t in ALL_TICKERS}
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                
                if result['earnings']:
                    for e in result['earnings']:
                        e['ticker'] = ticker
                    all_earnings.extend(result['earnings'])
                
                if result['dividends']:
                    for d in result['dividends']:
                        d['ticker'] = ticker
                    all_dividends.extend(result['dividends'])
                    
            except Exception as e:
                print(f"  ✗ {ticker}: Exception - {str(e)[:50]}")
    
    return all_earnings, all_dividends

# Download
print("Starting download...\n")
earnings_data, dividends_data = download_all()

print(f"\n{'='*100}")
print("DOWNLOAD COMPLETE")
print(f"{'='*100}")
print(f"Earnings records: {len(earnings_data):,}")
print(f"Dividends records: {len(dividends_data):,}")

# Save earnings
if earnings_data:
    df = pd.DataFrame(earnings_data)
    df = df.sort_values(['ticker', 'date'], ascending=[True, False])
    df.to_csv("earnings_dates_historical.csv", index=False)
    print(f"\n✓ Saved: earnings_dates_historical.csv ({len(df):,} rows)")
    print(f"  Companies: {df['ticker'].nunique()}")
    if 'date' in df.columns:
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    
    print(f"\n  Earnings data includes:")
    print(f"  - date: Earnings announcement date")
    print(f"  - epsActual: Actual reported EPS")
    print(f"  - epsEstimated: Consensus EPS estimate")
    print(f"  - revenueActual: Actual reported revenue")
    print(f"  - revenueEstimated: Consensus revenue estimate")

# Save dividends
if dividends_data:
    df = pd.DataFrame(dividends_data)
    df = df.sort_values(['ticker', 'date'], ascending=[True, False])
    df.to_csv("dividends_historical.csv", index=False)
    print(f"\n✓ Saved: dividends_historical.csv ({len(df):,} rows)")
    print(f"  Companies: {df['ticker'].nunique()}")
    if 'date' in df.columns:
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    
    print(f"\n  Dividends data includes:")
    print(f"  - date: Ex-dividend date")
    print(f"  - recordDate: Record date")
    print(f"  - paymentDate: Payment date")
    print(f"  - declarationDate: Declaration date")
    print(f"  - dividend: Dividend amount per share")
    print(f"  - yield: Dividend yield %")
    print(f"  - frequency: Quarterly, Annual, etc.")
