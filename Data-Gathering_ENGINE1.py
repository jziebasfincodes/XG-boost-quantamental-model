import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = "YOUR_API_KEY_HERE"
BASE_URL = "https://financialmodelingprep.com/stable"
MAX_WORKERS = 16  


TICKERS = {
    "TECH": [
       
        "NVDA", "AAPL", "MSFT", "AVGO", "ORCL", "PLTR", "AMD", "MU", "CSCO", "IBM",
        "CRM", "APP", "LRCX", "AMAT", "INTU", "QCOM", "INTC", "ANET", "APH", "ACN",
        "KLAC", "NOW", "TXN", "ADBE", "ADI", "PANW", "CRWD", "SNPS", "CDNS", "DELL",
        "GLW", "TEL", "ADSK", "MSI", "WDC", "FTNT", "STX", "WDAY", "NXPI", "ROP",
        "DDOG", "MPWR", "FICO", "CTSH", "SNDK", "KEYS", "MCHP", "HPE", "TER", "FSLR",
        "JBL", "TDY", "VRSN", "ON", "NTAP", "PTC", "HPQ", "TYL", "TRMB", "IT",
        "CDW", "SMCI", "Q", "GDDY", "GEN", "FFIV", "AKAM", "ZBRA", "EPAM", "SWKS",
     
        "TSM", "ASML", "SAP", "SONY", "SHOP", "SNOW", "ARM", "TEAM", "MDB", "WDAY", 
        "ZS", "OKTA", "SE", "NET", "WIT", "INFY", "STM", "GFS", "ANSS", "DATADOG",
        "LOGI", "JNPR", "FLEX", "COGN", "STNE", "TOST", "AFRM", "PATH", "BSY", "HUBB",
        "GRMN", "WIX", "Z", "U", "PAGER", "BILL", "DOCU", "SPLK", "DYN", "GLOB",
        "ESTC", "DT", "SSNC", "AZPN", "GWRE"
    ],
    "HEALTHCARE": [
     
        "LLY", "JNJ", "ABBV", "UNH", "MRK", "TMO", "ABT", "ISRG", "AMGN", "DHR",
        "GILD", "PFE", "BSX", "SYK", "MDT", "VRTX", "BMY", "HCA", "MCK", "CVS",
        "REGN", "ELV", "CI", "COR", "BDX", "ZTS", "IDXX", "EW", "CAH", "A",
        "IQV", "GEHC", "RMD", "HUM", "MTD", "DXCM", "BIIB", "STE", "WAT", "LH",
        "PODD", "CNC", "WST", "DGX", "INCY", "ZBH", "HOLX", "COO", "VTRS", "UHS",
        "SOLV", "MRNA", "ALGN", "RVTY", "CRL", "BAX", "TECH", "MOH", "HSIC", "DVA",
  
        "NVO", "AZN", "NVS", "GSK", "SNY", "HAE", "ALNY", "BMRN", "SGEN", "TAK", 
        "PRGO", "XRAY", "STT", "IONS", "UTHR", "EXEL", "TLY", "ONTF"
    ],
    "FINANCE": [
      
        "JPM", "BAC", "WFC", "V", "USB", "TRV", "TROW", "TFC", "SYF", "STT",
        "SPGI", "SCHW", "RJF", "RF", "PYPL", "PRU", "PNC", "PGR", "PFG", "NTRS",
        "NDAQ", "MTB", "MSCI", "MS", "MMC", "MET", "MCO", "MA", "L", "KKR",
        "KEY", "JKHY", "IVZ", "ICE", "IBKR", "HOOD", "HIG", "HBAN", "GS", "GPN",
        "GL", "FITB", "FISV", "FIS", "FDS", "ERIE", "EG", "CPAY", "COIN", "COF",
        "CME", "CINF", "CFG", "CBOE", "CB", "C", "BX", "BRO", "BRK.B", "BLK",
        "BK", "BEN", "AXP", "APO", "AON", "AMP", "ALL", "AJG", "AIZ", "AIG",
        "AFL", "ACGL", "XYZ", "WTW", "WRB",
      
        "HSBC", "RY", "TD", "HDB", "SAN", "BBVA", "MUFG", "SMFG", "BMO", "BNS", 
        "CM", "UBS", "DB", "ING", "NWG", "BCS", "LYG", "PUK", "MFG", "ITUB",
        "BBD", "NU", "SOFI", "AFRM", "ALLY", "PINS", "BKI", "DFS", "FHN", "CMA",
        "ZION"
    ],
    "CONSUMER_DISC": [
     
        "AMZN", "TSLA", "HD", "MCD", "BKNG", "TJX", "LOW", "DASH", "SBUX", "NKE",
        "MAR", "ABNB", "ORLY", "GM", "RCL", "HLT", "ROST", "AZO", "F", "CMG",
        "LVS", "DHI", "YUM", "EBAY", "GRMN", "CCL", "EXPE", "TSCO", "ULTA", "TPR",
        "LEN", "LULU", "PHM", "WSM", "DRI", "RL", "NVR", "GPC", "APTV", "DECK",
        "DPZ", "BBY", "WYNN", "HAS", "NCLH", "MGM", "POOL", "LKQ", "MHK",
       
        "TM", "HMC", "LVMUY", "BABA", "PDD", "JD", "MELI", "CPNG", "ETSY", "RIVN",
        "LCID", "STLA", "VWAGY", "BMWYY", "BYDDY", "NIO", "LI", "XPEV", "NTES",
        "CHWY", "DKNG", "PENN", "CZR", "H", "WH", "ADS", "SKX", "CPRI", "PVH",
        "VFC"
    ],
    "COMMS": [

        "GOOG", "GOOGL", "META", "NFLX", "TMUS", "DIS", "T", "VZ", "CMCSA", "WBD",
        "EA", "TTWO", "TKO", "LYV", "FOXA", "FOX", "CHTR", "TTD", "NWS", "OMC",
        "NWSA", "PSKY", "MTCH",
  
        "SPOT", "PARA", "ROKU", "SNAP", "PINS", "BIDU", "NTES", "TCEHY", "SEA", "WPP", 
        "IPG", "DISH", "LUMN", "ZME", "SIRI", "AMX", "LBTYA", "VOD", "ORAN", "TEF",
        "BT", "SKM", "KT", "PHI", "TU", "BCE", "RCI", "LBRDA", "LBRDK", "VIAC",
        "NXST", "TGNA", "GCI", "NYT", "SCOR", "IQ", "HUYA", "DOYU", "MOMO", "SINA",
        "SOHU", "YNDX", "GOGO", "TIGO", "VEON", "MTL", "MBT", "TKC", "TLC"
    ],
    "INDUSTRIALS": [
   
        "GE", "CAT", "RTX", "GEV", "UBER", "BA", "UNP", "DE", "HON", "ETN",
        "LMT", "PH", "ADP", "GD", "WM", "TT", "MMM", "UPS", "HWM", "NOC",
        "CTAS", "EMR", "JCI", "TDG", "ITW", "CMI", "FDX", "CSX", "RSG", "NSC",
        "PWR", "PCAR", "LHX", "URI", "GWW", "AME", "FAST", "AXON", "DAL", "CARR",
        "ROK", "PAYX", "CPRT", "WAB", "UAL", "OTIS", "XYL", "ODFL", "IR", "VRSK",
        "ROL", "EME", "DOV", "EFX", "BR", "VLTO", "HUBB", "LDOS", "LUV", "EXPD",
        "CHRW", "JBHT", "SNA", "FTV", "LII", "PNR", "J", "TXT", "ALLE", "NDSN",
        "HII", "IEX", "MAS", "SWK", "BLDR", "DAY", "AOS", "PAYC", "GNRC",
     
        "SI", "ABB", "FERG", "RYAAY", "KNX", "MATX", "ZTO", "XPO", "GXO", "HUBG",
        "ARCB", "LSTR", "SAIA", "UHAL", "CAR", "HTZ", "FBHS", "ITT", "PCTY", "INFO",
        "TRI", "RELX", "DNB"
    ],
    "REAL_ESTATE": [
      
        "WELL", "PLD", "AMT", "EQIX", "SPG", "DLR", "O", "CBRE", "PSA", "CCI",
        "VTR", "VICI", "CSGP", "EXR", "AVB", "IRM", "EQR", "SBAC", "WY", "INVH",
        "ESS", "MAA", "KIM", "REG", "HST", "UDR", "CPT", "DOC", "BXP", "FRT",
        "ARE",

        "RKT", "UNIT", "MPW", "VRE", "NLY", "AGNC", "STWD", "BXMT", "ABR", "RITM",
        "LADR", "HASI", "CUBE", "LSI", "PEAK", "TCO", "MAC", "SKT", "BRX", "AMH",
        "SUI", "ELS", "RHP", "GLPI", "MGP", "PCH", "RYN"
    ],
    "ENERGY": [
        "XOM", "CVX", "SHEL", "TTE", "COP", "BP", "EQNR", "PBR", "EOG", "SLB",
        "ENB", "TRP", "EPD", "KMI", "WMB", "MPC", "PSX", "VLO", "HES", "OXY",
        "DVN", "FANG", "HAL", "BKR", "OKE", "LNG", "EQT", "CTRA", "CHRD", "APA",
        "MRO", "OVV", "CHK", "PXD", "AR", "RRC", "SWN", "MUR", "MTDR", "PR",
        "CNQ", "CVE", "SU", "TOU", "ARX", "VET", "WDS", "STO", "REP", "GALP"
    ],
    "CONSUMER_STAPLES": [
        "WMT", "COST", "PG", "KO", "PEP", "PM", "MO", "CL", "EL", "KDP",
        "MNST", "MDLZ", "STZ", "GIS", "SYY", "ADM", "HSY", "K", "MKC", "CHD",
        "CLX", "CPB", "HRL", "SJM", "TSN", "TAP", "BF.B", "CASY", "KR", "WBA",
        "DLTR", "DG", "TGT", "BJ", "SFM", "POST", "LW", "CAG", "FLO", "UN",
        "UL", "DEO", "BUD", "STLA", "NESN", "OR", "MC", "RMS", "KER"
    ],
    "UTILITIES": [
        "NEE", "DUK", "SO", "D", "EXC", "AEP", "SRE", "XEL", "ED", "PEG",
        "WEC", "ES", "PCG", "AWK", "EIX", "FE", "DTE", "ETR", "PPL", "AEE",
        "LNT", "EVRG", "CNP", "CMS", "ATO", "NI", "PNW", "NRG", "VST", "CEG",
        "SRG", "IDA", "OGE", "ORA", "MGEE", "ALE", "NWN", "CWT", "YORW", "AWR",
        "IBE", "ENEL", "EDF", "NG", "RWE", "EOAN", "SSE", "FORT", "CPX", "INE"
    ],
    "MATERIALS": [
        "LIN", "BHP", "RIO", "FCX", "SHW", "APD", "ECL", "NEM", "NUE", "DOW",
        "CTVA", "PPG", "VALE", "ALB", "FMC", "CE", "DD", "MLM", "VMC", "STLD",
        "CF", "MOS", "EMN", "IFF", "AVY", "BALL", "PKG", "WRK", "SEE", "IP",
        "AA", "CLF", "X", "TECK", "AEM", "GOLD", "WPM", "FNV", "RGLD", "BASF",
        "BAYN", "UMG", "GLNCY", "ANGLO", "HOLCIM", "CRH", "SGO", "ARNC", "HBM"
    ]
}

ALL_SYMBOLS = []
for sector, tickers in TICKERS.items():
    ALL_SYMBOLS.extend(tickers)
ALL_SYMBOLS = list(set(ALL_SYMBOLS))

print(f"\nTotal unique tickers to analyze: {len(ALL_SYMBOLS)}")
print(f"Target: UP TO 15 YEARS (60 QUARTERS) - will download whatever is available")
print(f"This will take a while...\n")

def get_data_full_history(endpoint, symbol, limit=100):
    """Fetch MAXIMUM historical data from FMP API"""
    url = f"{BASE_URL}/{endpoint}?symbol={symbol}&period=quarter&limit={limit}&apikey={API_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if data else None
        return None
    except Exception as e:
        return None

def calculate_advanced_metrics(inc, bal, cf):
    """Calculate ALL advanced metrics"""
    try:
        revenue = inc.get('revenue', 0)
        ebit = inc.get('operatingIncome', 0)
        net_income = inc.get('netIncome', 0)
        ebitda = inc.get('ebitda', 0)
        gross_profit = inc.get('grossProfit', 0)
        operating_income = inc.get('operatingIncome', 0)
        income_before_tax = inc.get('incomeBeforeTax', 1)
        income_tax = inc.get('incomeTaxExpense', 0)
        interest_exp = abs(inc.get('interestExpense', 0.001))
        eps = inc.get('eps', 0)
        
        total_debt = bal.get('totalDebt', 0)
        cash = bal.get('cashAndCashEquivalents', 0)
        shareholders_equity = bal.get('totalStockholdersEquity', 1)
        total_assets = bal.get('totalAssets', 1)
        current_assets = bal.get('totalCurrentAssets', 0)
        current_liabilities = bal.get('totalCurrentLiabilities', 1)
        
        operating_cf = cf.get('operatingCashFlow', 0)
        capex = abs(cf.get('capitalExpenditure', 0))
        fcf = cf.get('freeCashFlow', 0)
        
        # MARGINS
        gross_margin = (gross_profit / revenue * 100) if revenue else 0
        operating_margin = (operating_income / revenue * 100) if revenue else 0
        net_margin = (net_income / revenue * 100) if revenue else 0
        fcf_margin = (fcf / revenue * 100) if revenue else 0
        
        # RETURNS
        roe = (net_income / shareholders_equity * 100) if shareholders_equity else 0
        roa = (net_income / total_assets * 100) if total_assets else 0
        
        # ROIC
        tax_rate = income_tax / income_before_tax if income_before_tax else 0
        nopat = ebit * (1 - tax_rate)
        invested_capital = total_debt + shareholders_equity
        roic = (nopat / invested_capital * 100) if invested_capital > 0 else 0
        
        # LEVERAGE & LIQUIDITY
        debt_to_equity = (total_debt / shareholders_equity) if shareholders_equity else 0
        net_debt = total_debt - cash
        current_ratio = current_assets / current_liabilities if current_liabilities else 0
        interest_coverage = ebit / interest_exp if interest_exp > 0 else 999
        
        # EFFICIENCY
        asset_turnover = revenue / total_assets if total_assets else 0
        
        return {
            "Revenue_M": round(revenue / 1e6, 2),
            "Net_Income_M": round(net_income / 1e6, 2),
            "EBITDA_M": round(ebitda / 1e6, 2),
            "Operating_CF_M": round(operating_cf / 1e6, 2),
            "Free_Cash_Flow_M": round(fcf / 1e6, 2),
            "CapEx_M": round(capex / 1e6, 2),
            "EPS": round(eps, 3),
            "Gross_Margin_%": round(gross_margin, 2),
            "Operating_Margin_%": round(operating_margin, 2),
            "Net_Margin_%": round(net_margin, 2),
            "FCF_Margin_%": round(fcf_margin, 2),
            "ROE_%": round(roe, 2),
            "ROA_%": round(roa, 2),
            "ROIC_%": round(roic, 2),
            "Total_Assets_M": round(total_assets / 1e6, 2),
            "Total_Debt_M": round(total_debt / 1e6, 2),
            "Cash_M": round(cash / 1e6, 2),
            "Net_Debt_M": round(net_debt / 1e6, 2),
            "Shareholders_Equity_M": round(shareholders_equity / 1e6, 2),
            "Debt_to_Equity": round(debt_to_equity, 2),
            "Current_Ratio": round(current_ratio, 2),
            "Interest_Coverage": round(interest_coverage, 2),
            "Asset_Turnover": round(asset_turnover, 2)
        }
    except Exception as e:
        return None

def process_ticker(ticker):
    """Process single ticker - get ALL available quarterly data"""
    
    # Pull maximum available data
    inc_q = get_data_full_history("income-statement", ticker, limit=100)
    time.sleep(0.2)
    
    if not inc_q:
        print(f"  ✗ {ticker}: No income statement data")
        return None
    
    bal_q = get_data_full_history("balance-sheet-statement", ticker, limit=100)
    time.sleep(0.2)
    
    if not bal_q:
        print(f"  ✗ {ticker}: No balance sheet data")
        return None
        
    cf_q = get_data_full_history("cash-flow-statement", ticker, limit=100)
    time.sleep(0.2)
    
    if not cf_q:
        print(f"  ✗ {ticker}: No cash flow data")
        return None
    
    # Process ALL available quarters (whatever they have)
    quarterly_data = []
    max_quarters = min(len(inc_q), len(bal_q), len(cf_q))
    
    for i in range(max_quarters):
        metrics = calculate_advanced_metrics(inc_q[i], bal_q[i], cf_q[i])
        if metrics:
            metrics['Ticker'] = ticker
            metrics['Date'] = inc_q[i].get('date', '')
            metrics['Period'] = inc_q[i].get('period', '')
            metrics['Fiscal_Year'] = inc_q[i].get('fiscalYear', '')
            quarterly_data.append(metrics)
    
    print(f"  ✓ {ticker}: {len(quarterly_data)} quarters ({inc_q[-1].get('date', 'N/A')} to {inc_q[0].get('date', 'N/A')})")
    
    return quarterly_data

def analyze_all_companies():
    """Main execution - pull ALL available quarterly data for ALL companies"""
    print(f"\n{'='*100}")
    print(f"ADAPTIVE BEAST MODE: MAXIMUM AVAILABLE QUARTERLY DATA")
    print(f"Companies: {len(ALL_SYMBOLS)}")
    print(f"Will download whatever historical data is available (up to 100 quarters)")
    print(f"{'='*100}\n")
    
    all_quarterly_data = []
    failed = []
    stats = {'quarters': {}}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t): t for t in ALL_SYMBOLS}
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                if result:
                    all_quarterly_data.extend(result)
                    quarters_count = len(result)
                    stats['quarters'][ticker] = quarters_count
                else:
                    failed.append(ticker)
            except Exception as e:
                print(f"  ✗ {ticker}: Exception - {str(e)[:50]}")
                failed.append(ticker)
    
    print(f"\n{'='*100}")
    print(f"DATA COLLECTION COMPLETE")
    print(f"{'='*100}")
    print(f"Total quarterly records: {len(all_quarterly_data):,}")
    print(f"Successful tickers: {len(stats['quarters'])}")
    print(f"Failed tickers: {len(failed)}")
    
    # Show data availability stats
    if stats['quarters']:
        quarters_list = list(stats['quarters'].values())
        print(f"\nData Availability Stats:")
        print(f"  Average quarters per company: {sum(quarters_list) / len(quarters_list):.1f}")
        print(f"  Max quarters: {max(quarters_list)}")
        print(f"  Min quarters: {min(quarters_list)}")
        print(f"  Companies with 60+ quarters (15 years): {sum(1 for q in quarters_list if q >= 60)}")
        print(f"  Companies with 40+ quarters (10 years): {sum(1 for q in quarters_list if q >= 40)}")
        print(f"  Companies with 20+ quarters (5 years): {sum(1 for q in quarters_list if q >= 20)}")
    
    if failed:
        print(f"\nFailed tickers ({len(failed)}): {', '.join(failed[:50])}{'...' if len(failed) > 50 else ''}")
    print()
    
    # Create massive DataFrame
    df = pd.DataFrame(all_quarterly_data)
    
    # Sort by Ticker and Date
    df = df.sort_values(['Ticker', 'Date'], ascending=[True, False])
    
    # Save EVERYTHING
    output_file = "sp500_maximum_quarterly_fundamentals.csv"
    df.to_csv(output_file, index=False)
    print(f"✓ Saved: {output_file}")
    print(f"  Total rows: {len(df):,}")
    print(f"  Total columns: {len(df.columns)}")
    print(f"  File size: ~{len(df) * len(df.columns) * 10 / 1024 / 1024:.1f} MB")
    
    # ANALYSIS: Show business cycle performance
    print(f"\n{'='*100}")
    print("BUSINESS CYCLE ANALYSIS - KEY PERIODS")
    print(f"{'='*100}\n")
    
    # Key recession/crisis periods
    crisis_periods = {
        '2008 Financial Crisis': ('2008-06-30', '2009-06-30'),
        '2020 COVID Crash': ('2020-03-31', '2020-06-30'),
        'Recent Period': ('2024-01-01', '2025-12-31')
    }
    
    for period_name, (start, end) in crisis_periods.items():
        period_data = df[(df['Date'] >= start) & (df['Date'] <= end)]
        if len(period_data) > 0:
            print(f"{period_name} ({start} to {end}):")
            print(f"  Companies: {period_data['Ticker'].nunique()}")
            print(f"  Avg Net Margin: {period_data['Net_Margin_%'].mean():.2f}%")
            print(f"  Avg ROIC: {period_data['ROIC_%'].mean():.2f}%")
            print(f"  Avg FCF Margin: {period_data['FCF_Margin_%'].mean():.2f}%")
            print()
    
    # Show top performers across full period
    print(f"{'='*100}")
    print("TOP 15 COMPANIES BY AVERAGE ROIC (FULL PERIOD)")
    print(f"{'='*100}\n")
    top_roic = df.groupby('Ticker').agg({
        'ROIC_%': 'mean',
        'Date': 'count'
    }).rename(columns={'Date': 'Quarters'}).sort_values('ROIC_%', ascending=False).head(15)
    print(top_roic.to_string())
    
    print(f"\n{'='*100}")
    print("✓ COMPLETE!")
    print(f"{'='*100}\n")

if __name__ == "__main__":
    analyze_all_companies()
