import requests
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# CONFIGURATION
# ==========================================
API_KEY = "YOUR_API_KEY_HERE"
BASE_URL = "https://financialmodelingprep.com/stable"
MAX_WORKERS = 5

# ALL YOUR TICKERS
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
        "ZS", "PANW", "OKTA", "SE", "NET", "WIT", "INFY", "STM", "GFS", "ON", 
        "CDNS", "SNPS", "ANSS", "DATADOG", "AKAM", "LOGI", "JNPR", "FLEX", "COGN", "STNE",
        "TOST", "AFRM", "PATH", "BSY", "HUBB", "TEL", "GRMN", "FICO", "WIX", "Z",
        "U", "PAGER", "BILL", "DOCU", "SPLK", "DYN", "GLOB", "ESTC", "DT", "GDDY",
        "SSNC", "AZPN", "PTC", "TYL", "GWRE"
    ],
    "HEALTHCARE": [
        
        "LLY", "JNJ", "ABBV", "UNH", "MRK", "TMO", "ABT", "ISRG", "AMGN", "DHR",
        "GILD", "PFE", "BSX", "SYK", "MDT", "VRTX", "BMY", "HCA", "MCK", "CVS",
        "REGN", "ELV", "CI", "COR", "BDX", "ZTS", "IDXX", "EW", "CAH", "A",
        "IQV", "GEHC", "RMD", "HUM", "MTD", "DXCM", "BIIB", "STE", "WAT", "LH",
        "PODD", "CNC", "WST", "DGX", "INCY", "ZBH", "HOLX", "COO", "VTRS", "UHS",
        "SOLV", "MRNA", "ALGN", "RVTY", "CRL", "BAX", "TECH", "MOH", "HSIC", "DVA",
        
        "NVO", "AZN", "NVS", "GSK", "SNY", "HAE", "ALNY", "BMRN", "VRTX", "SGEN", 
        "TAK", "ZBH", "MOH", "VTRS", "HAE", "DVA", "WST", "STT", "PRGO", "XRAY",
        "ELV", "CI", "MDT", "SYK", "BSX", "BDX", "ZTS", "IDXX", "EW", "RMD",
        "DXCM", "BIIB", "STE", "WAT", "LH", "PODD", "CNC", "DGX", "INCY", "HOLX",
        "COO", "UHS", "SOLV", "MRNA", "ALGN", "RVTY", "CRL", "BAX", "TECH", "HSIC",
        "IONS", "UTHR", "EXEL", "TLY", "ONTF"
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
        "BBD", "NU", "SOFI", "AFRM", "ALLY", "PINS", "BKI", "DFS", "SYF", "FHN",
        "CMA", "ZION", "KEY", "RF", "HBAN", "MTB", "TFC", "CFG", "STI", "WBS",
        "STT", "NTRS", "BK", "AMP", "TROW", "IVZ", "BEN", "PFG", "PRU", "MET",
        "AIG", "ALL", "PGR", "TRV", "CB", "MMC", "AON", "AJG", "BRO"
    ],
    "CONSUMER_DISC": [
     
        "AMZN", "TSLA", "HD", "MCD", "BKNG", "TJX", "LOW", "DASH", "SBUX", "NKE",
        "MAR", "ABNB", "ORLY", "GM", "RCL", "HLT", "ROST", "AZO", "F", "CMG",
        "LVS", "DHI", "YUM", "EBAY", "GRMN", "CCL", "EXPE", "TSCO", "ULTA", "TPR",
        "LEN", "LULU", "PHM", "WSM", "DRI", "RL", "NVR", "GPC", "APTV", "DECK",
        "DPZ", "BBY", "WYNN", "HAS", "NCLH", "MGM", "POOL", "LKQ", "MHK",
        
        "TM", "HMC", "LVMUY", "NKE", "BABA", "PDD", "JD", "MELI", "CPNG", "LULU", 
        "EBAY", "DASH", "ETSY", "RIVN", "LCID", "F", "GM", "STLA", "VWAGY", "BMWYY",
        "HMC", "TM", "BYDDY", "NIO", "LI", "XPEV", "SONY", "NTES", "CHWY", "DKNG",
        "PENN", "WYNN", "MGM", "LVS", "CZR", "MAR", "HLT", "H", "WH", "RCL",
        "CCL", "NCLH", "NKE", "ADS", "DECK", "SKX", "TPR", "CPRI", "PVH", "VFC",
        "RL", "WSM", "BBY", "ULTA", "ORLY", "AZO", "GPC", "LKQ", "TSCO", "ROST"
    ],
    "COMMS": [
      
        "GOOG", "GOOGL", "META", "NFLX", "TMUS", "DIS", "T", "VZ", "CMCSA", "WBD",
        "EA", "TTWO", "TKO", "LYV", "FOXA", "FOX", "CHTR", "TTD", "NWS", "NWSA", 
        "PSKY", "MTCH",
       
        "SPOT", "PARA", "ROKU", "SNAP", "PINS", "BIDU", "NTES", "TCEHY", "SEA", "WPP", 
        "IPG", "OMC", "DISH", "LUMN", "ZME", "SIRI", "AMX", "LBTYA", "VOD", "ORAN",
        "TEF", "BT", "SKM", "KT", "PHI", "TU", "BCE", "RCI", "LBRDA", "LBRDK",
        "VIAC", "NXST", "TGNA", "GCI", "NYT", "SCOR", "IQ", "HUYA", "DOYU", "MOMO",
        "SINA", "SOHU", "YNDX", "GOGO", "TIGO", "VEON", "MTL", "MBT", "TKC", "TLC"
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
       
        "SI", "ABB", "FERG", "RYAAY", "EXPD", "ODFL", "KNX", "JBHT", "CHRW", "MATX", 
        "ZTO", "XPO", "GXO", "HUBG", "ARCB", "LSTR", "SAIA", "UHAL", "CAR", "HTZ",
        "BLDR", "AOS", "MAS", "FBHS", "ALLE", "SNA", "SWK", "PNR", "IEX", "NDSN",
        "ITT", "DOV", "AME", "XYL", "VRSK", "EFX", "BR", "PAYX", "ADP", "CTAS",
        "PAYC", "PCTY", "DAY", "MCO", "SPGI", "INFO", "TRI", "RELX", "DNB", "LDOS"
    ],
    "REAL_ESTATE": [
        
        "WELL", "PLD", "AMT", "EQIX", "SPG", "DLR", "O", "CBRE", "PSA", "CCI",
        "VTR", "VICI", "CSGP", "EXR", "AVB", "IRM", "EQR", "SBAC", "WY", "INVH",
        "ESS", "MAA", "KIM", "REG", "HST", "UDR", "CPT", "DOC", "BXP", "FRT",
        "ARE",
        
        "RKT", "UNIT", "MPW", "BXP", "VRE", "NLY", "AGNC", "STWD", "BXMT", "ABR", 
        "RITM", "LADR", "HASI", "AMT", "CCI", "SBAC", "PLD", "DLR", "EQIX", "IRM",
        "PSA", "EXR", "CUBE", "LSI", "WELL", "VTR", "PEAK", "DOC", "ARE", "O",
        "SPG", "REG", "KIM", "FRT", "BRX", "TCO", "MAC", "SKT", "AVB", "EQR",
        "ESS", "MAA", "UDR", "CPT", "INVH", "AMH", "SUI", "ELS", "HST", "RHP",
        "VICI", "GLPI", "MGP", "WY", "PCH", "RYN"
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
        "DLTR", "DG", "TGT", "BJ", "SFM", "POST", "LW", "CAG", "FLO", "MKC",
        "UN", "UL", "DEO", "BUD", "STLA", "NESN", "OR", "MC", "RMS", "KER"
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
        "AA", "CLF", "X", "TECK", "AEM", "GOLD", "WPM", "FNV", "RGLD", "PANW",
        "BASF", "BAYN", "UMG", "GLNCY", "ANGLO", "HOLCIM", "CRH", "SGO", "ARNC", "HBM"
    ]
}

ALL_TICKERS = []
for sector, tickers in TICKERS.items():
    ALL_TICKERS.extend(tickers)
ALL_TICKERS = list(set(ALL_TICKERS))

print(f"\n{'='*100}")
print(f"BALANCE SHEET QUARTERLY DATA DOWNLOAD")
print(f"Total tickers: {len(ALL_TICKERS)}")
print(f"{'='*100}\n")

def get_balance_sheet_quarterly(ticker):
    """Get quarterly balance sheets - max history"""
    url = f"{BASE_URL}/balance-sheet-statement?symbol={ticker}&period=quarter&limit=100&apikey={API_KEY}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data if data else None
        return None
    except:
        return None

def process_ticker(ticker):
    """Get balance sheet data for ticker"""
    
    bs_data = get_balance_sheet_quarterly(ticker)
    time.sleep(0.2)
    
    if bs_data:
        print(f"  ✓ {ticker}: {len(bs_data)} quarters")
        return {'ticker': ticker, 'data': bs_data}
    else:
        print(f"  ✗ {ticker}: No data")
        return {'ticker': ticker, 'data': []}

def download_all():
    """Download all balance sheets"""
    
    all_data = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_ticker, t): t for t in ALL_TICKERS}
        
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
                if result['data']:
                    all_data.extend(result['data'])
            except Exception as e:
                print(f"  ✗ {ticker}: Exception - {str(e)[:50]}")
    
    return all_data

# Download
balance_sheet_data = download_all()

print(f"\n{'='*100}")
print("DOWNLOAD COMPLETE")
print(f"{'='*100}")
print(f"Total records: {len(balance_sheet_data):,}")

if balance_sheet_data:
    df = pd.DataFrame(balance_sheet_data)
    df = df.sort_values(['symbol', 'date'], ascending=[True, False])
    df.to_csv("balance_sheets_quarterly.csv", index=False)
    print(f"\n✓ Saved: balance_sheets_quarterly.csv ({len(df):,} rows)")
    print(f"  Companies: {df['symbol'].nunique()}")
    if 'date' in df.columns:
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
    
    print(f"\n  Balance sheet includes {len(df.columns)} fields:")
    print(f"  - Assets, Liabilities, Equity")
    print(f"  - Cash, Investments, Receivables")
    print(f"  - Debt, Payables, Current/Non-current breakdown")
else:
    print("\n✗ No data collected")
