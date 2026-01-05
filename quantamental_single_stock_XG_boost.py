import pandas as pd
import numpy as np
import yfinance as yf
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.cluster import KMeans
import warnings
import matplotlib.pyplot as plt

warnings.filterwarnings('ignore')

# --- THE INSTITUTIONAL FEATURE SET ---
FEATURES = [
    'epsActual', 'surprise', 'ROA', 'Margin_ROC', 'FCF_Yield', 
    'rev_accel', 'wedge', 'rsi', 'dist_sma50', 
    'dist_sma200', 'daily_volume', 'Days_In_Cycle'
]

def build_mega_dataset(use_kmeans_labels=True):
    print("--- Phase 1: Atomic Multi-Statement Alignment ---")
    
    # 1. LOAD DATA SOURCES
    try:
        df_fund = pd.read_csv("sp500_maximum_quarterly_fundamentals.csv")
        df_bs   = pd.read_csv("balance_sheets_quarterly.csv") 
        df_earn = pd.read_csv("earnings_dates_historical.csv")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return pd.DataFrame()

    # --- IMPROVED HEADER SANITIZER ---
    def sanitize_df(df):
        df = df.loc[:, ~df.columns.duplicated()]
        df.columns = df.columns.str.strip()
        
        rename_map = {}
        for col in df.columns:
            c_low = col.lower()
            if c_low in ['ticker', 'symbol', 'stock'] and 'Ticker' not in df.columns:
                rename_map[col] = 'Ticker'
            elif c_low in ['date', 'fiscaldate', 'periodend', 'report_date'] and 'Date' not in df.columns:
                rename_map[col] = 'Date'
        
        df = df.rename(columns=rename_map)
        df = df.loc[:, ~df.columns.duplicated()]
        
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        return df

    df_fund = sanitize_df(df_fund).drop_duplicates(subset=['Ticker', 'Date'])
    df_bs   = sanitize_df(df_bs).drop_duplicates(subset=['Ticker', 'Date'])
    df_earn = sanitize_df(df_earn).drop_duplicates(subset=['Ticker', 'Date'])
    
    print(f"Loaded: Fundamentals={len(df_fund)}, BalanceSheets={len(df_bs)}, Earnings={len(df_earn)}")

    # 2. ATOMIC MERGE: Income + Balance Sheet
    bs_cols = [c for c in df_bs.columns if c not in df_fund.columns or c in ['Ticker', 'Date']]
    df_accounting = pd.merge(df_fund, df_bs[bs_cols], on=['Ticker', 'Date'], how='inner')
    
    print(f"After merge: {len(df_accounting)} records")

    # 3. COMPUTE PHYSICS
    print("Computing Acceleration Physics & Margin Pivots...")
    df_accounting['Margin_ROC'] = df_accounting.groupby('Ticker')['Operating_Margin_%'].diff().fillna(0)
    df_accounting['rev_growth'] = df_accounting.groupby('Ticker')['Revenue_M'].pct_change().fillna(0)
    df_accounting['asset_growth'] = df_accounting.groupby('Ticker')['Total_Assets_M'].pct_change().fillna(0)
    df_accounting['wedge'] = (df_accounting['asset_growth'] - df_accounting['rev_growth']).fillna(0)
    df_accounting['FCF_Yield'] = df_accounting['Free_Cash_Flow_M'].fillna(0)

    # 4. ALIGN TO EARNINGS CALENDAR
    print("Aligning to Earnings Events...")
    
    earn_clean = df_earn[['Ticker', 'Date', 'epsActual', 'epsEstimated']].copy()
    earn_clean = earn_clean.rename(columns={'Date': 'Report_Date'})
    earn_clean['Report_Date'] = pd.to_datetime(earn_clean['Report_Date'], errors='coerce')
    
    print(f"Clean earnings records before dedup: {len(earn_clean)}")
    
    # Clean both dataframes
    df_accounting = df_accounting.dropna(subset=['Date'])
    earn_clean = earn_clean.dropna(subset=['Report_Date'])

    # CRITICAL: merge_asof requires GLOBAL monotonic sort on the 'on' key
    # PRIMARY: Date / Report_Date
    # SECONDARY: Ticker
    df_accounting = (
        df_accounting
        .sort_values(['Date', 'Ticker'])
        .reset_index(drop=True)
    )

    earn_clean = (
        earn_clean
        .sort_values(['Report_Date', 'Ticker'])
        .reset_index(drop=True)
    )

    print("Performing merge_asof...")
    df_mega = pd.merge_asof(
        df_accounting,
        earn_clean,
        left_on='Date',
        right_on='Report_Date',
        by='Ticker',
        direction='forward',
        tolerance=pd.Timedelta('90 days')
    )
    
    print(f"After earnings alignment: {len(df_mega)} records")

    # 5. TACTICAL SYNC (yfinance)
    tickers = df_mega['Ticker'].unique().tolist()
    print(f"\nSyncing {len(tickers)} tickers with yfinance prices...")
    
    try:
        raw = yf.download(tickers, start="2016-01-01", auto_adjust=False, progress=False)
        prices = raw['Close']
        volumes = raw['Volume']
    except Exception as e:
        print(f"Error downloading price data: {e}")
        return pd.DataFrame()

    master_list = []

    for idx, t in enumerate(tickers):
        if idx % 50 == 0:
            print(f"Processing ticker {idx+1}/{len(tickers)}: {t}")
            
        if t not in prices.columns: 
            continue
        
        t_p = prices[t].dropna().to_frame(name='Close')
        t_v = volumes[t].dropna().to_frame(name='Volume')
        
        if len(t_p) < 200: 
            continue

        # Technical Indicators
        t_p['sma50'] = t_p['Close'].rolling(50).mean()
        t_p['sma200'] = t_p['Close'].rolling(200).mean()
        
        delta = t_p['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        t_p['rsi'] = 100 - (100 / (1 + (gain/(loss + 0.001))))
        
        t_f = df_mega[df_mega['Ticker'] == t].copy()
        t_f = t_f.dropna(subset=['Report_Date'])
        
        if len(t_f) == 0:
            continue

        t_f = t_f.sort_values('Report_Date').reset_index(drop=True)
        t_p_reset = (
        t_p
        .reset_index()
        .rename(columns={'Date': 'Price_Date'})
        .sort_values('Price_Date')     # ✅ sort on the renamed column
        .reset_index(drop=True)
    )


        # Merge Price with Fundamentals
        t_combined = pd.merge_asof(
        t_p_reset,
        t_f,
        left_on='Price_Date',
        right_on='Report_Date',
        direction='backward'
    )

        # Calculate Targets (90-day forward return)
        t_combined['fwd_ret'] = t_combined['Close'].shift(-90) / t_combined['Close'] - 1
        # --- CAP NON-ECONOMIC RETURNS (CRITICAL FOR KMEANS) ---
        t_combined['fwd_ret'] = np.clip(
            t_combined['fwd_ret'],
            -0.8,
            1.0
        )

        
        # Feature Engineering
        t_combined['dist_sma50'] = (t_combined['Close'] / t_combined['sma50']) - 1
        t_combined['dist_sma200'] = (t_combined['Close'] / t_combined['sma200']) - 1
        t_combined['daily_volume'] = t_v['Volume'].reindex(t_combined['Price_Date']).values
        t_combined['Days_In_Cycle'] = (t_combined['Price_Date'] - t_combined['Report_Date']).dt.days
        t_combined['surprise'] = (t_combined['epsActual'] / (t_combined['epsEstimated'] + 0.001)) - 1
        t_combined['ROA'] = t_combined.get('ROA_%', t_combined.get('ROIC_%', 0))
        t_combined['rev_accel'] = t_combined.get('rev_growth', 0)

        t_combined = (
        t_combined
        .groupby(['Ticker', 'Report_Date'], as_index=False)
        .last()
    )

        master_list.append(t_combined)

    if not master_list:
        print("ERROR: No data collected for any ticker!")
        return pd.DataFrame()

    df_final = pd.concat(master_list, ignore_index=True)
    df_final = df_final.dropna(subset=['fwd_ret'] + FEATURES)
    
    print(f"\nBefore labeling: {len(df_final)} records")
    
    # UNSUPERVISED LABELING via K-Means
    if use_kmeans_labels:
        print("\n--- Using K-Means for Unsupervised Labeling ---")
        
        # --- TRAIN-ONLY KMEANS (NO REGIME LEAKAGE) ---
        # --- Train KMeans only on training data (no leakage) ---
        train_mask = ~df_final['Report_Date'].dt.year.isin([2022, 2025])
        kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
        kmeans.fit(df_final.loc[train_mask, ['fwd_ret']])

        # Predict cluster labels for all events
        clusters = kmeans.predict(df_final[['fwd_ret']])

        # Sort cluster centers to map consistently to 0,1,2
        centers = kmeans.cluster_centers_.flatten()
        sorted_indices = np.argsort(centers)
        label_map = {sorted_indices[0]: 0, sorted_indices[1]: 1, sorted_indices[2]: 2}

        # Apply mapping to get final target
        df_final['target'] = pd.Series(clusters).map(label_map).values

        
        print(f"K-Means cluster centers (sorted):")
        for i, center in enumerate(sorted(centers)):
            print(f"  Cluster {i}: {center:.4f} avg return")
        
    else:
        print("\n--- Using Hardcoded Thresholds (±12%) ---")
        df_final['target'] = 1
        df_final.loc[df_final['fwd_ret'] > 0.12, 'target'] = 2
        df_final.loc[df_final['fwd_ret'] < -0.12, 'target'] = 0
    
    # Final sanitize for XGBoost
    df_final[FEATURES] = np.clip(
        df_final[FEATURES].replace([np.inf, -np.inf], np.nan).fillna(0), 
        -10, 10
    )
    
    print(f"\nDataset complete. Shape: {df_final.shape}")
    print(f"Target distribution:\n{df_final['target'].value_counts()}")
    
    df_final.to_csv("FINAL_MEGA_CLEAN.csv", index=False)
    return df_final

def train_cuda(df):
    if df.empty:
        print("No data to train on.")
        return

    print("\n--- Phase 2: GPU Training (hist tree method) ---")
    
    train_df = df[~df['Report_Date'].dt.year.isin([2022, 2025])]
    test_df  = df[df['Report_Date'].dt.year.isin([2022, 2025])]
    
    EMBARGO = 90

    test_start = test_df['Report_Date'].min()

    train_df = train_df[
    train_df['Report_Date'] < (test_start - pd.Timedelta(days=EMBARGO))
]

    print(f"Train set: {len(train_df)} records")
    print(f"Test set: {len(test_df)} records")
    
    if test_df.empty:
        print("Test set is empty. Check date ranges.")
        return

    X_train, X_val, y_train, y_val = train_test_split(
        train_df[FEATURES], train_df['target'], test_size=0.1, shuffle=False
    )

    model = xgb.XGBClassifier(
        tree_method='hist',
        device='cuda',
        n_estimators=1000, 
        learning_rate=0.01, 
        max_depth=6,
        objective='multi:softmax', 
        num_class=3,
        subsample=0.8,
        colsample_bytree=0.8
    )

    model.fit(
        X_train, y_train, 
        eval_set=[(X_val, y_val)], 
        verbose=100
    )

    preds = model.predict(test_df[FEATURES])
    print("\nMODEL PERFORMANCE ON WITHHELD DATA (2022 & 2025):\n")
    print(classification_report(test_df['target'], preds))
    
    print("\nTop 10 Feature Importances:")
    importances = pd.DataFrame({
        'feature': FEATURES,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(importances.head(10))

def backtest_per_ticker(df_path="FINAL_MEGA_CLEAN.csv"):
    """
    Run isolated per-ticker backtest and plot cumulative returns.
    """
    df = pd.read_csv(df_path, parse_dates=['Report_Date', 'Price_Date'])

    # Filter withheld test set
    test_df = df[df['Report_Date'].dt.year.isin([2022, 2025])].copy()
    if test_df.empty:
        raise ValueError("No test data found for 2022/2025!")

    # Cap returns for sanity
    test_df['fwd_ret_capped'] = np.clip(test_df['fwd_ret'], -0.8, 1.0)

    tickers = test_df['Ticker'].unique()
    cumrets_per_ticker = {}

    plt.figure(figsize=(14,7))
    for t in tickers:
        df_t = test_df[test_df['Ticker']==t].sort_values('Report_Date')
        if df_t.empty:
            continue
        # Compute cumulative return per ticker
        cumrets = (1 + df_t['fwd_ret_capped']).cumprod() - 1
        cumrets_per_ticker[t] = cumrets.values
        plt.plot(cumrets.values, alpha=0.3, label=t)

    plt.title("Per-Ticker Isolated Cumulative Returns (Test Set)")
    plt.xlabel("Events (chronological)")
    plt.ylabel("Cumulative Return")
    plt.grid(True)
    plt.show()

    # Summary stats
    summary = test_df.groupby('Ticker')['fwd_ret_capped'].agg(
        count='count',
        mean='mean',
        median='median',
        std='std',
        min='min',
        max='max'
    ).sort_values('mean', ascending=False).reset_index()
    print("\nPer-Ticker Forward Return Summary (Test Set):")
    print(summary.head(20))
    
    return test_df, cumrets_per_ticker, summary




if __name__ == "__main__":
    data = build_mega_dataset(use_kmeans_labels=True)
    train_cuda(data)
    test_df, cumrets_per_ticker, summary = backtest_per_ticker()
