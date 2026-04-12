import os
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def fetch_data_for_analysis(db_url):
    print("Fetching data from the database...")
    engine = create_engine(db_url)
    
    query = text("""
        SELECT *
        FROM daily_indicators
        WHERE market_date >= CURRENT_DATE - INTERVAL '2 years'
    """)
    
    df = pd.read_sql(query, engine)
    
    # MEMORY HACK: Downcast float64 to float32
    print("Downcasting float64 columns to float32 to save RAM...")
    float_cols = df.select_dtypes(include=['float64']).columns
    df[float_cols] = df[float_cols].astype('float32')
    
    return df

def run_feature_analysis():
    load_dotenv()
    db_url = os.getenv("DB_URL")
    
    if not db_url:
        print("Error: Missing DB_URL environment variable.")
        return

    df = fetch_data_for_analysis(db_url)
    if df.empty:
        print("No data found.")
        return

    print(f"Data loaded: {len(df)} rows. Preparing T+5 Swing Trade targets...")

    # 1. SORTING IS CRITICAL
    df = df.sort_values(by=['ticker', 'market_date']).reset_index(drop=True)

    # 2. CREATE THE TARGET VARIABLE (Predicting T+5)
    df['future_5d_close'] = df.groupby('ticker')['close'].shift(-5)
    df['future_5d_return_pct'] = ((df['future_5d_close'] - df['close']) / df['close'].replace(0, np.nan)) * 100
    
    # Convert to Binary Classification: 1 if the stock goes up by > 3% in 5 days
    hurdle_rate = 3.0
    df['target_is_positive'] = (df['future_5d_return_pct'] > hurdle_rate).astype(int)

    # 3. SELECT FEATURES
    exclude_cols = [
        'ticker', 'market_date', 'target_is_positive', 'future_5d_close', 'future_5d_return_pct', 
        'price_change_dod_pct', 'open_to_close_pct', 'gap_pct', 'prev_close', 'close'
    ]
    features = [col for col in df.columns if col not in exclude_cols]

    ml_df = df[features + ['target_is_positive']].dropna()

    X = ml_df[features]
    y = ml_df['target_is_positive']

    print(f"Training on {len(X)} historical market days across all tickers...")

    # 4. TRAIN XGBOOST WITH GPU ACCELERATION
    print("Initializing GPU-Accelerated XGBoost...")
    model = xgb.XGBClassifier(
        n_estimators=100, 
        max_depth=5, 
        random_state=42,
        tree_method='hist',  # Histogram binning drastically reduces RAM usage
        device='cuda'        # Shifts computation to NVIDIA GPU
    )
    
    model.fit(X, y)

    # 5. EXTRACT AND DISPLAY FEATURE IMPORTANCE
    importances = model.feature_importances_
    
    feature_importance_df = pd.DataFrame({
        'Feature': features,
        'Importance': importances
    }).sort_values(by='Importance', ascending=False).reset_index(drop=True)

    print("\n" + "="*60)
    print(" TOP 15 MOST PREDICTIVE FEATURES (XGBOOST T+5 SWING TRADE)")
    print("="*60)
    
    for index, row in feature_importance_df.head(15).iterrows():
        print(f"{index + 1}. {row['Feature']:<30} {row['Importance']*100:.2f}%")

    print("\n" + "="*60)
    print(" TOP LINEAR CORRELATIONS (PEARSON)")
    print("="*60)
    
    correlations = ml_df.corr()['target_is_positive'].drop('target_is_positive')
    correlations = correlations.sort_values(key=abs, ascending=False)
    
    for feature, corr_val in correlations.head(10).items():
        direction = "Positive" if corr_val > 0 else "Negative"
        print(f"{feature:<30} {corr_val:+.4f} ({direction})")

    print("\n(Note: In financial markets, correlations above 0.03 are considered significant!)")

if __name__ == "__main__":
    run_feature_analysis()
