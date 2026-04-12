import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

def fetch_data_for_analysis(db_url):
    print("Fetching data from the database...")
    engine = create_engine(db_url)
    
    # We pull the last 2 years of data to have a statistically significant 
    # sample without overloading memory.
    query = text("""
        SELECT *
        FROM daily_indicators
        WHERE market_date >= CURRENT_DATE - INTERVAL '2 years'
    """)
    
    df = pd.read_sql(query, engine)
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

    print(f"Data loaded: {len(df)} rows. Preparing targets...")

    # 1. SORTING IS CRITICAL
    # We must sort by ticker and date so our shift() operation applies to the correct days
    df = df.sort_values(by=['ticker', 'market_date']).reset_index(drop=True)

    # 2. CREATE THE TARGET VARIABLE (Predicting TOMORROW)
    # We shift the DoD percentage backwards by 1. 
    # This aligns TODAY'S indicators with TOMORROW'S price change.
    df['next_day_return'] = df.groupby('ticker')['price_change_dod_pct'].shift(-1)
    
    # Convert to Binary Classification: 1 if positive (Up day), 0 if negative/flat (Down day)
    df['target_is_positive'] = (df['next_day_return'] > 0).astype(int)

    # 3. SELECT FEATURES
    # Exclude non-predictive columns (ticker, date) and target/future columns
    exclude_cols = [
        'ticker', 'market_date', 'next_day_return', 'target_is_positive', 
        'price_change_dod_pct', 'open_to_close_pct', 'gap_pct', 'prev_close', 'close'
    ]
    features = [col for col in df.columns if col not in exclude_cols]

    # Drop any rows with NaN values (mostly the most recent day since it has no 'tomorrow' yet, 
    # plus the first 200 days of a stock that lack SMA_200)
    ml_df = df[features + ['target_is_positive']].dropna()

    X = ml_df[features]
    y = ml_df['target_is_positive']

    print(f"Training on {len(X)} historical market days across all tickers...")

    # 4. TRAIN THE RANDOM FOREST
    # Max depth is limited to prevent overfitting on the noise of the stock market
    rf = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42, n_jobs=-1)
    rf.fit(X, y)

    # 5. EXTRACT AND DISPLAY FEATURE IMPORTANCE
    importances = rf.feature_importances_
    
    # Pair feature names with their importance scores
    feature_importance_df = pd.DataFrame({
        'Feature': features,
        'Importance': importances
    }).sort_values(by='Importance', ascending=False).reset_index(drop=True)

    print("\n" + "="*50)
    print(" TOP 15 MOST PREDICTIVE FEATURES (RANDOM FOREST)")
    print("="*50)
    
    for index, row in feature_importance_df.head(15).iterrows():
        # Format as percentage
        print(f"{index + 1}. {row['Feature']:<25} {row['Importance']*100:.2f}%")

    print("\n" + "="*50)
    print(" TOP LINEAR CORRELATIONS (PEARSON)")
    print("="*50)
    # While Random forest captures non-linear relationships, 
    # basic correlation shows simple directional relationships (-1 to 1)
    correlations = ml_df.corr()['target_is_positive'].drop('target_is_positive')
    correlations = correlations.sort_values(key=abs, ascending=False)
    
    for feature, corr_val in correlations.head(10).items():
        direction = "Positive" if corr_val > 0 else "Negative"
        print(f"{feature:<25} {corr_val:+.4f} ({direction})")

    print("\n(Note: In financial markets, correlations above 0.03 are considered significant!)")


if __name__ == "__main__":
    run_feature_analysis()
