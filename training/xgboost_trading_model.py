import os
import pandas as pd
import numpy as np
import xgboost as xgb
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sklearn.metrics import accuracy_score, precision_score, classification_report

def fetch_data(db_url):
    print("Fetching historical data from database...")
    engine = create_engine(db_url)
    query = text("""
        SELECT *
        FROM daily_indicators
        WHERE market_date >= CURRENT_DATE - INTERVAL '2 years'
    """)
    return pd.read_sql(query, engine)

def train_xgboost_model():
    load_dotenv()
    db_url = os.getenv("DB_URL")
    
    if not db_url:
        print("Error: Missing DB_URL environment variable.")
        return

    df = fetch_data(db_url)
    if df.empty:
        print("No data found.")
        return

    print("Preparing data and targets...")

    # 1. Target Creation (Predicting Tomorrow)
    df = df.sort_values(by=['ticker', 'market_date']).reset_index(drop=True)
    df['next_day_return'] = df.groupby('ticker')['price_change_dod_pct'].shift(-1)
    df['target'] = (df['next_day_return'] > 0).astype(int)

    # 2. Select Features
    exclude_cols = [
        'ticker', 'market_date', 'next_day_return', 'target', 
        'price_change_dod_pct', 'open_to_close_pct', 'gap_pct', 'prev_close', 'close'
    ]
    features = [col for col in df.columns if col not in exclude_cols]

    # Drop NaNs
    ml_df = df[features + ['target', 'market_date', 'ticker']].dropna()

    # 3. CHRONOLOGICAL SPLIT (Critical for Finance)
    # Sort entirely by date so the test set is chronologically strictly AFTER the train set
    ml_df = ml_df.sort_values(by='market_date').reset_index(drop=True)
    
    # Let's use the last 20% of trading days as our unseen test set
    split_idx = int(len(ml_df) * 0.8)
    
    train_df = ml_df.iloc[:split_idx]
    test_df = ml_df.iloc[split_idx:]
    
    train_start, train_end = train_df['market_date'].min(), train_df['market_date'].max()
    test_start, test_end = test_df['market_date'].min(), test_df['market_date'].max()

    print(f"\n--- Data Split Summary ---")
    print(f"Train Set: {len(train_df)} rows ({train_start} to {train_end})")
    print(f"Test Set:  {len(test_df)} rows ({test_start} to {test_end})")

    X_train = train_df[features]
    y_train = train_df['target']
    X_test = test_df[features]
    y_test = test_df['target']

    # 4. TRAIN XGBOOST
    print("\nTraining XGBoost Classifier...")
    model = xgb.XGBClassifier(
        n_estimators=300,        # Number of trees
        learning_rate=0.05,      # Slower learning rate to prevent overfitting
        max_depth=5,             # Shallow trees (trading data is noisy)
        subsample=0.8,           # Use 80% of rows per tree
        colsample_bytree=0.8,    # Use 80% of features per tree
        random_state=42,
        n_jobs=-1,
        eval_metric='logloss'
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_test, y_test)],
        verbose=50 # Print progress every 50 trees
    )

    # 5. STANDARD EVALUATION
    preds = model.predict(X_test)
    probs = model.predict_proba(X_test)[:, 1] # Get probabilities for the "Up" class (1)

    print("\n" + "="*50)
    print(" MODEL PERFORMANCE ON UNSEEN FUTURE DATA")
    print("="*50)
    
    # In trading, 50% accuracy is a coin flip. Anything consistent above 53-55% is a massive edge.
    acc = accuracy_score(y_test, preds)
    # Precision is crucial: When the model SAYS it will go up, how often does it actually go up?
    prec = precision_score(y_test, preds)
    
    print(f"Overall Accuracy:  {acc*100:.2f}%")
    print(f"Overall Precision: {prec*100:.2f}%")

    # 6. HIGH-CONFIDENCE TRADING (The Real Value of XGBoost)
    # We don't have to trade every stock. What if we only trade the setups 
    # where the model is > 60% confident it will go up?
    high_confidence_threshold = 0.60
    high_conf_preds = (probs > high_confidence_threshold).astype(int)
    
    # Filter to only look at instances where the model fired a high-confidence signal
    high_conf_indices = np.where(high_conf_preds == 1)[0]
    
    if len(high_conf_indices) > 0:
        high_conf_actuals = y_test.iloc[high_conf_indices]
        high_conf_win_rate = high_conf_actuals.mean()
        
        print("\n" + "="*50)
        print(" HIGH CONFIDENCE TRADES (>60% Probability)")
        print("="*50)
        print(f"Total Trade Signals Fired: {len(high_conf_indices)}")
        print(f"Win Rate on these Trades:  {high_conf_win_rate*100:.2f}%")
    else:
        print(f"\nThe model did not find any setups with >{high_confidence_threshold*100}% confidence.")

if __name__ == "__main__":
    train_xgboost_model()
