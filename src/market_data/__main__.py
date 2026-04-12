import os
import sys
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


def init_database(db_url):
    """Executes a SQL file to initialize the database schema."""
    sql_file_path = os.path.join(os.path.dirname(__file__), "sql", "init_schema.sql")

    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file not found at {sql_file_path}")
        return

    try:
        engine = create_engine(db_url)
        with engine.begin() as conn:
            with open(sql_file_path, "r") as file:
                sql_script = file.read()
                conn.execute(text(sql_script))
        print(f"Successfully initialized database schema.")
    except Exception as e:
        print(f"Database Initialization Error: {e}")


def get_entire_market_ohlcv(date, api_key):
    """Fetches daily OHLCV for the entire US stock market for a specific date."""
    client = RESTClient(api_key)

    try:
        all_market_data = client.get_grouped_daily_aggs(date)
        if not all_market_data:
            return None

        print(f"--- Successfully pulled {len(all_market_data)} tickers for {date} ---")

        data_dicts = [
            {
                "ticker": getattr(agg, "ticker", None),
                "open": getattr(agg, "open", None),
                "high": getattr(agg, "high", None),
                "low": getattr(agg, "low", None),
                "close": getattr(agg, "close", None),
                "volume": getattr(agg, "volume", None),
                "vwap": getattr(agg, "vwap", None),
                "timestamp": getattr(agg, "timestamp", None),
                "transactions": getattr(agg, "transactions", None),
            }
            for agg in all_market_data
        ]

        df = pd.DataFrame(data_dicts)
        if "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df

    except Exception as e:
        print(f"Client Error: {e}")
        return None


def upload_to_postgres(df, table_name, db_url):
    """Uploads a pandas DataFrame to a PostgreSQL database."""
    try:
        engine = create_engine(db_url)
        df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
        print(f"Uploaded {len(df)} rows to '{table_name}'.")
    except IntegrityError:
        print(
            "Upload Skipped: Data for this date already exists (Primary Key constraint)."
        )
    except Exception as e:
        if "UniqueViolation" in str(e) or "duplicate key" in str(e):
            print("Upload Skipped: Data for this date already exists.")
        else:
            print(f"Database Upload Error: {str(e)[:200]}")


def fetch_and_upload(target_date, db_url, api_key):
    entire_market_data = get_entire_market_ohlcv(target_date, api_key)
    if entire_market_data is not None and not entire_market_data.empty:
        entire_market_data["market_date"] = pd.to_datetime(target_date).date()
        upload_to_postgres(
            df=entire_market_data, table_name="daily_market_data", db_url=db_url
        )
    else:
        print(f"No market data found for {target_date}.")


def run_python_indicator_pipeline(db_url, target_date=None):
    """
    Calculates ATR, SMAs, EMAs, Bollinger Bands, Keltner Channels, Gap %, RVOL, RSI, MACD, OBV, and ADX entirely in Pandas.
    If target_date is set, runs in Daily Mode. If None, runs in Bulk Reset Mode.
    """
    engine = create_engine(db_url)

    if target_date:
        print(f"\n[INDICATORS] Calculating for {target_date} using Pandas...")
        # Daily Mode: Pull 300 days of history to give moving averages runway to calculate
        query = text("""
            SELECT ticker, market_date, open, high, low, close, volume
            FROM daily_market_data
            WHERE market_date >= (CAST(:dt AS DATE) - INTERVAL '300 days')
              AND market_date <= CAST(:dt AS DATE)
        """)
        df = pd.read_sql(query, engine, params={"dt": target_date})
    else:
        print(f"\n[INDICATORS] Bulk calculating entire database using Pandas...")
        query = text(
            "SELECT ticker, market_date, open, high, low, close, volume FROM daily_market_data"
        )
        df = pd.read_sql(query, engine)

    if df.empty:
        print("No data found to calculate indicators.")
        return

    # Ensure chronologically sorted for moving averages and shift operations
    df = df.sort_values(by=["ticker", "market_date"]).reset_index(drop=True)

    # Base grouping object for performance
    grouped_ticker = df.groupby("ticker")

    # --- 1. True Range & Gap Calculation ---
    df["prev_close"] = grouped_ticker["close"].shift(1)

    # Gap % Calculation
    df["gap_pct"] = (
        ((df["open"] - df["prev_close"]) / df["prev_close"].replace(0, float("nan")))
        * 100
    ).round(4)

    # Price Change % Calculation (Close vs Prev Close) -> Updated to DoD
    df["price_change_dod_pct"] = (
        ((df["close"] - df["prev_close"]) / df["prev_close"].replace(0, float("nan")))
        * 100
    ).round(4)

    # Open to Close % Calculation (Close vs Open)
    df["open_to_close_pct"] = (
        ((df["close"] - df["open"]) / df["open"].replace(0, float("nan"))) * 100
    ).round(4)

    df["tr0"] = df["high"] - df["low"]
    df["tr1"] = (df["high"] - df["prev_close"]).abs()
    df["tr2"] = (df["low"] - df["prev_close"]).abs()
    df["true_range"] = df[["tr0", "tr1", "tr2"]].max(axis=1)

    # --- 2. Baselines Calculation ---
    grouped_tr = df.groupby("ticker")["true_range"]
    grouped_vol = df.groupby("ticker")["volume"]
    grouped_close = df.groupby("ticker")["close"]

    # Price Indicators
    df["atr_14"] = (
        grouped_tr.ewm(alpha=1 / 14, min_periods=14, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )

    df["atr_14_pct"] = (
        (df["atr_14"] / df["close"].replace(0, float("nan"))) * 100
    ).round(4)

    df["atr_5"] = (
        grouped_tr.ewm(alpha=1 / 5, min_periods=5, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )

    df["atr_21"] = (
        grouped_tr.ewm(alpha=1 / 21, min_periods=21, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )

    df["atr_5_21_dist_pct"] = (
        ((df["atr_5"] - df["atr_21"]) / df["atr_21"].replace(0, float("nan"))) * 100
    ).round(2)

    # SMAs
    df["sma_50"] = (
        grouped_close.rolling(50, min_periods=50)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )

    df["sma_50_dist_pct"] = (
        ((df["close"] - df["sma_50"]) / df["sma_50"].replace(0, float("nan"))) * 100
    ).round(2)

    df["sma_200"] = (
        grouped_close.rolling(200, min_periods=200)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )

    df["sma_200_dist_pct"] = (
        ((df["close"] - df["sma_200"]) / df["sma_200"].replace(0, float("nan"))) * 100
    ).round(2)

    # EMAs
    df["ema_9"] = (
        grouped_close.ewm(span=9, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )
    df["ema_21"] = (
        grouped_close.ewm(span=21, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )

    df["ema_9_21_dist_pct"] = (
        ((df["ema_9"] - df["ema_21"]) / df["ema_21"].replace(0, float("nan"))) * 100
    ).round(2)

    # MACD
    ema_12 = (
        grouped_close.ewm(span=12, adjust=False).mean().reset_index(level=0, drop=True)
    )
    ema_26 = (
        grouped_close.ewm(span=26, adjust=False).mean().reset_index(level=0, drop=True)
    )
    df["macd"] = (ema_12 - ema_26).round(4)

    df["macd_signal"] = (
        df.groupby("ticker")["macd"]
        .ewm(span=9, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )
    df["macd_hist"] = (df["macd"] - df["macd_signal"]).round(4)

    # Bollinger Bands
    sma_20 = (
        grouped_close.rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    )
    std_20 = (
        grouped_close.rolling(20, min_periods=20).std().reset_index(level=0, drop=True)
    )
    df["bb_mid"] = sma_20.round(4)
    df["bb_upper"] = (sma_20 + (2 * std_20)).round(4)
    df["bb_lower"] = (sma_20 - (2 * std_20)).round(4)

    # Keltner Channels
    atr_10 = (
        grouped_tr.ewm(alpha=1 / 10, min_periods=10, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
    )
    df["kc_mid"] = (
        grouped_close.ewm(span=20, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(4)
    )
    df["kc_upper"] = (df["kc_mid"] + (2 * atr_10)).round(4)
    df["kc_lower"] = (df["kc_mid"] - (2 * atr_10)).round(4)

    # Volume Baselines
    df["vol_ema_5"] = (
        grouped_vol.ewm(span=5, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(2)
    )
    df["vol_sma_10"] = (
        grouped_vol.rolling(10, min_periods=10)
        .mean()
        .reset_index(level=0, drop=True)
        .round(2)
    )
    df["vol_ema_21"] = (
        grouped_vol.ewm(span=21, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(2)
    )
    df["vol_sma_60"] = (
        grouped_vol.rolling(60, min_periods=60)
        .mean()
        .reset_index(level=0, drop=True)
        .round(2)
    )

    df["vol_5_21_dist_pct"] = (
        (
            (df["vol_ema_5"] - df["vol_ema_21"])
            / df["vol_ema_21"].replace(0, float("nan"))
        )
        * 100
    ).round(2)

    # --- 3. RSI Calculations ---
    delta = df["close"] - df["prev_close"]
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)

    def calculate_rsi(periods):
        roll_up = (
            up.groupby(df["ticker"])
            .ewm(alpha=1 / periods, min_periods=periods, adjust=False)
            .mean()
            .reset_index(level=0, drop=True)
        )
        roll_down = (
            down.groupby(df["ticker"])
            .ewm(alpha=1 / periods, min_periods=periods, adjust=False)
            .mean()
            .reset_index(level=0, drop=True)
        )
        rs = roll_up / roll_down
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi.mask(roll_down == 0, 100.0).round(2)

    df["rsi_14"] = calculate_rsi(14)
    df["rsi_5"] = calculate_rsi(5)
    df["rsi_21"] = calculate_rsi(21)
    df["rsi_5_21_diff"] = (df["rsi_5"] - df["rsi_21"]).round(2)

    # --- 4. Cumulative & Trend Strength (OBV, ADX) ---
    direction = np.sign(df["close"] - df["prev_close"]).fillna(0)
    df["obv_change"] = direction * df["volume"]

    if target_date:
        yesterday_obv_query = text("""
            SELECT ticker, obv as prev_obv
            FROM daily_indicators
            WHERE market_date = (
                SELECT MAX(market_date) 
                FROM daily_indicators 
                WHERE market_date < CAST(:dt AS DATE)
            )
        """)
        prev_obv_df = pd.read_sql(
            yesterday_obv_query, engine, params={"dt": target_date}
        )

        df = df.merge(prev_obv_df, on="ticker", how="left")
        df["prev_obv"] = df["prev_obv"].fillna(0)

        is_target = df["market_date"].astype(str) == target_date
        df["obv"] = 0.0  
        df.loc[is_target, "obv"] = (
            df.loc[is_target, "prev_obv"] + df.loc[is_target, "obv_change"]
        )
    else:
        df["obv"] = df.groupby("ticker")["obv_change"].cumsum()

    # ADX Calculation
    grouped_high = df.groupby("ticker")["high"]
    grouped_low = df.groupby("ticker")["low"]

    df["prev_high"] = grouped_high.shift(1)
    df["prev_low"] = grouped_low.shift(1)

    up_move = df["high"] - df["prev_high"]
    down_move = df["prev_low"] - df["low"]

    df["plus_dm"] = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    df["minus_dm"] = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    smoothed_plus_dm = (
        df.groupby("ticker")["plus_dm"]
        .ewm(alpha=1 / 14, min_periods=14, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
    )
    smoothed_minus_dm = (
        df.groupby("ticker")["minus_dm"]
        .ewm(alpha=1 / 14, min_periods=14, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["plus_di"] = (100 * smoothed_plus_dm / df["atr_14"].replace(0, np.nan)).round(2)
    df["minus_di"] = (100 * smoothed_minus_dm / df["atr_14"].replace(0, np.nan)).round(2)

    df["dx"] = 100 * (
        np.abs(df["plus_di"] - df["minus_di"])
        / (df["plus_di"] + df["minus_di"]).replace(0, np.nan)
    )
    df["adx_14"] = (
        df.groupby("ticker")["dx"]
        .ewm(alpha=1 / 14, min_periods=14, adjust=False)
        .mean()
        .reset_index(level=0, drop=True)
        .round(2)
    )

    # --- 5. RVOL Calculations ---
    df["rvol_ema_5"] = (df["volume"] / df["vol_ema_5"].replace(0, float("nan"))).round(2)
    df["rvol_sma_10"] = (df["volume"] / df["vol_sma_10"].replace(0, float("nan"))).round(2)
    df["rvol_ema_21"] = (df["volume"] / df["vol_ema_21"].replace(0, float("nan"))).round(2)
    df["rvol_sma_60"] = (df["volume"] / df["vol_sma_60"].replace(0, float("nan"))).round(2)

    # --- 6. DoD, WoW, and MoM Rate of Change Calculations ---
    
    # RVOL DoD Shift
    df["rvol_ema_5_dod_diff"] = (
        df["rvol_ema_5"] - grouped_ticker["rvol_ema_5"].shift(1)
    ).round(2)

    # Volume Surge: Percentage change in raw volume compared to yesterday
    df["volume_dod_pct"] = (
        ((df["volume"] - grouped_ticker["volume"].shift(1)) 
        / grouped_ticker["volume"].shift(1).replace(0, float("nan"))) * 100
    ).round(2)

    # Momentum Velocity: Absolute difference in RSI
    df["rsi_14_dod_diff"] = (
        df["rsi_14"] - grouped_ticker["rsi_14"].shift(1)
    ).round(2)

    # Trend Acceleration: Difference in MACD Histogram
    df["macd_hist_dod_diff"] = (
        df["macd_hist"] - grouped_ticker["macd_hist"].shift(1)
    ).round(4)

    # Volatility Expansion: Percentage change in ATR
    df["atr_14_dod_pct"] = (
        ((df["atr_14"] - grouped_ticker["atr_14"].shift(1)) 
        / grouped_ticker["atr_14"].shift(1).replace(0, float("nan"))) * 100
    ).round(2)
    
    # WoW (Week-over-Week) Tracking - approx 5 trading days
    df["price_change_wow_pct"] = (
        ((df["close"] - grouped_ticker["close"].shift(5)) 
         / grouped_ticker["close"].shift(5).replace(0, float("nan"))) * 100
    ).round(4)
    
    df["volume_wow_pct"] = (
        ((df["volume"] - grouped_ticker["volume"].shift(5)) 
         / grouped_ticker["volume"].shift(5).replace(0, float("nan"))) * 100
    ).round(2)

    # MoM (Month-over-Month) Tracking - approx 21 trading days
    df["price_change_mom_pct"] = (
        ((df["close"] - grouped_ticker["close"].shift(21)) 
         / grouped_ticker["close"].shift(21).replace(0, float("nan"))) * 100
    ).round(4)
    
    df["volume_mom_pct"] = (
        ((df["volume"] - grouped_ticker["volume"].shift(21)) 
         / grouped_ticker["volume"].shift(21).replace(0, float("nan"))) * 100
    ).round(2)


    # --- 7. Filtering and Output ---
    cols_to_keep = [
        "ticker",
        "market_date",
        "close",
        "prev_close",
        "gap_pct",
        "price_change_dod_pct", 
        "price_change_wow_pct", # <--- Added WoW Price
        "price_change_mom_pct", # <--- Added MoM Price
        "open_to_close_pct",
        "atr_14",
        "atr_14_pct",
        "atr_5",
        "atr_21",
        "atr_5_21_dist_pct",
        "sma_50",
        "sma_50_dist_pct",
        "sma_200",
        "sma_200_dist_pct",
        "ema_9",
        "ema_21",
        "ema_9_21_dist_pct",
        "macd",
        "macd_signal",
        "macd_hist",
        "bb_mid",
        "bb_upper",
        "bb_lower",
        "kc_mid",
        "kc_upper",
        "kc_lower",
        "rsi_14",
        "rsi_5",
        "rsi_21",
        "rsi_5_21_diff",
        "adx_14",
        "plus_di",
        "minus_di",
        "obv",
        "vol_ema_5",
        "vol_sma_10",
        "vol_ema_21",
        "vol_sma_60",
        "vol_5_21_dist_pct",
        "rvol_ema_5",
        "rvol_sma_10",
        "rvol_ema_21",
        "rvol_sma_60",
        "rvol_ema_5_dod_diff",
        "volume_dod_pct",      
        "volume_wow_pct",       # <--- Added WoW Volume
        "volume_mom_pct",       # <--- Added MoM Volume
        "rsi_14_dod_diff",     
        "macd_hist_dod_diff",  
        "atr_14_dod_pct"       
    ]
    final_df = df[cols_to_keep].copy()

    if target_date:
        final_df = final_df[final_df["market_date"].astype(str) == target_date]

    final_df = final_df.dropna(
        subset=["atr_14", "sma_50", "sma_200", "rvol_ema_5", "rsi_14"], how="all"
    )

    if final_df.empty:
        print("No new calculated indicators to upload.")
        return

    print(f"Uploading {len(final_df)} calculated records to database...")

    with engine.begin() as conn:
        if target_date:
            conn.execute(
                text("DELETE FROM daily_indicators WHERE market_date = :dt"),
                {"dt": target_date},
            )
        else:
            conn.execute(text("TRUNCATE TABLE daily_indicators"))

    final_df.to_sql(
        "daily_indicators", engine, if_exists="append", index=False, chunksize=20000
    )
    print("Indicators successfully updated!")


if __name__ == "__main__":
    load_dotenv()
    API_KEY = os.getenv("POLYGON_API_KEY")
    DB_URL = os.getenv("DB_URL")

    # --- Configuration ---
    RESET_DATABASE = True

    if not API_KEY or not DB_URL:
        print("Error: Missing env variables.")
        sys.exit(1)

    if RESET_DATABASE:
        print("\n=== STARTING 1-YEAR DATABASE RESET ===")
        init_database(DB_URL)

        end_date = datetime.today()
        start_date = end_date - timedelta(days=365)
        dates_to_fetch = pd.bdate_range(start=start_date, end=end_date)

        print(f"\n[PHASE 1] Fetching {len(dates_to_fetch)} days of raw market data...")
        for date_obj in dates_to_fetch:
            target_date = date_obj.strftime("%Y-%m-%d")
            print(f"\n--- Processing Raw Data: {target_date} ---")
            fetch_and_upload(target_date, DB_URL, API_KEY)
            print("Sleeping for 13 seconds to avoid rate limits...")
            time.sleep(13)

        print("\n[PHASE 2] Bulk calculating all indicators...")
        run_python_indicator_pipeline(DB_URL, target_date=None)

        print("\n=== RESET COMPLETE ===")

    else:
        # Standard Daily Run
        TARGET_DATE = datetime.today().strftime("%Y-%m-%d")
        print(f"\n=== RUNNING DAILY UPDATE FOR {TARGET_DATE} ===")

        fetch_and_upload(TARGET_DATE, DB_URL, API_KEY)
        run_python_indicator_pipeline(DB_URL, target_date=TARGET_DATE)
