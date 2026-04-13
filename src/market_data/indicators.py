import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from .database import copy_to_sql_with_progress


def run_python_indicator_pipeline(db_url, target_date=None):
    """
    Calculates ATR, SMAs, EMAs, Bollinger Bands, Keltner Channels, Gap %, RVOL, RSI, MACD, OBV, and ADX entirely in Pandas.
    If target_date is set, runs in Daily Mode. If None, runs in Bulk Reset Mode.
    """
    engine = create_engine(db_url)

    if target_date:
        print(f"\n[INDICATORS] Calculating for {target_date} using Pandas...")
        # Daily Mode: Pull 400 days of history to give moving averages runway to calculate
        query = text("""
            SELECT ticker, market_date, open, high, low, close, volume
            FROM daily_market_data
            WHERE market_date >= (CAST(:dt AS DATE) - INTERVAL '400 days')
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
        # Fetch the most recent historical OBV and the exact date it occurred
        yesterday_obv_query = text("""
            SELECT DISTINCT ON (ticker) ticker, market_date as anchor_date, obv as anchor_obv
            FROM daily_indicators
            WHERE market_date < CAST(:dt AS DATE)
            ORDER BY ticker, market_date DESC
        """)
        anchor_obv_df = pd.read_sql(
            yesterday_obv_query, engine, params={"dt": target_date}
        )

        df = df.merge(anchor_obv_df, on="ticker", how="left")

        # 1. Calculate a naive cumulative sum over the available 400-day window
        df["naive_obv"] = df.groupby("ticker")["obv_change"].cumsum()

        # 2. Find the exact row where our raw data date intersects the DB's latest indicator date
        is_anchor = df["market_date"].astype(str) == df["anchor_date"].astype(str)

        # 3. Calculate the absolute offset specifically at that exact intersection anchor point
        anchor_rows = df[is_anchor].copy()
        anchor_rows["offset"] = anchor_rows["anchor_obv"] - anchor_rows["naive_obv"]

        # 4. Seamlessly broadcast the offset to all rows for each ticker using map
        offsets = anchor_rows.set_index("ticker")["offset"]
        df["obv"] = df["naive_obv"] + df["ticker"].map(offsets).fillna(0)

        # Clean up temporary columns
        df = df.drop(columns=["naive_obv", "anchor_date", "anchor_obv"])
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
    df["minus_di"] = (100 * smoothed_minus_dm / df["atr_14"].replace(0, np.nan)).round(
        2
    )

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
    df["rvol_ema_5"] = (df["volume"] / df["vol_ema_5"].replace(0, float("nan"))).round(
        2
    )
    df["rvol_sma_10"] = (
        df["volume"] / df["vol_sma_10"].replace(0, float("nan"))
    ).round(2)
    df["rvol_ema_21"] = (
        df["volume"] / df["vol_ema_21"].replace(0, float("nan"))
    ).round(2)
    df["rvol_sma_60"] = (
        df["volume"] / df["vol_sma_60"].replace(0, float("nan"))
    ).round(2)

    # --- 6. DoD, WoW, and MoM Rate of Change Calculations ---

    # RVOL DoD Shift
    df["rvol_ema_5_dod_diff"] = (
        df["rvol_ema_5"] - grouped_ticker["rvol_ema_5"].shift(1)
    ).round(2)

    # Volume Surge: Percentage change in raw volume compared to yesterday
    df["volume_dod_pct"] = (
        (
            (df["volume"] - grouped_ticker["volume"].shift(1))
            / grouped_ticker["volume"].shift(1).replace(0, float("nan"))
        )
        * 100
    ).round(2)

    # Momentum Velocity: Absolute difference in RSI
    df["rsi_14_dod_diff"] = (df["rsi_14"] - grouped_ticker["rsi_14"].shift(1)).round(2)

    # --- NEW: Smoothed Velocity ---
    df["volume_dod_sma_3"] = (
        grouped_ticker["volume_dod_pct"]
        .transform(lambda x: x.rolling(3, min_periods=1).mean())
        .round(2)
    )

    df["rsi_velocity_3d"] = (
        grouped_ticker["rsi_14_dod_diff"]
        .transform(lambda x: x.rolling(3, min_periods=1).mean())
        .round(2)
    )

    # Trend Acceleration: Difference in MACD Histogram
    df["macd_hist_dod_diff"] = (
        df["macd_hist"] - grouped_ticker["macd_hist"].shift(1)
    ).round(4)

    # Volatility Expansion: Percentage change in ATR
    df["atr_14_dod_pct"] = (
        (
            (df["atr_14"] - grouped_ticker["atr_14"].shift(1))
            / grouped_ticker["atr_14"].shift(1).replace(0, float("nan"))
        )
        * 100
    ).round(2)

    # WoW (Week-over-Week) Tracking - approx 5 trading days
    df["price_change_wow_pct"] = (
        (
            (df["close"] - grouped_ticker["close"].shift(5))
            / grouped_ticker["close"].shift(5).replace(0, float("nan"))
        )
        * 100
    ).round(4)

    df["volume_wow_pct"] = (
        (
            (df["volume"] - grouped_ticker["volume"].shift(5))
            / grouped_ticker["volume"].shift(5).replace(0, float("nan"))
        )
        * 100
    ).round(2)

    # MoM (Month-over-Month) Tracking - approx 21 trading days
    df["price_change_mom_pct"] = (
        (
            (df["close"] - grouped_ticker["close"].shift(21))
            / grouped_ticker["close"].shift(21).replace(0, float("nan"))
        )
        * 100
    ).round(4)

    df["volume_mom_pct"] = (
        (
            (df["volume"] - grouped_ticker["volume"].shift(21))
            / grouped_ticker["volume"].shift(21).replace(0, float("nan"))
        )
        * 100
    ).round(2)

    # --- 6.5 Trend Trajectory (Linear Regression Slopes & R-Squared) ---
    print("\nCalculating Linear Regression Slopes and R-Squared (Vectorized Engine)...")

    # Mathematically, the slope of a linear regression with x=[0, 1, ..., w-1] is:
    # m = Cov(x, y) / Var(x)
    # The Variance of x=[0, 1, ..., w-1] is a mathematically known constant: w(w+1)/12.
    # To handle the DataFrame without slow loops or cross-boundary contamination,
    # we create a rolling grouping index that natively resets per ticker.
    df["_idx"] = df.groupby("ticker").cumcount()

    windows = [3, 5, 10, 21]
    metrics = ["close", "rsi_14", "obv", "macd_hist", "atr_14"]

    # Dictionary to collect new columns and prevent DataFrame fragmentation
    new_columns = {}

    for w in windows:
        print(f"  -> Processing {w}d trajectories...")

        # Calculate sample variance for x = [0, 1, ... w-1]
        var_x = (w * (w + 1)) / 12.0

        # Valid windows MUST not cross the ticker boundary
        is_valid_window = df["_idx"] >= (w - 1)

        for col in metrics:
            # We track standard deviation to prevent zero-division artifacts (flat lines) in R-Squared
            roll_std = df[col].rolling(w).std()

            # --- Calculate Slope ---
            # min_periods=w ensures that ANY NaN in the period results safely in NaN
            cov_xy = df[col].rolling(w, min_periods=w).cov(df["_idx"])
            slope = cov_xy / var_x

            # Nullify any calculations that rolled over boundaries
            slope = np.where(is_valid_window, slope, np.nan)
            new_columns[f"{col}_slope_{w}d"] = np.round(slope, 4)

            # --- Calculate R-Squared ---
            # Mathematically equivalent to np.corrcoef(x, y)[0, 1] ** 2
            r = df[col].rolling(w, min_periods=w).corr(df["_idx"])
            r2 = r**2

            # Handle float inaccuracies near zero (if std is functionally 0, the fit is perfectly flat but R2 equation fractures)
            r2 = np.where(roll_std < 1e-8, 0.0, r2)

            # Nullify boundary overlaps
            r2 = np.where(is_valid_window, r2, np.nan)
            new_columns[f"{col}_r2_{w}d"] = np.round(r2, 4)

    # Attach all new trajectory columns to the main DataFrame at once
    df = pd.concat([df, pd.DataFrame(new_columns, index=df.index)], axis=1)

    # Clean up the mathematical index
    df.drop(columns=["_idx"], inplace=True)

    # --- 7. Filtering and Output ---
    cols_to_keep = [
        "ticker",
        "market_date",
        "close",
        "prev_close",
        "gap_pct",
        "price_change_dod_pct",
        "price_change_wow_pct",
        "price_change_mom_pct",
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
        "volume_wow_pct",
        "volume_mom_pct",
        "rsi_14_dod_diff",
        "macd_hist_dod_diff",
        "atr_14_dod_pct",
        "volume_dod_sma_3",
        "rsi_velocity_3d",
        # --- Trajectory Metrics ---
        "close_slope_3d",
        "close_r2_3d",
        "close_slope_5d",
        "close_r2_5d",
        "close_slope_10d",
        "close_r2_10d",
        "close_slope_21d",
        "close_r2_21d",
        "rsi_14_slope_3d",
        "rsi_14_r2_3d",
        "rsi_14_slope_5d",
        "rsi_14_r2_5d",
        "rsi_14_slope_10d",
        "rsi_14_r2_10d",
        "rsi_14_slope_21d",
        "rsi_14_r2_21d",
        # --- Trajectory Metrics (OBV, MACD, ATR) ---
        "obv_slope_3d",
        "obv_r2_3d",
        "obv_slope_5d",
        "obv_r2_5d",
        "obv_slope_10d",
        "obv_r2_10d",
        "obv_slope_21d",
        "obv_r2_21d",
        "macd_hist_slope_3d",
        "macd_hist_r2_3d",
        "macd_hist_slope_5d",
        "macd_hist_r2_5d",
        "macd_hist_slope_10d",
        "macd_hist_r2_10d",
        "macd_hist_slope_21d",
        "macd_hist_r2_21d",
        "atr_14_slope_3d",
        "atr_14_r2_3d",
        "atr_14_slope_5d",
        "atr_14_r2_5d",
        "atr_14_slope_10d",
        "atr_14_r2_10d",
        "atr_14_slope_21d",
        "atr_14_r2_21d",
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

    copy_to_sql_with_progress(final_df, "daily_indicators", engine)
    print("Indicators successfully updated!")
