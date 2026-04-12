DROP TABLE IF EXISTS daily_market_data CASCADE;
DROP TABLE IF EXISTS daily_indicators CASCADE;

CREATE TABLE daily_market_data (
    ticker TEXT NOT NULL,
    market_date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    vwap DOUBLE PRECISION,
    timestamp BIGINT,
    transactions DOUBLE PRECISION,
    datetime TIMESTAMP WITHOUT TIME ZONE,

    CONSTRAINT pk_ticker_date PRIMARY KEY (ticker, market_date)
);

CREATE TABLE daily_indicators (
    ticker TEXT NOT NULL,
    market_date DATE NOT NULL,
    
    -- Price & Trend Indicators
    prev_close DOUBLE PRECISION,
    gap_pct DOUBLE PRECISION,
    price_change_dod_pct DOUBLE PRECISION,
    open_to_close_pct DOUBLE PRECISION,
    atr_14 DOUBLE PRECISION,
    atr_14_pct DOUBLE PRECISION,
    atr_5 DOUBLE PRECISION,
    atr_21 DOUBLE PRECISION,
    atr_5_21_dist_pct DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    sma_50_dist_pct DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,
    sma_200_dist_pct DOUBLE PRECISION,
    ema_9 DOUBLE PRECISION,
    ema_21 DOUBLE PRECISION,
    ema_9_21_dist_pct DOUBLE PRECISION,
    
    -- Bollinger Bands
    bb_mid DOUBLE PRECISION,
    bb_upper DOUBLE PRECISION,
    bb_lower DOUBLE PRECISION,
    
    -- Keltner Channels
    kc_mid DOUBLE PRECISION,
    kc_upper DOUBLE PRECISION,
    kc_lower DOUBLE PRECISION,

    -- Oscillators & Momentum
    rsi_14 DOUBLE PRECISION,
    rsi_5 DOUBLE PRECISION,
    rsi_21 DOUBLE PRECISION,
    rsi_5_21_diff DOUBLE PRECISION,
    macd DOUBLE PRECISION,
    macd_signal DOUBLE PRECISION,
    macd_hist DOUBLE PRECISION,
    
    -- Trend Strength (ADX) & Cumulative Volume (OBV)
    adx_14 DOUBLE PRECISION,
    plus_di DOUBLE PRECISION,
    minus_di DOUBLE PRECISION,
    obv DOUBLE PRECISION,

    -- Volume Baselines (The Denominators)
    vol_ema_5 DOUBLE PRECISION,
    vol_sma_10 DOUBLE PRECISION,
    vol_ema_21 DOUBLE PRECISION,
    vol_sma_60 DOUBLE PRECISION,
    vol_5_21_dist_pct DOUBLE PRECISION,

    -- Relative Volume (RVOL) Metrics
    rvol_ema_5 DOUBLE PRECISION,
    rvol_sma_10 DOUBLE PRECISION,
    rvol_ema_21 DOUBLE PRECISION,
    rvol_sma_60 DOUBLE PRECISION,
    
    -- Day-Over-Day (DoD) Rate of Change Metrics
    rvol_ema_5_dod_diff DOUBLE PRECISION,
    volume_dod_pct DOUBLE PRECISION,
    rsi_14_dod_diff DOUBLE PRECISION,
    macd_hist_dod_diff DOUBLE PRECISION,
    atr_14_dod_pct DOUBLE PRECISION,

    CONSTRAINT pk_ticker_indicator_date PRIMARY KEY (ticker, market_date)
);

-- Indexes to drastically speed up Pandas pulling historical chunks
CREATE INDEX idx_daily_market_data_date ON daily_market_data(market_date);
CREATE INDEX idx_daily_market_data_ticker ON daily_market_data(ticker);
