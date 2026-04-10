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
    
    -- Price Indicators
    atr_14_simple DOUBLE PRECISION,
    atr_14_smoothed DOUBLE PRECISION,
    sma_50 DOUBLE PRECISION,
    sma_200 DOUBLE PRECISION,

    -- Volume Baselines (The Denominators)
    vol_ema_5 DOUBLE PRECISION,
    vol_sma_10 DOUBLE PRECISION,
    vol_ema_21 DOUBLE PRECISION,
    vol_sma_60 DOUBLE PRECISION,

    -- Relative Volume (RVOL) Metrics
    rvol_ema_5 DOUBLE PRECISION,
    rvol_sma_10 DOUBLE PRECISION,
    rvol_ema_21 DOUBLE PRECISION,
    rvol_sma_60 DOUBLE PRECISION,

    -- Final Ranking Metric
    attention_score DOUBLE PRECISION,

    CONSTRAINT pk_ticker_indicator_date PRIMARY KEY (ticker, market_date)
);

-- Indexes to drastically speed up Pandas pulling historical chunks
CREATE INDEX idx_daily_market_data_date ON daily_market_data(market_date);
CREATE INDEX idx_daily_market_data_ticker ON daily_market_data(ticker);
