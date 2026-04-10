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
    ticker VARCHAR(20),
    market_date DATE,
    atr_14 NUMERIC,
    sma_50 NUMERIC,
    sma_200 NUMERIC,

    CONSTRAINT pk_ticker_indicator_date PRIMARY KEY (ticker, market_date)
);

-- Create an index on the market_date to dramatically speed up 
-- queries where you filter by a specific day (e.g., WHERE market_date = '2026-04-03')
-- CREATE INDEX idx_daily_market_data_date ON daily_market_data(market_date);

-- Create an index on the ticker to speed up querying a single stock's history
-- CREATE INDEX idx_daily_market_data_ticker ON daily_market_data(ticker);
