-- Step 1: Ensure your destination table exists
CREATE TABLE IF NOT EXISTS daily_indicators (
    ticker VARCHAR(20),
    market_date DATE,
    atr_14 NUMERIC,
    CONSTRAINT pk_ticker_indicator_date PRIMARY KEY (ticker, market_date)
);

-- Step 2: The ELT Insert Query
WITH raw_data AS (
    -- Pull just the recent data to keep the query lightning fast
    SELECT ticker, market_date, high, low, close
    FROM daily_market_data
    WHERE market_date >= (CAST(:target_date AS DATE) - INTERVAL '30 days')
      AND market_date <= CAST(:target_date AS DATE)
),
true_range_calc AS (
    -- Calculate True Range using the LAG() window function to get yesterday's close
    SELECT 
        ticker,
        market_date,
        -- GREATEST returns the maximum value from a list in SQL
        GREATEST(
            high - low,
            ABS(high - LAG(close) OVER (PARTITION BY ticker ORDER BY market_date)),
            ABS(low - LAG(close) OVER (PARTITION BY ticker ORDER BY market_date))
        ) AS true_range
    FROM raw_data
),
atr_calc AS (
    -- Calculate the Average True Range
    SELECT 
        ticker,
        market_date,
        -- Calculate a 14-day moving average of the True Range
        AVG(true_range) OVER (
            PARTITION BY ticker 
            ORDER BY market_date 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS atr_14
    FROM true_range_calc
)
-- Finally, insert ONLY the target date into our running indicators table
INSERT INTO daily_indicators (ticker, market_date, atr_14)
SELECT ticker, market_date, atr_14
FROM atr_calc
WHERE market_date = CAST(:target_date AS DATE) 
  AND atr_14 IS NOT NULL
-- If run twice on the same day, update instead of duplicating
ON CONFLICT (ticker, market_date) DO UPDATE 
SET atr_14 = EXCLUDED.atr_14;
