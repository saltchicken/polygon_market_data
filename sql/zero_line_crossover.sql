WITH Recent_Data AS (
    SELECT 
        ticker,
        market_date,
        rsi_14,
        rsi_5_21_diff,
        -- Fetch the previous trading day's diff for the same ticker
        LAG(rsi_5_21_diff) OVER (PARTITION BY ticker ORDER BY market_date) as prev_rsi_diff
    FROM daily_indicators
    -- Performance optimization: Only scan the last 10 days so the database 
    -- doesn't try to run a window function on your entire historical dataset
    WHERE market_date >= (SELECT MAX(market_date) - INTERVAL '10 days' FROM daily_indicators)
)
SELECT 
    ticker,
    market_date,
    rsi_14,
    prev_rsi_diff,
    rsi_5_21_diff
FROM Recent_Data
WHERE market_date = (SELECT MAX(market_date) FROM daily_indicators)
  -- The Crossover Logic: Yesterday was negative or flat, Today is positive
  AND prev_rsi_diff <= 0 
  AND rsi_5_21_diff > 0
ORDER BY rsi_5_21_diff DESC;
