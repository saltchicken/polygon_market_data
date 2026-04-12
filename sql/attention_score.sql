SELECT 
    ticker,
    market_date,
    rvol_ema_5,
    rvol_sma_10,
    rvol_ema_21,
    rvol_sma_60,
    -- The Attention Score Formula
    ROUND(CAST(
        (0.15 * COALESCE(rvol_ema_5, 0)) +
        (0.15 * COALESCE(rvol_sma_10, 0)) +
        (0.30 * COALESCE(rvol_ema_21, 0)) +
        (0.40 * COALESCE(rvol_sma_60, 0))
    AS NUMERIC), 2) AS dynamic_attention_score
FROM daily_indicators
WHERE market_date = (SELECT MAX(market_date) FROM daily_indicators)
ORDER BY dynamic_attention_score DESC
LIMIT 50;
