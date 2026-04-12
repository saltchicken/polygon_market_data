WITH BaseMetrics AS (
    SELECT 
        ticker,
        market_date,
        -- The original Relative Volume inputs
        rvol_ema_5,
        rvol_sma_10,
        rvol_ema_21,
        rvol_sma_60,
        
        -- The Base Volume Score (Original Formula)
        (
            (0.15 * COALESCE(rvol_ema_5, 0)) +
            (0.15 * COALESCE(rvol_sma_10, 0)) +
            (0.30 * COALESCE(rvol_ema_21, 0)) +
            (0.40 * COALESCE(rvol_sma_60, 0))
        ) AS base_vol_score,
        
        -- Trend Trajectory & Quality Metrics
        COALESCE(close_r2_10d, 0) AS price_trend_quality,
        COALESCE(obv_r2_10d, 0) AS vol_accumulation_quality,
        COALESCE(rsi_14_slope_3d, 0) AS short_term_momentum,
        
        -- Volatility
        atr_14_pct
    FROM daily_indicators
    WHERE market_date = (SELECT MAX(market_date) FROM daily_indicators)
)
SELECT 
    bm.ticker,
    fd.name,
    fd.sector,
    bm.market_date,
    ROUND(CAST(bm.atr_14_pct AS NUMERIC), 2) AS atr_14_pct,
    ROUND(CAST(bm.base_vol_score AS NUMERIC), 2) AS base_vol_score,
    ROUND(CAST(bm.price_trend_quality AS NUMERIC), 2) AS close_r2_10d,
    ROUND(CAST(bm.vol_accumulation_quality AS NUMERIC), 2) AS obv_r2_10d,
    ROUND(CAST(bm.short_term_momentum AS NUMERIC), 2) AS rsi_slope_3d,
    
    -- ==========================================
    -- THE ENHANCED DYNAMIC ATTENTION SCORE
    -- ==========================================
    -- 1. Base Score: Start with the weighted Relative Volume.
    -- 2. Price Quality Boost: Up to 50% multiplier if the price is moving in a clean, linear trend (high R^2).
    -- 3. Volume Quality Boost: Up to 50% multiplier if On-Balance-Volume is trending cleanly.
    -- 4. Momentum Kick: A small flat bonus if the 3-day RSI is actively accelerating upward.
    ROUND(CAST(
        (bm.base_vol_score 
         * (1.0 + (bm.price_trend_quality * 0.5)) 
         * (1.0 + (bm.vol_accumulation_quality * 0.5))) 
         + GREATEST(0, bm.short_term_momentum * 0.1) 
    AS NUMERIC), 2) AS enhanced_attention_score

FROM BaseMetrics bm
LEFT JOIN financedatabase fd 
    ON bm.ticker = fd.ticker
-- Minimum threshold: We only want to rank stocks that actually have above-average volume
WHERE bm.base_vol_score > 1.0
  AND fd.country = 'United States'
  -- AND fd.asset_class = 'Equity'
  AND fd.sector = 'Energy'
  AND atr_14_pct > 8.0
ORDER BY enhanced_attention_score DESC
LIMIT 50;

