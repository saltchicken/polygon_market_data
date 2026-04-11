SELECT 
    ticker,
    market_date,
    vol_5_21_dist_pct,
    atr_5_21_dist_pct,
    sma_50_dist_pct,
    rsi_14
FROM daily_indicators
WHERE market_date = (SELECT MAX(market_date) FROM daily_indicators)
  
  -- 1. The Stealth Volume: Short-term baseline volume is at least 25% higher than the monthly average
  AND vol_5_21_dist_pct > 25.0 
  
  -- 2. The Coiled Spring: Volatility is heavily compressed (The Squeeze)
  AND atr_5_21_dist_pct < -20.0 
  
  -- 3. The Launchpad: Price is hovering peacefully near the 50-day moving average
  AND ABS(sma_50_dist_pct) < 4.0
  
  -- 4. Neutral Momentum: RSI is neither overbought nor oversold, indicating price is truly flat
  AND rsi_14 BETWEEN 45 AND 55

ORDER BY vol_5_21_dist_pct DESC;
