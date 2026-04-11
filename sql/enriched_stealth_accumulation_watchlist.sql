SELECT 
    di.ticker,
    fd.name,
    fd.sector,
    fd.industry,
    di.vol_5_21_dist_pct,
    di.atr_5_21_dist_pct,
    di.sma_50_dist_pct,
    di.rsi_14
FROM daily_indicators di
LEFT JOIN financedatabase fd 
    ON di.ticker = fd.ticker
WHERE di.market_date = (SELECT MAX(market_date) FROM daily_indicators)
  AND di.vol_5_21_dist_pct > 25.0 
  AND di.atr_5_21_dist_pct < -20.0 
  AND ABS(di.sma_50_dist_pct) < 4.0
  AND di.rsi_14 BETWEEN 45 AND 55
ORDER BY di.vol_5_21_dist_pct DESC;
