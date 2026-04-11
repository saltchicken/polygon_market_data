-- The "Sympathy Play": If the clustering query shows that 12 different Regional Bank stocks are all squeezing with high volume, you know there is a systemic, thematic move happening. You can pick the strongest stock in that group with confidence, knowing the entire sector has a tailwind.
-- Filtering the Noise: If a stock shows up on the main watchlist, but it's the only stock in its sector doing so, the volume surge might just be an isolated corporate event (like an upcoming earnings report or a CEO resignation) rather than broad institutional accumulation.

SELECT 
    fd.sector,
    COUNT(di.ticker) AS stocks_accumulating,
    ROUND(AVG(di.vol_5_21_dist_pct)::numeric, 2) AS avg_volume_surge_pct
FROM daily_indicators di
LEFT JOIN financedatabase fd 
    ON di.ticker = fd.ticker
WHERE di.market_date = (SELECT MAX(market_date) FROM daily_indicators)
  AND di.vol_5_21_dist_pct > 25.0 
  AND di.atr_5_21_dist_pct < -20.0 
  AND ABS(di.sma_50_dist_pct) < 4.0
  AND di.rsi_14 BETWEEN 45 AND 55
  AND fd.sector IS NOT NULL
GROUP BY fd.sector
ORDER BY stocks_accumulating DESC;


