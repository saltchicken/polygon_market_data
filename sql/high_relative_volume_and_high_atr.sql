SELECT 
	ticker,
	market_date,
	rvol_ema_21,
	rsi_14,
	atr_14_pct
FROM daily_indicators
WHERE market_date = (SELECT MAX(market_date) FROM daily_indicators)
AND atr_14_pct > 8.0
AND rsi_14 IS NOT NULL AND atr_14_pct IS NOT NULL
ORDER BY
	rvol_ema_21 DESC