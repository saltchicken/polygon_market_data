SELECT 
    dmd.ticker,
    dmd.market_date,
    dmd.close,
    dmd.volume,
    fd.name,
    fd.asset_class,
    fd.sector,
    fd.industry_group,
    fd.industry,
    fd.exchange,
    fd.market,
    fd.country,
	fd.currency,
    fd.market_cap,
	fd.summary
FROM daily_market_data dmd
LEFT JOIN financedatabase fd 
    ON dmd.ticker = fd.ticker
WHERE dmd.market_date = (SELECT MAX(market_date) FROM daily_market_data)
AND fd.name IS NOT NULL
AND fd.sector IS NOT NULL
ORDER BY dmd.volume DESC;