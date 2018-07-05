

SELECT f_symbol,f_ex1_name,f_ex1_bid,FROM_UNIXTIME(f_ex1_ts/1000, '%T') as f_ex1_ts,f_ex1_fee,f_ex2_name,f_ex2_ask,FROM_UNIXTIME(f_ex2_ts/1000, '%T') as f_ex2_ts,f_ex2_fee,FROM_UNIXTIME(f_ts/1000, '%T') as f_ts,convert(f_spread, decimal(16, 8)) as f_spread,convert(f_fee, decimal(16, 8)) as f_fee,convert(f_profit, decimal(16, 8)) as f_profit,convert(f_profit_p, decimal(5, 3)) as f_profit_p
FROM db_bewin.t_spread_current 
WHERE f_profit_p > 0 and f_ex1_bid > 0 and f_ex2_ask > 0
ORDER BY f_ts desc, f_profit_p DESC 
LIMIT 100
;




