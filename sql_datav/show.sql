


SELECT f_symbol,f_ex_id,FROM_UNIXTIME(f_ts/1000) as f_ts,f_bid,f_ask 
FROM t_ticker
where f_ex_id='okex'
order by f_ts desc
;



