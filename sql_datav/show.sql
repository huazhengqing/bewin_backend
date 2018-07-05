





SELECT f_symbol,f_ex1,f_ex2,FROM_UNIXTIME(f_ts/1000) as f_ts,f_spread 
FROM t_ticker
where f_spread > 0
order by f_spread desc,f_ts desc
;

