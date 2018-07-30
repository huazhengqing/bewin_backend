SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS  `t_exchanges`;
CREATE TABLE `t_exchanges` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_ex_name` varchar(64) NOT NULL COMMENT '交易所',
  `f_countries` varchar(128) NOT NULL COMMENT '国家地区',
  `f_url_www` varchar(256) NOT NULL COMMENT '主页',
  `f_url_logo` varchar(256) NOT NULL,
  `f_url_referral` varchar(256) NOT NULL,
  `f_url_api` varchar(256) NOT NULL,
  `f_url_doc` varchar(256) NOT NULL,
  `f_url_fees` varchar(256) NOT NULL,
  `f_timeframes` varchar(256) NOT NULL COMMENT 'k线时间周期',
  `f_ts` bigint(20) NOT NULL COMMENT '时间',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_ex_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_markets`;
CREATE TABLE `t_markets` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_base` varchar(32) NOT NULL COMMENT '基准币',
  `f_quote` varchar(32) NOT NULL COMMENT '定价币',
  `f_active` tinyint(1) NOT NULL COMMENT '有效',
  `f_url` varchar(256) NOT NULL COMMENT '交易url',
  `f_fee_maker` double unsigned NOT NULL COMMENT '手续费',
  `f_fee_taker` double unsigned NOT NULL COMMENT '手续费',
  `f_precision_amount` bigint(20) unsigned NOT NULL COMMENT '精度',
  `f_precision_price` bigint(20) unsigned NOT NULL COMMENT '精度',
  `f_limits_amount_min` double unsigned NOT NULL COMMENT '最小数量',
  `f_limits_price_min` double unsigned NOT NULL COMMENT '最小价格',
  `f_recommend` double NOT NULL DEFAULT '0' COMMENT '推荐[1=成交量好;0=普通;-1=价格太低;-2=成交量太小]',
  `f_ts_create` bigint(20) unsigned NOT NULL COMMENT '上币时间',
  `f_ts` bigint(20) unsigned NOT NULL,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '表更新时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`),
  KEY `f_ts` (`f_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_ohlcv`;
CREATE TABLE `t_ohlcv` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(64) NOT NULL COMMENT '交易对',
  `f_timeframe` bigint(12) unsigned NOT NULL COMMENT '周期(分钟)',
  `f_ts` bigint(20) unsigned NOT NULL COMMENT '时间(ms)',
  `f_o` double unsigned NOT NULL COMMENT '开始价格',
  `f_h` double unsigned NOT NULL COMMENT '最高价格',
  `f_l` double unsigned NOT NULL COMMENT '最低价格',
  `f_c` double unsigned NOT NULL COMMENT '结束价格',
  `f_v` double unsigned NOT NULL COMMENT '成交量',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`,`f_timeframe`,`f_ts`),
  KEY `f_h` (`f_h`) USING BTREE,
  KEY `f_v` (`f_v`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_spread_current`;
CREATE TABLE `t_spread_current` (
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_ex1` varchar(64) NOT NULL COMMENT '交易所1',
  `f_ex1_name` varchar(64) NOT NULL COMMENT '交易所1',
  `f_ex1_bid` double NOT NULL COMMENT '交易所1买1价',
  `f_ex1_ts` bigint(20) NOT NULL COMMENT '交易所1时间',
  `f_ex1_fee` double NOT NULL COMMENT '交易所1手续费',
  `f_ex2` varchar(64) NOT NULL COMMENT '交易所2',
  `f_ex2_name` varchar(64) NOT NULL COMMENT '交易所1',
  `f_ex2_ask` double NOT NULL COMMENT '交易所2卖1价',
  `f_ex2_ts` bigint(20) NOT NULL COMMENT '交易所2时间',
  `f_ex2_fee` double NOT NULL COMMENT '交易所1手续费',
  `f_ts` bigint(20) NOT NULL COMMENT '时间',
  `f_spread` double NOT NULL COMMENT '价差(1买1-2卖1)',
  `f_fee` double NOT NULL COMMENT '手续费',
  `f_profit` double NOT NULL COMMENT '收益',
  `f_profit_p` double NOT NULL COMMENT '收益(%)',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_symbol`,`f_ex1`,`f_ex2`),
  KEY `f_ts` (`f_ts`),
  KEY `f_spread` (`f_spread`),
  KEY `f_profit_p` (`f_profit_p`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_symbols_analyze`;
CREATE TABLE `t_symbols_analyze` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_timeframe` bigint(12) unsigned NOT NULL COMMENT '周期(分钟)',
  `f_bid` double unsigned NOT NULL DEFAULT '0' COMMENT '买价(即时更新)',
  `f_ask` double unsigned NOT NULL DEFAULT '0' COMMENT '卖价(即时更新)',
  `f_spread` double unsigned NOT NULL COMMENT '点差(即时更新)',
  `f_bar_trend` bigint(20) NOT NULL DEFAULT '0' COMMENT 'k线涨跌',
  `f_volume_mean` double unsigned NOT NULL COMMENT '成交量均值',
  `f_volume` double unsigned NOT NULL COMMENT '当前成交量(即时更新)',
  `f_ma_period` bigint(20) unsigned NOT NULL DEFAULT '34' COMMENT '均线周期',
  `f_ma_up` double unsigned NOT NULL DEFAULT '0' COMMENT '均线高位',
  `f_ma_low` double unsigned NOT NULL DEFAULT '0' COMMENT '均线低位',
  `f_ma_trend` bigint(20) NOT NULL DEFAULT '0' COMMENT '均线方向',
  `f_channel_period` bigint(12) unsigned NOT NULL DEFAULT '40' COMMENT '通道周期',
  `f_channel_up` double unsigned NOT NULL DEFAULT '0' COMMENT '通道高位',
  `f_channel_low` double unsigned NOT NULL DEFAULT '0' COMMENT '通道低位',
  `f_breakout_trend` bigint(20) NOT NULL DEFAULT '0' COMMENT '突破方向',
  `f_breakout_ts` bigint(20) unsigned NOT NULL COMMENT '突破时间(ms)',
  `f_breakout_price` double unsigned NOT NULL COMMENT '突破时价格',
  `f_breakout_volume` double unsigned NOT NULL DEFAULT '0' COMMENT '突破时成交量',
  `f_breakout_volume_rate` double unsigned NOT NULL COMMENT '突破时成交量倍数',
  `f_breakout_price_highest` double unsigned NOT NULL DEFAULT '0' COMMENT '突破后最高价格',
  `f_breakout_price_highest_ts` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '突破后新高时间(ms)',
  `f_breakout_rate` double NOT NULL DEFAULT '0' COMMENT '突破后当前涨幅(%)(即时更新)',
  `f_breakout_rate_max` double NOT NULL DEFAULT '0' COMMENT '突破后最大涨幅(%)',
  `f_recommend` double NOT NULL DEFAULT '0' COMMENT '推荐[1=成交量好;0=普通;-1=价格太低;-2=成交量太小]',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`,`f_timeframe`),
  KEY `f_breakout_ts` (`f_breakout_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_ticker_crrent`;
CREATE TABLE `t_ticker_crrent` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_ts` bigint(20) NOT NULL COMMENT '时间',
  `f_bid` double NOT NULL COMMENT '买1价',
  `f_bid_volume` double NOT NULL,
  `f_ask` double NOT NULL COMMENT '卖1价',
  `f_ask_volume` double NOT NULL,
  `f_vwap` double NOT NULL,
  `f_open` double NOT NULL,
  `f_high` double NOT NULL,
  `f_low` double NOT NULL,
  `f_close` double NOT NULL,
  `f_last` double NOT NULL,
  `f_previous_close` double NOT NULL,
  `f_change` double NOT NULL,
  `f_percentage` double NOT NULL,
  `f_average` double NOT NULL,
  `f_base_volume` double NOT NULL,
  `f_quote_volume` double NOT NULL,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_ex_id`,`f_symbol`),
  KEY `f_ts` (`f_ts`) USING BTREE,
  KEY `f_quote_volume` (`f_quote_volume`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_user_balances`;
CREATE TABLE `t_user_balances` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_base` varchar(32) NOT NULL COMMENT '币',
  `f_base_amount` double unsigned NOT NULL DEFAULT '0' COMMENT '数量',
  `f_quote` varchar(32) NOT NULL COMMENT '定价币',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_cost` double unsigned NOT NULL DEFAULT '0' COMMENT '成本',
  `f_bid` double unsigned NOT NULL DEFAULT '0' COMMENT '现买价',
  `f_ask` double unsigned NOT NULL DEFAULT '0' COMMENT '现卖价',
  `f_fee_open` double unsigned NOT NULL DEFAULT '0' COMMENT '手续费',
  `f_fee_close` double unsigned NOT NULL DEFAULT '0' COMMENT '手续费',
  `f_profit` double NOT NULL COMMENT '收益',
  `f_profit_rate` double NOT NULL COMMENT '收益率',
  `f_stoploss` double unsigned NOT NULL DEFAULT '0' COMMENT '止损',
  `f_close_bid` double unsigned NOT NULL DEFAULT '0' COMMENT '平仓价',
  `f_close_profit` double NOT NULL COMMENT '平仓收益',
  `f_close_profit_rate` double NOT NULL COMMENT '平仓收益率',
  `f_open_ts` bigint(20) unsigned NOT NULL COMMENT '建仓时间',
  `f_close_ts` bigint(20) unsigned NOT NULL COMMENT '平仓时间',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_userid`,`f_ex_id`,`f_base`),
  KEY `f_userid` (`f_userid`),
  KEY `f_ex_id` (`f_ex_id`),
  KEY `f_symbol` (`f_base`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_user_balances_dryrun`;
CREATE TABLE `t_user_balances_dryrun` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_base` varchar(32) NOT NULL COMMENT '币',
  `f_base_amount` double unsigned NOT NULL DEFAULT '0' COMMENT '数量',
  `f_quote` varchar(32) NOT NULL COMMENT '定价币',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_cost` double unsigned NOT NULL DEFAULT '0' COMMENT '成本',
  `f_bid` double unsigned NOT NULL DEFAULT '0' COMMENT '现买价',
  `f_ask` double unsigned NOT NULL DEFAULT '0' COMMENT '现卖价',
  `f_fee_open` double unsigned NOT NULL DEFAULT '0' COMMENT '手续费',
  `f_fee_close` double unsigned NOT NULL DEFAULT '0' COMMENT '手续费',
  `f_profit` double NOT NULL COMMENT '收益',
  `f_profit_rate` double NOT NULL COMMENT '收益率',
  `f_stoploss` double unsigned NOT NULL DEFAULT '0' COMMENT '止损',
  `f_close_bid` double unsigned NOT NULL DEFAULT '0' COMMENT '平仓价',
  `f_close_profit` double NOT NULL COMMENT '平仓收益',
  `f_close_profit_rate` double NOT NULL COMMENT '平仓收益率',
  `f_open_ts` bigint(20) unsigned NOT NULL COMMENT '建仓时间',
  `f_close_ts` bigint(20) unsigned NOT NULL COMMENT '平仓时间',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_userid`,`f_ex_id`,`f_base`),
  KEY `f_userid` (`f_userid`),
  KEY `f_ex_id` (`f_ex_id`),
  KEY `f_symbol` (`f_base`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_user_config`;
CREATE TABLE `t_user_config` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_telegram_token` varchar(128) NOT NULL,
  `f_telegram_chat_id` varchar(128) NOT NULL,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_userid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_user_exchange`;
CREATE TABLE `t_user_exchange` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(32) NOT NULL COMMENT '交易所',
  `f_apikey` varchar(128) NOT NULL COMMENT 'api key',
  `f_secret` varchar(128) NOT NULL COMMENT 'secret',
  `f_password` varchar(128) NOT NULL COMMENT 'GDAX',
  `f_uid` varchar(128) NOT NULL COMMENT 'QuadrigaCX',
  `f_aiohttp_proxy` varchar(128) NOT NULL COMMENT '异步代理',
  `f_proxy` varchar(128) NOT NULL COMMENT 'cors代理',
  `f_proxies` varchar(128) NOT NULL COMMENT '同步代理',
  `f_symbols_whitelist` text NOT NULL COMMENT '白名单',
  `f_symbols_blacklist` text NOT NULL COMMENT '黑名单',
  `f_symbols_auto` tinyint(1) NOT NULL DEFAULT '0' COMMENT '自动选择交易对',
  `f_long_hold` text NOT NULL COMMENT '长期持有币',
  `f_quote` varchar(32) NOT NULL COMMENT '定价币',
  `f_quote_amount` double unsigned NOT NULL COMMENT '定价币数量',
  `f_fiat_display_currency` varchar(32) NOT NULL COMMENT '显示法币',
  `f_max_open_trades` bigint(20) unsigned NOT NULL COMMENT '最大下单',
  `f_stoploss_rate` double NOT NULL COMMENT '止损(%)',
  `f_trailing_stop_rate` double NOT NULL COMMENT '移动止损(%)',
  `f_trailing_stop_rate_positive` double NOT NULL COMMENT '赚钱时移动止损(%)',
  `f_trailing_stop_channel` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '通道止损(min周期)',
  `f_strategy` varchar(128) NOT NULL COMMENT '交易策略',
  `f_ts` bigint(20) unsigned NOT NULL COMMENT '创建时间',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`f_userid`,`f_ex_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_user_exchange_symbol`;
CREATE TABLE `t_user_exchange_symbol` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_quote` varchar(32) NOT NULL DEFAULT 'BTC' COMMENT '定价币',
  `f_quote_amount` double unsigned NOT NULL DEFAULT '0' COMMENT '定价币数量',
  `f_fiat_display_currency` varchar(32) NOT NULL DEFAULT 'USD' COMMENT '显示法币',
  `f_max_open_trades` bigint(20) unsigned NOT NULL DEFAULT '1' COMMENT '最大下单',
  `f_stoploss_rate` double(20,3) NOT NULL DEFAULT '-0.300' COMMENT '止损(%)',
  `f_trailing_stop_rate` double(20,3) NOT NULL DEFAULT '-0.300' COMMENT '移动止损(%)',
  `f_trailing_stop_rate_positive` double(20,3) NOT NULL DEFAULT '-0.500' COMMENT '赚钱时移动止损(%)',
  `f_trailing_stop_channel` bigint(20) unsigned NOT NULL DEFAULT '1440' COMMENT '通道止损(min周期)',
  `f_strategy` varchar(128) NOT NULL,
  `f_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_userid`,`f_ex_id`,`f_symbol`),
  KEY `f_userid` (`f_userid`),
  KEY `f_ex_id` (`f_ex_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;

/* EVENTS */;
DROP EVENT IF EXISTS `event_update`;
DELIMITER $$
CREATE EVENT `event_update` ON SCHEDULE EVERY 1 HOUR STARTS '2018-07-30 22:55:23' ON COMPLETION PRESERVE ENABLE DO begin
UPDATE `db_bewin`.`t_symbols_analyze` a,`db_bewin`.`t_ticker_crrent` b SET a.f_bid=b.f_bid,a.f_ask=b.f_ask,a.f_spread=(b.f_ask - b.f_bid) WHERE  a.f_ex_id = b.f_ex_id and a.f_symbol = b.f_symbol;
update t_markets a,t_ticker_crrent b set a.f_recommend=-1 where a.f_ex_id=b.f_ex_id and a.f_symbol=b.f_symbol and (b.f_bid < 0.05 and b.f_symbol REGEXP '/USDT$' or b.f_bid < 0.000005 and b.f_symbol REGEXP '/BTC$' or b.f_bid < 0.0001 and b.f_symbol REGEXP '/ETH$');
update t_markets a,t_ticker_crrent b set a.f_recommend=-2 where a.f_ex_id=b.f_ex_id and a.f_symbol=b.f_symbol and b.f_quote_volume>0 and (b.f_quote_volume < 500000 and b.f_symbol REGEXP '/USDT$' or b.f_quote_volume < 50 and b.f_symbol REGEXP '/BTC$' or b.f_quote_volume < 1000 and b.f_symbol REGEXP '/ETH$');
update t_markets a,t_ticker_crrent b set a.f_recommend=-2 where a.f_ex_id=b.f_ex_id and b.f_ex_id='okex' and a.f_symbol=b.f_symbol and b.f_quote_volume<=0 and (b.f_base_volume*b.f_bid < 500000 and b.f_symbol REGEXP '/USDT$' or b.f_base_volume*b.f_bid < 50 and b.f_symbol REGEXP '/BTC$' or b.f_base_volume*b.f_bid < 1000 and b.f_symbol REGEXP '/ETH$');
update t_markets a,t_ticker_crrent b set a.f_recommend=1 where a.f_ex_id=b.f_ex_id and a.f_symbol=b.f_symbol and b.f_quote_volume>0 and (b.f_quote_volume>1000000 and b.f_symbol REGEXP '/USDT$' or b.f_quote_volume>100 and b.f_symbol REGEXP '/BTC$' or b.f_quote_volume>2000 and b.f_symbol REGEXP '/ETH$');
update t_markets a,t_ticker_crrent b set a.f_recommend=1 where a.f_ex_id=b.f_ex_id and b.f_ex_id='okex' and a.f_symbol=b.f_symbol and b.f_quote_volume<=0 and (b.f_base_volume*b.f_bid>1000000 and b.f_symbol REGEXP '/USDT$' or b.f_base_volume*b.f_bid>100 and b.f_symbol REGEXP '/BTC$' or b.f_base_volume*b.f_bid>2000 and b.f_symbol REGEXP '/ETH$');
update t_markets a,t_symbols_analyze b set b.f_recommend=a.f_recommend where a.f_ex_id=b.f_ex_id and a.f_symbol=b.f_symbol;
update t_symbols_analyze set f_recommend=2 where f_recommend=1 and f_timeframe=240 and f_bar_trend=1 and f_ma_trend=1 and f_bid>f_ma_up;

end
$$
DELIMITER ;

