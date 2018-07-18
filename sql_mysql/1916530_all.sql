SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS  `t_exchanges`;
CREATE TABLE `t_exchanges` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_ex_name` varchar(64) NOT NULL COMMENT '交易所',
  `f_countries` varchar(128) NOT NULL COMMENT '国家地区',
  `f_url_www` varchar(128) NOT NULL COMMENT 'url',
  `f_ts` bigint(20) NOT NULL COMMENT '时间',
  `f_timeframes` varchar(250) NOT NULL COMMENT 'k线时间周期',
  PRIMARY KEY (`f_ex_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_markets`;
CREATE TABLE `t_markets` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_base` varchar(32) NOT NULL COMMENT '基准币',
  `f_quote` varchar(32) NOT NULL COMMENT '定价币',
  `f_fee_maker` double NOT NULL COMMENT '手续费',
  `f_fee_taker` double NOT NULL COMMENT '手续费',
  `f_precision_amount` bigint(20) NOT NULL COMMENT '精度',
  `f_precision_price` bigint(20) NOT NULL COMMENT '精度',
  `f_limits_amount_min` double NOT NULL COMMENT '最小数量',
  `f_limits_price_min` double NOT NULL COMMENT '最小价格',
  `f_ts` bigint(20) NOT NULL COMMENT '时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`),
  KEY `f_ts` (`f_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_ohlcv`;
CREATE TABLE `t_ohlcv` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_peroid` bigint(12) NOT NULL COMMENT '周期(分钟)',
  `f_ts` bigint(20) NOT NULL COMMENT '时间(ms)',
  `f_o` double NOT NULL COMMENT '开始价格',
  `f_h` double NOT NULL COMMENT '最高价格',
  `f_l` double NOT NULL COMMENT '最低价格',
  `f_c` double NOT NULL COMMENT '结束价格',
  `f_v` double NOT NULL COMMENT '成交量',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`,`f_peroid`,`f_ts`),
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
  `f_breakout_volume_rate` double NOT NULL COMMENT '突破时成交量倍数',
  `f_breakout_price_highest` double unsigned NOT NULL DEFAULT '0' COMMENT '突破后最高价格',
  `f_breakout_price_highest_ts` bigint(20) unsigned NOT NULL DEFAULT '0' COMMENT '突破后新高时间(ms)',
  `f_breakout_rate` double NOT NULL DEFAULT '0' COMMENT '突破后当前涨幅(%)(即时更新)',
  `f_breakout_rate_max` double NOT NULL DEFAULT '0' COMMENT '突破后最大涨幅(%)',
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
  PRIMARY KEY (`f_ex_id`,`f_symbol`),
  KEY `f_ts` (`f_ts`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_trades`;
CREATE TABLE `t_trades` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_is_open` tinyint(1) NOT NULL,
  `f_fee_open` double NOT NULL,
  `f_fee_close` double NOT NULL,
  `f_open_rate` double NOT NULL,
  `f_open_rate_requested` double NOT NULL,
  `f_close_rate` double NOT NULL,
  `f_close_rate_requested` double NOT NULL,
  `f_close_profit` double NOT NULL,
  `f_stake_amount` double NOT NULL,
  `f_amount` double NOT NULL,
  `f_open_date` bigint(20) unsigned NOT NULL,
  `f_close_date` bigint(20) unsigned NOT NULL,
  `f_open_order_id` varchar(32) NOT NULL,
  `f_stop_loss` double NOT NULL,
  `f_initial_stop_loss` double NOT NULL,
  `f_max_rate` double NOT NULL,
  PRIMARY KEY (`id`),
  KEY `f_userid` (`f_userid`),
  KEY `f_ex_id` (`f_ex_id`),
  KEY `f_symbol` (`f_symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_trades_dryrun`;
CREATE TABLE `t_trades_dryrun` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_is_open` tinyint(1) NOT NULL,
  `f_fee_open` double NOT NULL,
  `f_fee_close` double NOT NULL,
  `f_open_rate` double NOT NULL,
  `f_open_rate_requested` double NOT NULL,
  `f_close_rate` double NOT NULL,
  `f_close_rate_requested` double NOT NULL,
  `f_close_profit` double NOT NULL,
  `f_stake_amount` double NOT NULL,
  `f_amount` double NOT NULL,
  `f_open_date` bigint(20) unsigned NOT NULL,
  `f_close_date` bigint(20) unsigned NOT NULL,
  `f_open_order_id` varchar(32) NOT NULL,
  `f_stop_loss` double NOT NULL,
  `f_initial_stop_loss` double NOT NULL,
  `f_max_rate` double NOT NULL,
  PRIMARY KEY (`id`),
  KEY `f_userid` (`f_userid`),
  KEY `f_ex_id` (`f_ex_id`),
  KEY `f_symbol` (`f_symbol`)
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
  `f_ex_id` varchar(128) NOT NULL COMMENT '交易所',
  `f_apikey` varchar(128) NOT NULL COMMENT 'api key',
  `f_secret` varchar(128) NOT NULL COMMENT 'secret',
  `f_password` varchar(128) NOT NULL COMMENT 'GDAX',
  `f_uid` varchar(128) NOT NULL COMMENT 'QuadrigaCX',
  `f_aiohttp_proxy` varchar(128) NOT NULL COMMENT '异步代理',
  `f_proxy` varchar(128) NOT NULL COMMENT 'cors代理',
  `f_proxies` varchar(255) NOT NULL COMMENT '同步代理',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `f_ts` bigint(20) unsigned NOT NULL COMMENT '创建时间',
  PRIMARY KEY (`f_userid`,`f_ex_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS  `t_user_trades_config`;
CREATE TABLE `t_user_trades_config` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_stake_currency` varchar(32) NOT NULL DEFAULT 'BTC' COMMENT '币',
  `f_stake_amount` double NOT NULL DEFAULT '0' COMMENT '币数量',
  `f_fiat_display_currency` varchar(32) NOT NULL DEFAULT 'USD' COMMENT '显示法币',
  `f_symbols_whitelist` text NOT NULL COMMENT '白名单',
  `f_symbols_blacklist` text NOT NULL COMMENT '黑名单',
  `f_max_open_trades` bigint(20) unsigned NOT NULL DEFAULT '1' COMMENT '最大下单',
  `f_stoploss_rate` double(20,3) NOT NULL DEFAULT '-0.300' COMMENT '止损(%)',
  `f_trailing_stop_rate` double(20,3) NOT NULL DEFAULT '-0.300' COMMENT '移动止损(%)',
  `f_trailing_stop_rate_positive` double(20,3) NOT NULL DEFAULT '-0.500' COMMENT '赚钱时移动止损(%)',
  `f_trailing_stop_channel` bigint(20) unsigned NOT NULL DEFAULT '1440' COMMENT '通道止损(min周期)',
  `f_strategy` varchar(128) NOT NULL,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `f_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_userid`,`f_ex_id`,`f_stake_currency`),
  KEY `f_userid` (`f_userid`),
  KEY `f_ex_id` (`f_ex_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;

