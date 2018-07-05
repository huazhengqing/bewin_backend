SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS  `t_exchanges`;
CREATE TABLE `t_exchanges` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_ex_name` varchar(64) NOT NULL COMMENT '交易所',
  `f_countries` varchar(128) NOT NULL COMMENT '国家地区',
  `f_url_www` varchar(128) NOT NULL COMMENT 'url',
  `f_ts` bigint(20) NOT NULL COMMENT '时间',
  PRIMARY KEY (`f_ex_id`),
  KEY `f_ts` (`f_ts`)
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

DROP TABLE IF EXISTS  `t_spread_current`;
CREATE TABLE `t_spread_current` (
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_ex1` varchar(64) NOT NULL COMMENT '交易所1',
  `f_ex1_name` varchar(64) NOT NULL COMMENT '交易所1',
  `f_ex1_bid` double NOT NULL COMMENT '交易所1买1价',
  `f_ex1_ts` bigint(20) NOT NULL COMMENT '交易所1时间',
  `f_ex1_fee` double NOT NULL COMMENT '交易所1手续费',
  `f_ex2` varchar(64) NOT NULL COMMENT '交易所2',
  `f_ex2_name` varchar(64) NOT NULL COMMENT '交易所2',
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

SET FOREIGN_KEY_CHECKS = 1;

