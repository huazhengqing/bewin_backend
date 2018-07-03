SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS  `t_markets`;
CREATE TABLE `t_markets` (
  `f_ex_id` varchar(64) NOT NULL COMMENT '交易所',
  `f_symbol` varchar(32) NOT NULL COMMENT '交易对',
  `f_base` varchar(32) NOT NULL,
  `f_quote` varchar(32) NOT NULL,
  `f_fee_maker` double NOT NULL,
  `f_fee_taker` double NOT NULL COMMENT '手续费',
  `f_precision_amount` bigint(20) NOT NULL,
  `f_precision_price` bigint(20) NOT NULL,
  `f_limits_amount_min` double NOT NULL,
  `f_limits_price_min` double NOT NULL,
  `f_ts` bigint(20) NOT NULL,
  PRIMARY KEY (`f_ex_id`,`f_symbol`),
  KEY `f_ts` (`f_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS  `t_spread_current`;
CREATE TABLE `t_spread_current` (
  `f_symbol` varchar(32) NOT NULL,
  `f_ex1` varchar(64) NOT NULL,
  `f_ex2` varchar(64) NOT NULL,
  `f_spread` double NOT NULL,
  `f_ts` bigint(20) NOT NULL,
  PRIMARY KEY (`f_symbol`,`f_ex1`,`f_ex2`),
  KEY `f_ts` (`f_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS  `t_ticker_crrent`;
CREATE TABLE `t_ticker_crrent` (
  `f_ex_id` varchar(64) NOT NULL,
  `f_symbol` varchar(32) NOT NULL,
  `f_ts` bigint(20) NOT NULL,
  `f_bid` double NOT NULL,
  `f_bid_volume` double NOT NULL,
  `f_ask` double NOT NULL,
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

SET FOREIGN_KEY_CHECKS = 1;

