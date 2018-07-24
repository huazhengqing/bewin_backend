#!/usr/bin/python
import os
import sys
import time
import arrow
import logging
import traceback
import configparser
from concurrent.futures import ThreadPoolExecutor
#from tornado.concurrent import run_on_executor
from datetime import datetime
from decimal import Decimal, getcontext
from typing import Any, Dict, Optional
from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, String, TIMESTAMP, create_engine, inspect, desc)
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.pool import StaticPool
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
from conf.conf_aliyun import conf_aliyun_mysql
import conf
import db
import util
logger = util.get_log(__name__)


_DECL_BASE: Any = declarative_base()
Session = None


def init() -> None:
    host = conf_aliyun_mysql['dev_db_host']
    port = conf_aliyun_mysql['dev_db_port']
    user = conf_aliyun_mysql['dev_user']
    password = conf_aliyun_mysql['dev_password']
    if conf.dev_or_product == 2:
        host = conf_aliyun_mysql['product_db_host']
        port = conf_aliyun_mysql['product_db_port']
        user = conf_aliyun_mysql['product_user']
        password = conf_aliyun_mysql['product_password']
    db_name = conf_aliyun_mysql['db']
    charset = conf_aliyun_mysql['charset']
    db_url = "mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset={5}".format(user, password, host, port, db_name, charset)
    logger.info(db_url)
    engine = create_engine(
        db_url, 
        #max_overflow=0,  # 超过连接池大小外最多创建的连接
        #pool_size=5,  # 连接池大小
        #pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
        #pool_recycle=-1, # 多久之后对线程池中的线程进行一次连接的回收（重置）
        echo = False
    )
    session_factory = sessionmaker(bind=engine, autoflush=True, autocommit=True)
    db.Session = scoped_session(session_factory)

'''
    t_exchanges.session = session()
    t_exchanges.query = session.query_property()

    t_markets.session = session()
    t_markets.query = session.query_property()

    t_ohlcv.session = session()
    t_ohlcv.query = session.query_property()

    t_spread_current.session = session()
    t_spread_current.query = session.query_property()

    t_symbols_analyze.session = session()
    t_symbols_analyze.query = session.query_property()

    t_ticker_crrent.session = session()
    t_ticker_crrent.query = session.query_property()

    t_user_balances.session = session()
    t_user_balances.query = session.query_property()



    t_user_config.session = session()
    t_user_config.query = session.query_property()

    t_user_exchange.session = session()
    t_user_exchange.query = session.query_property()

    t_user_exchange_symbol.session = session()
    t_user_exchange_symbol.query = session.query_property()
'''

init()

def has_column(columns, searchname: str) -> bool:
    return len(list(filter(lambda x: x["name"] == searchname, columns))) == 1


def get_column_def(columns, column: str, default: str) -> str:
    return default if not has_column(columns, column) else column




'''
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
'''
class t_exchanges(_DECL_BASE):
    __tablename__ = 't_exchanges'
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_ex_name = Column(String, nullable=False)
    f_countries = Column(String, nullable=False, default='')
    f_url_www = Column(String, nullable=False, default='')
    f_url_logo = Column(String, nullable=False, default='')
    f_url_referral = Column(String, nullable=False, default='')
    f_url_api = Column(String, nullable=False, default='')
    f_url_doc = Column(String, nullable=False, default='')
    f_url_fees = Column(String, nullable=False, default='')
    f_timeframes = Column(String, nullable=False, default='')
    #f_ts = Column(Integer, nullable=False)
    #f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
  `f_ts_create` bigint(20) unsigned NOT NULL COMMENT '上币时间',
  `f_ts` bigint(20) unsigned NOT NULL,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '表更新时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`),
  KEY `f_ts` (`f_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
'''
class t_markets(_DECL_BASE):
    __tablename__ = 't_markets'
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_symbol = Column(String, nullable=False, primary_key=True)
    f_base = Column(String, nullable=False, default='')
    f_quote = Column(String, nullable=False, default='')
    f_active = Column(Boolean, nullable=False)
    f_url = Column(String, nullable=False, default='')
    f_fee_maker = Column(Float, nullable=False, default=0)
    f_fee_taker = Column(Float, nullable=False, default=0)
    f_precision_amount = Column(Integer, nullable=False, default=0)
    f_precision_price = Column(Integer, nullable=False, default=0)
    f_limits_amount_min = Column(Float, nullable=False, default=0)
    f_limits_price_min = Column(Float, nullable=False, default=0)
    f_ts_create = Column(Integer, nullable=False, default=0)
    #f_ts = Column(Integer, nullable=False, default=datetime.utcnow)
    #f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
'''
class t_ohlcv(_DECL_BASE):
    __tablename__ = 't_ohlcv'
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_symbol = Column(String, nullable=False, primary_key=True)
    f_timeframe = Column(Integer, nullable=False, primary_key=True)
    f_ts = Column(Integer, nullable=False, primary_key=True)
    f_o = Column(Float, nullable=False)
    f_h = Column(Float, nullable=False)
    f_l = Column(Float, nullable=False)
    f_c = Column(Float, nullable=False)
    f_v = Column(Float, nullable=False)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
'''
class t_spread_current(_DECL_BASE):
    __tablename__ = 't_spread_current'
    f_symbol = Column(String, nullable=False, primary_key=True)
    f_ex1 = Column(String, nullable=False, primary_key=True)
    f_ex1_name = Column(String, nullable=False)
    f_ex1_bid = Column(Float, nullable=False)
    f_ex1_ts = Column(Integer, nullable=False)
    f_ex1_fee = Column(Float, nullable=False)
    f_ex2 = Column(String, nullable=False, primary_key=True)
    f_ex2_name = Column(String, nullable=False)
    f_ex2_ask = Column(Float, nullable=False)
    f_ex2_ts = Column(Integer, nullable=False)
    f_ex2_fee = Column(Float, nullable=False)
    f_ts = Column(Integer, nullable=False)
    f_spread = Column(Float, nullable=False)
    f_fee = Column(Float, nullable=False)
    f_profit = Column(Float, nullable=False)
    f_profit_p = Column(Float, nullable=False)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
  `f_recommend` double unsigned NOT NULL DEFAULT '0' COMMENT '推荐分数',
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`f_ex_id`,`f_symbol`,`f_timeframe`),
  KEY `f_breakout_ts` (`f_breakout_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
'''
class t_symbols_analyze(_DECL_BASE):
    __tablename__ = 't_symbols_analyze'
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_symbol = Column(String, nullable=False, primary_key=True)
    f_timeframe = Column(Integer, nullable=False, primary_key=True)
    f_bid = Column(Float, nullable=False, default=0.0)
    f_ask = Column(Float, nullable=False, default=0.0)
    f_spread = Column(Float, nullable=False, default=0.0)
    f_bar_trend = Column(Integer, nullable=False, default=0)
    f_volume_mean = Column(Float, nullable=False, default=0.0)
    f_volume = Column(Float, nullable=False, default=0.0)
    f_ma_period = Column(Integer, nullable=False, default=0)
    f_ma_up = Column(Float, nullable=False, default=0.0)
    f_ma_low = Column(Float, nullable=False, default=0.0)
    f_ma_trend = Column(Integer, nullable=False, default=0)
    f_channel_period = Column(Integer, nullable=False, default=0)
    f_channel_up = Column(Float, nullable=False, default=0.0)
    f_channel_low = Column(Float, nullable=False, default=0.0)
    f_breakout_trend = Column(Integer, nullable=False, default=0)
    f_breakout_ts = Column(Integer, nullable=False, default=0)
    f_breakout_price = Column(Float, nullable=False, default=0.0)
    f_breakout_volume = Column(Float, nullable=False, default=0.0)
    f_breakout_volume_rate = Column(Float, nullable=False, default=0.0)
    f_breakout_price_highest = Column(Float, nullable=False, default=0.0)
    f_breakout_price_highest_ts = Column(Integer, nullable=False, default=0)
    f_breakout_rate = Column(Float, nullable=False, default=0.0)
    f_breakout_rate_max = Column(Float, nullable=False, default=0.0)
    f_recommend = Column(Float, nullable=False, default=0.0)
    #f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
'''
class t_ticker_crrent(_DECL_BASE):
    __tablename__ = 't_ticker_crrent'
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_symbol = Column(String, nullable=False, primary_key=True)
    f_ts = Column(Integer, nullable=False)
    f_bid = Column(Float, nullable=False)
    f_bid_volume = Column(Float, nullable=False)
    f_ask = Column(Float, nullable=False)
    f_ask_volume = Column(Float, nullable=False)
    f_vwap = Column(Float, nullable=False)
    f_open = Column(Float, nullable=False)
    f_high = Column(Float, nullable=False)
    f_low = Column(Float, nullable=False)
    f_close = Column(Float, nullable=False)
    f_last = Column(Float, nullable=False)
    f_previous_close = Column(Float, nullable=False)
    f_change = Column(Float, nullable=False)
    f_percentage = Column(Float, nullable=False)
    f_average = Column(Float, nullable=False)
    f_base_volume = Column(Float, nullable=False)
    f_quote_volume = Column(Float, nullable=False)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)


'''
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
'''
class t_user_balances(_DECL_BASE):
    __tablename__ = 't_user_balances'
    f_userid = Column(Integer, nullable=False, primary_key=True)
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_base = Column(String, nullable=False, primary_key=True)
    f_base_amount = Column(Float, nullable=False, default=0.0)
    f_quote = Column(String, nullable=False)
    f_symbol = Column(String, nullable=False)
    f_cost = Column(Float, nullable=False, default=0.0)
    f_bid = Column(Float, nullable=False, default=0.0)
    f_ask = Column(Float, nullable=False, default=0.0)
    f_fee_open = Column(Float, nullable=False, default=0.0)
    f_fee_close = Column(Float, nullable=False, default=0.0)
    f_profit = Column(Float, nullable=False, default=0.0)
    f_profit_rate = Column(Float, nullable=False, default=0.0)
    f_stoploss = Column(Float, nullable=False, default=0.0)
    f_close_bid = Column(Float, nullable=False, default=0.0)
    f_close_profit = Column(Float, nullable=False, default=0.0)
    f_close_profit_rate = Column(Float, nullable=False, default=0.0)
    f_open_ts = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    f_close_ts = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)

    def cleanup(self) -> None:
        t_user_balances.session = db.Session()
        t_user_balances.query = db.Session.query_property()
        t_user_balances.session.flush()

    def update(self, userid, ex_id, base, amount, quote = "BTC") -> None:
        if amount <= 0.0:
            return
        self.f_userid = userid
        self.f_ex_id = ex_id
        self.f_base = base
        self.f_base_amount = amount
        if quote is None:
            quote = "BTC"
        self.f_quote = quote
        if self.quote in ["USDT", "USD"]:
            if self.f_base not in ["USDT", "USD"]:
                self.f_symbol = self.f_base + "/" + self.f_quote
        if self.quote in ["BTC"]:
            if self.f_base not in ["BTC", "USDT", "USD"]:
                self.f_symbol = self.f_base + "/" + self.f_quote
        if self.quote in ["ETH"]:
            if self.f_base not in ["ETH", "BTC", "USDT", "USD"]:
                self.f_symbol = self.f_base + "/" + self.f_quote
        if self.quote not in ["ETH", "BTC", "USDT", "USD"]:
            if self.f_base != self.quote:
                self.f_symbol = self.f_base + "/" + self.f_quote


    def close(self, bid: float) -> None:
        self.f_close_bid = bid
        self.f_close_profit = 0
        self.f_close_profit_rate = 0
        self.f_close_ts = datetime.utcnow()
        


    def update_long_stoploss(self, stoploss_new: float):
        if not self.f_stoploss:
            self.f_stoploss = stoploss_new
        else:
            if stoploss_new > self.f_stoploss:
                self.f_stoploss = stoploss_new
        
    def update_long_stoploss_by_rate(self, bid: float, stoploss_rate: float):
        self.f_bid = bid
        new_loss = float(self.f_bid * (1 - abs(stoploss_rate)))
        self.update_long_stoploss(new_loss)











'''
CREATE TABLE `t_user_config` (
  `f_userid` bigint(20) unsigned NOT NULL COMMENT '用户ID',
  `f_telegram_token` varchar(128) NOT NULL,
  `f_telegram_chat_id` varchar(128) NOT NULL,
  `f_ts_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`f_userid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
'''
class t_user_config(_DECL_BASE):
    __tablename__ = 't_user_config'
    f_userid = Column(Integer, nullable=False, primary_key=True)
    f_telegram_token = Column(String, nullable=False)
    f_telegram_chat_id = Column(String, nullable=False)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
'''
class t_user_exchange(_DECL_BASE):
    __tablename__ = 't_user_exchange'
    f_userid = Column(Integer, nullable=False, primary_key=True)
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_apikey = Column(String, nullable=False)
    f_secret = Column(String, nullable=False)
    f_password = Column(String, nullable=False)
    f_uid = Column(String, nullable=False)
    f_aiohttp_proxy = Column(String, nullable=False)
    f_proxy = Column(String, nullable=False)
    f_proxies = Column(String, nullable=False)
    f_symbols_whitelist = Column(String, nullable=False)
    f_symbols_blacklist = Column(String, nullable=False)
    f_symbols_auto = Column(Boolean, nullable=False)
    f_long_hold = Column(String, nullable=False)
    f_quote = Column(String, nullable=False)
    f_quote_amount = Column(Float, nullable=False)
    f_fiat_display_currency = Column(String, nullable=False)
    f_max_open_trades = Column(Integer, nullable=False)
    f_stoploss_rate = Column(Float, nullable=False)
    f_trailing_stop_rate = Column(Float, nullable=False)
    f_trailing_stop_rate_positive = Column(Float, nullable=False)
    f_trailing_stop_channel = Column(Integer, nullable=False)
    f_strategy = Column(String, nullable=False)
    f_ts = Column(Integer, nullable=False)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)



'''
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
'''
class t_user_exchange_symbol(_DECL_BASE):
    __tablename__ = 't_user_exchange_symbol'
    f_userid = Column(Integer, nullable=False, primary_key=True)
    f_ex_id = Column(String, nullable=False, primary_key=True)
    f_symbol = Column(String, nullable=False, primary_key=True)
    f_quote = Column(String, nullable=False, primary_key=True)
    f_quote_amount = Column(Float, nullable=False)
    f_fiat_display_currency = Column(String, nullable=False)
    f_max_open_trades = Column(Integer, nullable=False)
    f_stoploss_rate = Column(Float, nullable=False)
    f_trailing_stop_rate = Column(Float, nullable=False)
    f_trailing_stop_rate_positive = Column(Float, nullable=False)
    f_trailing_stop_channel = Column(Integer, nullable=False)
    f_strategy = Column(String, nullable=False)
    f_ts = Column(Integer, nullable=False)
    f_ts_update = Column(TIMESTAMP, nullable=False, default=datetime.utcnow)
    





