import os
import sys
import copy
import logging
import asyncio
import time
from collections import defaultdict
import traceback
from datetime import datetime
from sqlalchemy import desc
from typing import Any, Callable, Dict, List, Optional
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
import arrow
import requests
import ccxt.async_support as ccxt
#from cachetools import TTLCache, cached
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf
import util
import strategy
from strategy.analyze import analyze
from strategy.strategy_breakout import strategy_breakout
import db
from exchange.exchange_trade import exchange_trade
from db.datahub import datahub
logger = util.get_log(__name__)


def nesteddict(): 
  return defaultdict(nesteddict)


class strategy_bot(object):
    def __init__(self)-> None:

        self.user_config = nesteddict()
        '''
        self.user_config["userid"]["ex_id"]["f_symbols_whitelist"] = []
        self.user_config["userid"]["ex_id"]["f_symbols_blacklist"] = []
        self.user_config["userid"]["ex_id"]["f_symbols_auto"] = 0
        self.user_config["userid"]["ex_id"]["f_long_hold"] = ["ETH", "BTC", "USDT", "USD", "EOS"]
        self.user_config["userid"]["ex_id"]["f_quote"] = "ETH"
        self.user_config["userid"]["ex_id"]["f_quote_amount"] = 0
        self.user_config["userid"]["ex_id"]["f_fiat_display_currency"] = "USDT"
        self.user_config["userid"]["ex_id"]["f_max_open_trades"] = 0
        self.user_config["userid"]["ex_id"]["f_stoploss_rate"] = 0
        self.user_config["userid"]["ex_id"]["f_trailing_stop_rate"] = 0
        self.user_config["userid"]["ex_id"]["f_trailing_stop_rate_positive"] = 0
        self.user_config["userid"]["ex_id"]["f_trailing_stop_channel"] = 0
        self.user_config["userid"]["ex_id"]["f_strategy"] = ""
        self.user_config["userid"]["ex_id"]["symbols"]["symbol"]["f_max_open_trades"] = 0
        self.user_config["userid"]["ex_id"]["symbols"]["symbol"]["f_stoploss_rate"] = 0
        self.user_config["userid"]["ex_id"]["symbols"]["symbol"]["f_trailing_stop_rate"] = 0
        self.user_config["userid"]["ex_id"]["symbols"]["symbol"]["f_trailing_stop_rate_positive"] = 0
        self.user_config["userid"]["ex_id"]["symbols"]["symbol"]["f_trailing_stop_channel"] = 0
        self.user_config["userid"]["ex_id"]["symbols"]["symbol"]["f_strategy"] = ""
        '''

        self.user_exchange = nesteddict()
        #self.user_exchange["userid"]["ex_id"] = None

        self.user_strategy = nesteddict()
        #self.user_strategy["userid"]["ex_id"]["symbol"]['timeframe'] = None

        self.exchange_whitelist_auto = nesteddict()
        #self.exchange_whitelist_auto["ex_id"] = []

        self.user_balances = nesteddict()
        #self.user_balances["userid"]["ex_id"]["symbol"] = None

        self.userid_system = 0

        self.datahub = datahub()
        db.init()
        self.init_system_user_conifg()
        self.init_user_conifg()
        self.refresh_whitelist_all()
        self.init_system_strategy()

        self.load_user_strategy()
        self.load_user_balances()


    async def init_all_data(self):
        self.init_user_conifg()
        self.refresh_whitelist_all()
        self.init_system_strategy()
        self.load_user_strategy()
        await self.fetch_balances()
        self.load_user_balances()


    def to_string(self):
        return "strategy_bot[] "

    def init_system_user_conifg(self):
        #logger.debug(self.to_string() + "init_system_user_conifg()  start")
        t_user_exchange_list = db.t_user_exchange.query.filter(
            db.t_user_exchange.f_userid == 0, 
        ).all()
        logger.debug(self.to_string() + "init_system_user_conifg() len={0}".format(len(t_user_exchange_list)))
        for t_user_exchange in t_user_exchange_list:
            self.user_exchange[t_user_exchange.f_userid][t_user_exchange.f_ex_id] = exchange_trade(t_user_exchange)
            self.user_config[t_user_exchange.f_userid][t_user_exchange.f_ex_id] = {
                "f_symbols_whitelist" : t_user_exchange.f_symbols_whitelist,
                "f_symbols_blacklist" : t_user_exchange.f_symbols_blacklist,
                "f_symbols_auto" : t_user_exchange.f_symbols_auto,
                "f_long_hold" : t_user_exchange.f_long_hold,
                "f_quote" : t_user_exchange.f_quote,
                "f_quote_amount" : t_user_exchange.f_quote_amount,
                "f_fiat_display_currency" : t_user_exchange.f_fiat_display_currency,
                "f_max_open_trades" : t_user_exchange.f_max_open_trades,
                "f_stoploss_rate" : t_user_exchange.f_stoploss_rate,
                "f_trailing_stop_rate" : t_user_exchange.f_trailing_stop_rate,
                "f_trailing_stop_rate_positive" : t_user_exchange.f_trailing_stop_rate_positive,
                "f_trailing_stop_channel" : t_user_exchange.f_trailing_stop_channel,
                "f_strategy" : t_user_exchange.f_strategy,
            }
        #logger.debug(self.to_string() + "init_system_user_conifg() user_config={0}".format(self.user_config))


    def init_user_conifg(self):
        logger.debug(self.to_string() + "init_user_conifg()  start")
        t_user_exchange_list = db.t_user_exchange.query.filter(
            db.t_user_exchange.f_userid > 1, 
            db.t_user_exchange.f_apikey != "",
        ).all()
        logger.debug(self.to_string() + "init_user_conifg() len={0}".format(len(t_user_exchange_list)))
        for t_user_exchange in t_user_exchange_list:
            self.user_exchange[t_user_exchange.f_userid][t_user_exchange.f_ex_id] = exchange_trade(t_user_exchange)
            self.user_config[t_user_exchange.f_userid][t_user_exchange.f_ex_id] = {
                "f_symbols_whitelist" : t_user_exchange.f_symbols_whitelist,
                "f_symbols_blacklist" : t_user_exchange.f_symbols_blacklist,
                "f_symbols_auto" : t_user_exchange.f_symbols_auto,
                "f_long_hold" : t_user_exchange.f_long_hold,
                "f_quote" : t_user_exchange.f_quote,
                "f_quote_amount" : t_user_exchange.f_quote_amount,
                "f_fiat_display_currency" : t_user_exchange.f_fiat_display_currency,
                "f_max_open_trades" : t_user_exchange.f_max_open_trades,
                "f_stoploss_rate" : t_user_exchange.f_stoploss_rate,
                "f_trailing_stop_rate" : t_user_exchange.f_trailing_stop_rate,
                "f_trailing_stop_rate_positive" : t_user_exchange.f_trailing_stop_rate_positive,
                "f_trailing_stop_channel" : t_user_exchange.f_trailing_stop_channel,
                "f_strategy" : t_user_exchange.f_strategy,
            }
        '''
        t_list = db.t_user_exchange_symbol.query.all()
        for t_user_exchange_symbol in t_list:
            self.user_config[t_user_exchange_symbol.f_userid][t_user_exchange_symbol.f_ex_id]["symbols"][t_user_exchange_symbol.f_symbol] = {
                "f_quote" : t_user_exchange_symbol.f_quote,
                "f_quote_amount" : t_user_exchange_symbol.f_quote_amount,
                "f_fiat_display_currency" : t_user_exchange_symbol.f_fiat_display_currency,
                "f_max_open_trades" : t_user_exchange_symbol.f_max_open_trades,
                "f_stoploss_rate" : t_user_exchange_symbol.f_stoploss_rate,
                "f_trailing_stop_rate" : t_user_exchange_symbol.f_trailing_stop_rate,
                "f_trailing_stop_rate_positive" : t_user_exchange_symbol.f_trailing_stop_rate_positive,
                "f_trailing_stop_channel" : t_user_exchange_symbol.f_trailing_stop_channel,
                "f_strategy" : t_user_exchange_symbol.f_strategy,
            }
        '''
        logger.debug(self.to_string() + "init_user_conifg()  end")

    def refresh_whitelist(self, ex_id):
        #logger.debug(self.to_string() + "refresh_whitelist({0})  start".format(ex_id))
        whitelist = []
        for t_ticker_crrent in db.t_ticker_crrent.query.filter(
            db.t_ticker_crrent.f_ex_id == ex_id
            ).order_by(desc(db.t_ticker_crrent.f_quote_volume)).limit(50):
            whitelist.append(t_ticker_crrent.f_symbol)
        self.exchange_whitelist_auto[ex_id] = whitelist
        if len(whitelist) > 0:
            logger.debug(self.to_string() + "refresh_whitelist({0}) end len={1}".format(ex_id, len(whitelist)))

    def refresh_whitelist_all(self):
        logger.debug(self.to_string() + "refresh_whitelist_all()  start")
        for id in ccxt.exchanges:
            self.refresh_whitelist(id)
        logger.debug(self.to_string() + "refresh_whitelist_all()  end")

    def init_system_strategy(self):
        logger.debug(self.to_string() + "init_system_strategy()  start")
        t_markets_list = db.t_markets.query.all()
        for t_markets in t_markets_list:
            if t_markets.f_ex_id in util.System_Strategy_ex:
                if t_markets.f_quote in util.System_Strategy_quote:
                    for tf in util.System_Strategy_Minutes_TimeFrame.keys():
                        #if t_markets.f_base != "XLM":   # only for test
                        #    continue
                        logger.debug(self.to_string() + "init_system_strategy() self.user_strategy[{0}][{1}][{2}][{3}] ".format(self.userid_system, t_markets.f_ex_id, t_markets.f_symbol, tf))
                        a = analyze(self.userid_system, t_markets.f_ex_id, t_markets.f_symbol, tf, strategy_breakout())
                        a.datahub = self.datahub
                        self.user_strategy[self.userid_system][t_markets.f_ex_id][t_markets.f_symbol][tf] = a
        logger.debug(self.to_string() + "init_system_strategy()  end")






    def load_user_strategy(self):
        logger.debug(self.to_string() + "load_user_strategy()  start")
        for userid, v1 in self.user_config.items():
            for ex_id, v2 in v1.items():
                whitelist = []
                whitelist = self.user_config[userid][ex_id]["f_symbols_whitelist"]
                if len(whitelist) <= 0 and self.user_config[userid][ex_id]["f_symbols_auto"] >= 1:
                    whitelist = self.exchange_whitelist_auto[ex_id]
                for symbol in whitelist:
                    if symbol in self.user_config[userid][ex_id]["f_symbols_blacklist"]:
                        continue
                    user_strategy = strategy.load_strategy(v2["f_strategy"])
                    if user_strategy is None:
                        continue
                    a = analyze(userid, ex_id, symbol, user_strategy._timeframe, user_strategy)
                    self.user_strategy[userid][ex_id][symbol][user_strategy._timeframe] = a

        t_list = db.t_user_exchange_symbol.query.all()
        for t_user_exchange_symbol in t_list:
            if t_user_exchange_symbol.f_symbol in self.user_config[userid][ex_id]["f_symbols_blacklist"]:
                continue
            user_strategy = strategy.load_strategy(t_user_exchange_symbol.f_strategy)
            if user_strategy is None:
                continue
            a = analyze(self.userid_system, t_user_exchange_symbol.f_ex_id, t_user_exchange_symbol.f_symbol, user_strategy._timeframe, user_strategy)
            self.user_strategy[t_user_exchange_symbol.f_userid][t_user_exchange_symbol.f_ex_id][t_user_exchange_symbol.f_symbol][user_strategy._timeframe] = a
        logger.debug(self.to_string() + "load_user_strategy()  end")



    def filter_user_symbol(self, userid, ex_id, symbols):
        #logger.debug(self.to_string() + "filter_user_symbol({0},{1},{2})  start".format(userid, ex_id, symbols))
        ret = []
        for symbol in symbols:
            if symbol in self.user_config[userid][ex_id]["f_symbols_blacklist"]:
                continue
            if symbol in self.user_config[userid][ex_id]["f_symbols_whitelist"]:
                ret.append(symbol)
                continue
            #logger.debug(self.to_string() + "filter_user_symbol()  end self.user_config[userid][ex_id]={0}".format(self.user_config[userid][ex_id]))
            if len(self.user_config[userid][ex_id]["f_symbols_whitelist"]) <= 0 and self.user_config[userid][ex_id]["f_symbols_auto"] >= 1:
                if symbol in self.exchange_whitelist_auto[ex_id]:
                    ret.append(symbol)
                    continue
        logger.debug(self.to_string() + "filter_user_symbol()  end ret={0}".format(ret))
        return ret



    # fetch_balances
    async def fetch_balances_by_userid(self, userid):
        if self.user_exchange.get(userid) is None:
            return
        if self.user_config.get(userid) is None:
            return
        for ex_id in self.user_config[userid].keys():
            if self.user_exchange[userid].get(ex_id) is None:
                continue
            ex = self.user_exchange[userid][ex_id]
            if ex is None:
                continue
            b = await ex.fetch_balances()
            for base, amount in b["free"].items():
                quote = self.user_config[userid][ex_id]["f_quote"]
                db.t_user_balances().update(userid, ex_id, base, amount, quote)
            
    async def fetch_balances(self):
        for userid in self.user_config.keys():
            await self.fetch_balances_by_userid(userid)

    def load_user_balances(self):
        t_user_balances = db.t_user_balances.query.all()
        for t_user_balance in t_user_balances:
            if t_user_balance.f_symbol is not None and t_user_balance.f_symbol != "":
                self.user_balances[t_user_balance.f_userid][t_user_balance.f_ex_id][t_user_balance.f_symbol] = t_user_balance



    '''
    ['f_ex_id', 'f_symbol', 'f_timeframe', 'f_ts', 'f_o', 'f_h', 'f_l', 'f_c', 'f_v', 'f_ts_update']
    record.values=('okex', 'NGC/BTC', 5, 1532270100000, 6.444e-05, 6.444e-05, 6.444e-05, 6.444e-05, 0.0, 1532270536)
    '''
    def process_topic_records(self, records: TupleRecord):
        for userid, user_config in self.user_config.items():
            for record in records:
                ex_id = record.values[0]
                symbol = record.values[1]
                tf = record.values[2]
                ohlcv = [record.values[3], record.values[4], record.values[5], record.values[6], record.values[7], record.values[8]]
                #logger.debug(self.to_string() + "process_topic_records(){0},{1}".format(userid, record.values))
                try:
                    self.process_position(userid, ex_id, symbol, tf, [ohlcv])
                except:
                    logger.error(traceback.format_exc())
                try:
                    if userid == 0:
                        self.process_strategy_system(userid, ex_id, symbol, tf, [ohlcv])
                    else:
                        self.process_strategy_user(userid, ex_id, symbol, tf, [ohlcv])
                except:
                    logger.error(traceback.format_exc())

    # ['f_ts', 'f_o', 'f_h', 'f_l', 'f_c', 'f_v']
    def process_position(self, userid, ex_id, symbol, tf, ohlcv_list):
        #logger.debug(self.to_string() + "process_position({0},{1},{2},{3},{4}) start".format(userid, ex_id, symbol, tf, ohlcv_list))
        if self.user_balances.get(userid) is None:
            return
        if self.user_balances[userid].get(ex_id) is None:
            return
        if self.user_balances[userid][ex_id].get(symbol) is None:
            return
        if self.user_balances[userid][ex_id][symbol] is None:
            return
        if self.user_exchange.get(userid) is None:
            return
        if self.user_exchange[userid].get(ex_id) is None:
            return
        if self.user_exchange[userid][ex_id] is None:
            return
        if self.user_config.get(userid) is None:
            return
        if self.user_config[userid].get(ex_id) is None:
            return
        if self.user_config[userid][ex_id] is None:
            return
        logger.debug(self.to_string() + "process_position({0},{1},{2},{3}) start".format(userid, ex_id, symbol, ohlcv_list))
        lowest = 0.0
        ts = 0
        for ohlcv in ohlcv_list:
            if lowest <= 0.0:
                lowest = ohlcv.f_l
            lowest = min(lowest, ohlcv.f_l)
            ts = max(ts, ohlcv.f_ts)
        t_user_balances = self.user_balances[userid][ex_id][symbol]
        if self.is_long_stoploss(t_user_balances, lowest, ts):
            self.close_position(t_user_balances)
            return
        if self.is_long_take_profit(t_user_balances, lowest, ts):
            self.close_position(t_user_balances)
            return


    def is_long_stoploss(self, t_user_balances: db.t_user_balances, bid: float, current_time: datetime) -> bool:
        if self.user_config.get(t_user_balances.f_userid) is None:
            return False
        if self.user_config[t_user_balances.f_userid].get(t_user_balances.f_ex_id) is None:
            return False
        user_config_ex = self.user_config[t_user_balances.f_userid][t_user_balances.f_ex_id]
        if user_config_ex["f_stoploss_rate"] < 0:
            t_user_balances.update_long_stoploss_by_rate(t_user_balances.f_open_rate, user_config_ex["f_stoploss_rate"])
        if user_config_ex["f_trailing_stop_rate"] < 0:
            t_user_balances.update_long_stoploss_by_rate(bid, user_config_ex["f_stoploss_rate"])
        if user_config_ex["f_trailing_stop_rate_positive"] < 0:
            if bid > t_user_balances.f_open_rate:
                t_user_balances.update_long_stoploss_by_rate(bid, user_config_ex["f_stoploss_rate"])

        channel_timeframe = user_config_ex["f_trailing_stop_channel"]
        if channel_timeframe > 0:
            t_symbols_analyze = db.t_symbols_analyze.query.filter(
                db.t_symbols_analyze.f_ex_id == t_user_balances.f_ex_id, 
                db.t_symbols_analyze.f_symbol == t_user_balances.f_symbol, 
                db.t_symbols_analyze.f_timeframe == channel_timeframe, 
            ).first()
            if t_symbols_analyze is not None:
                stoploss_absolute = t_symbols_analyze.f_channel_low
                t_user_balances.update_long_stoploss(bid, stoploss_absolute)

        if t_user_balances.f_stop_loss is not None and t_user_balances.f_stop_loss > 0:
            if bid <= t_user_balances.f_stop_loss:
                return True

        return False

    def is_long_take_profit(self, t_user_balances: db.t_user_balances, bid: float, current_time: datetime) -> bool:
        if self.user_config.get(t_user_balances.f_userid) is None:
            return False
        if self.user_config[t_user_balances.f_userid].get(t_user_balances.f_ex_id) is None:
            return False
        user_config_ex = self.user_config[t_user_balances.f_userid][t_user_balances.f_ex_id]
        time_diff = (current_time.timestamp() - t_user_balances.f_open_date.timestamp()) / 60
        '''
        for duration, threshold in self.strategy.minimal_roi.items():
            if time_diff <= duration:
                return False
            if current_profit > threshold:
                return True
        '''
        return False


    def close_position(self, t_user_balances: db.t_user_balances):
        if self.user_exchange.get(t_user_balances.f_userid) is None:
            return
        if self.user_exchange[t_user_balances.f_userid].get(t_user_balances.f_ex_id) is None:
            return
        if self.user_exchange[t_user_balances.f_userid][t_user_balances.f_ex_id] is None:
            return
        ex = self.user_exchange[t_user_balances.f_userid][t_user_balances.f_ex_id]
        ex.sell_all(str(t_user_balances.f_symbol), t_user_balances.f_amount)
        t_user_balances.session.flush()






    # ['f_ex_id', 'f_symbol', 'f_timeframe', 'f_ts', 'f_o', 'f_h', 'f_l', 'f_c', 'f_v', 'f_ts_update']
    def process_strategy_system(self, userid, ex_id, symbol, tf, ohlcv_list):
        #logger.debug(self.to_string() + "process_strategy_system({0},{1},{2},{3},{4}) start".format(userid, ex_id, symbol, tf, ohlcv_list))
        if userid != 0:
            return
        if self.user_strategy.get(userid) is None:
            return
        if self.user_strategy[userid].get(ex_id) is None:
            return
        if self.user_strategy[userid][ex_id].get(symbol) is None:
            return
        if self.user_strategy[userid][ex_id][symbol].get(tf) is None:
            return
        '''
        s_list = self.filter_user_symbol(userid, ex_id, [symbol])
        if len(s_list) <= 0:
            return
        '''
        a = self.user_strategy[userid][ex_id][symbol][tf]
        if a is None:
            return
        a.calc_signal(ohlcv_list)





    # ['f_ex_id', 'f_symbol', 'f_timeframe', 'f_ts', 'f_o', 'f_h', 'f_l', 'f_c', 'f_v', 'f_ts_update']
    def process_strategy_user(self, userid, ex_id, symbol, tf, ohlcv_list):
        #logger.debug(self.to_string() + "process_strategy_user({0},{1},{2},{3},{4}) start".format(userid, ex_id, symbol, tf, ohlcv_list))
        if self.user_strategy.get(userid) is None:
            return
        if self.user_strategy[userid].get(ex_id) is None:
            return
        if self.user_strategy[userid][ex_id].get(symbol) is None:
            return
        if self.user_strategy[userid][ex_id][symbol].get(tf) is None:
            return
        s_list = self.filter_user_symbol(userid, ex_id, [symbol])
        if len(s_list) <= 0:
            return
        a = self.user_strategy[userid][ex_id][symbol][tf]
        if a is None:
            return
        (buy, sell) = a.calc_signal(ohlcv_list)
        '''
        if buy and not sell:
            self.process_strategy_long(userid, record.f_ex_id, record.f_symbol, a)
            self.user_balances[userid][record.f_ex_id][record.f_symbol] = db.t_user_balances.query.filter(
                db.t_user_balances.f_userid == userid, 
                db.t_user_balances.f_ex_id == record.f_ex_id, 
                db.t_user_balances.f_symbol == record.f_symbol, 
                db.t_user_balances.f_is_open.is_(True),
                ).all()
            db.t_user_balances.session.flush()
        if sell and not buy:
            self.process_strategy_short(userid, record.f_ex_id, record.f_symbol, a)
            self.user_balances[userid][record.f_ex_id][record.f_symbol] = db.t_user_balances.query.filter(
                db.t_user_balances.f_userid == userid, 
                db.t_user_balances.f_ex_id == record.f_ex_id, 
                db.t_user_balances.f_symbol == record.f_symbol, 
                db.t_user_balances.f_is_open.is_(True),
                ).all()
            db.t_user_balances.session.flush()
        '''














