import os
import sys
import logging
import traceback
from datetime import datetime
from enum import Enum
from typing import Dict, List, Tuple
import arrow
from sqlalchemy import (Boolean, Column, DateTime, Float, Integer, String, TIMESTAMP, create_engine, inspect, desc)
from random import randint
from pandas import DataFrame, to_datetime
from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
from strategy.strategy_interface import IStrategy
from strategy.strategy_breakout import strategy_breakout
import conf.conf_aliyun
import conf
import util
import db
from db.datahub import datahub
from exchange.exchange import exchange
from exchange import parse_ohlcv_dataframe
logger = util.get_log(__name__)





class analyze(object):
    def __init__(self, userid, ex_id, symbol, timeframe, strategy) -> None:
        self.userid = userid
        self.ex_id = ex_id
        self.ex = exchange(ex_id)
        self.symbol = symbol
        self.timeframe = timeframe

        self.strategy = strategy
        if self.strategy is None:
            raise Exception(self.to_string() + "strategy is None")
        self.strategy._exchange = self.ex_id
        self.strategy._symbol = self.symbol
        self.strategy._timeframe = self.timeframe

        self.ohlcv_list = self.load_ohlcv_from_db()
        self.dataframe = None
        
        self.datahub = None

    def to_string(self):
        return "analyze[{0},{1},{2},{3}] ".format(self.userid, self.ex_id, self.symbol, self.timeframe)

    def load_ohlcv_from_db(self):
        #logger.debug(self.to_string() + "load_ohlcv_from_db() start")
        list_ohlcv = []
        for t_ohlcv in db.Session().query(db.t_ohlcv).filter(
            db.t_ohlcv.f_ex_id == self.ex_id,
            db.t_ohlcv.f_symbol == self.symbol,
            db.t_ohlcv.f_timeframe == self.timeframe
        ).order_by(desc(db.t_ohlcv.f_ts)).limit(300):
            list_ohlcv.append([t_ohlcv.f_ts, t_ohlcv.f_o, t_ohlcv.f_h, t_ohlcv.f_l, t_ohlcv.f_c, t_ohlcv.f_v])
        logger.debug(self.to_string() + "load_ohlcv_from_db() end  len={0} ".format(len(list_ohlcv)))
        return list_ohlcv
 


    def calc_signal(self, ohlcv : List[Dict]) -> Tuple[bool, bool]:
        self.ohlcv_list.extend(ohlcv)
        if len(self.ohlcv_list) <= 30:
            return (False, False)
        try:
            self.dataframe = parse_ohlcv_dataframe(self.ohlcv_list)
            self.dataframe = self.strategy.calc_indicators(self.dataframe)
            self.dataframe = self.strategy.buy(self.dataframe)
            self.dataframe = self.strategy.sell(self.dataframe)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.warning(self.to_string() +  'calc_signal() Exception= {0}'.format(e))
            return (False, False)
        if self.dataframe.empty:
            return (False, False)
        latest = self.dataframe.iloc[-1]
        signal_date = arrow.get(latest['date'])
        if signal_date < (arrow.utcnow().shift(minutes=-(self.timeframe * 2))):
            logger.warning(self.to_string() +  'calc_signal() ohlcv is old = {0}'.format(latest['date']))
            return (False, False)
        if (self.userid == 0):
            self.update_db()
        (buy, sell) = latest["buy"] == 1, latest["sell"] == 1
        return (buy, sell)



    def update_db(self):
        #logger.debug(self.to_string() + "update_db() start  ")
        latest = self.dataframe.iloc[-1]
        if latest['ma_high'] == 'nan' or latest['max'] == 'nan' or latest['volume_mean'] == 'nan':
            return
        s = db.Session()
        t_symbols_analyze = s.query(db.t_symbols_analyze).filter(
            db.t_symbols_analyze.f_ex_id == str(self.ex_id),
            db.t_symbols_analyze.f_symbol == str(self.symbol),
            db.t_symbols_analyze.f_timeframe == int(self.timeframe)
        ).first()
        if t_symbols_analyze is None:
            #logger.debug(self.to_string() + "update_db() t_symbols_analyze is None  ")
            t_symbols_analyze = db.t_symbols_analyze(
                f_ex_id = self.ex_id,
                f_symbol = self.symbol,
                f_timeframe = int(self.timeframe),
                f_bid = 0.0,
                f_ask = 0.0,
                f_spread = 0.0,
                f_bar_trend = 0,
                f_volume_mean = 0.0,
                f_volume = 0.0,
                f_ma_period = 0,
                f_ma_up = 0.0,
                f_ma_low = 0.0,
                f_ma_trend = 0,
                f_channel_period = 0,
                f_channel_up = 0.0,
                f_channel_low = 0.0,
                f_breakout_trend = 0,
                f_breakout_ts = 0,
                f_breakout_price = 0.0,
                f_breakout_volume = 0.0,
                f_breakout_volume_rate = 0.0,
                f_breakout_price_highest = 0.0,
                f_breakout_price_highest_ts = 0,
                f_breakout_rate = 0.0,
                f_breakout_rate_max = 0.0,
                f_recommend = 0.0
            )
        #logger.debug(self.to_string() + "update_db() t_symbols_analyze  ")
        t_symbols_analyze.f_bar_trend = latest['ha_open'] < latest['ha_close'] and 1 or -1
        t_symbols_analyze.f_volume_mean = float(latest['volume_mean'])
        t_symbols_analyze.f_volume = float(latest['volume'])
        t_symbols_analyze.f_ma_period = self.strategy._ma_period
        t_symbols_analyze.f_ma_up = float(latest['ma_high'])
        t_symbols_analyze.f_ma_low = float(latest['ma_low'])
        t_symbols_analyze.f_ma_trend = float(latest['ma_trend'])
        t_symbols_analyze.f_channel_period = self.strategy._channel_period
        t_symbols_analyze.f_channel_up = float(latest['max'])
        t_symbols_analyze.f_channel_low = float(latest['min'])


        date_ms = arrow.get(latest['date']).timestamp * 1000
        if t_symbols_analyze.f_breakout_trend == 0 or date_ms - t_symbols_analyze.f_breakout_ts > 3*60*60*1000:
            if float(latest['high']) >= t_symbols_analyze.f_channel_up:
                t_symbols_analyze.f_breakout_trend = 1
                t_symbols_analyze.f_breakout_price = float(latest['close'])
                t_symbols_analyze.f_breakout_price_highest = t_symbols_analyze.f_channel_up
                t_symbols_analyze.f_breakout_rate = t_symbols_analyze.f_channel_up / t_symbols_analyze.f_breakout_price
            elif float(latest['low']) <= t_symbols_analyze.f_channel_low:
                t_symbols_analyze.f_breakout_trend = -1
                t_symbols_analyze.f_breakout_price = float(latest['close'])
                t_symbols_analyze.f_breakout_price_highest = t_symbols_analyze.f_channel_low
                t_symbols_analyze.f_breakout_rate = t_symbols_analyze.f_channel_low / t_symbols_analyze.f_breakout_price
            if t_symbols_analyze.f_breakout_trend != 0:
                t_symbols_analyze.f_breakout_ts = date_ms
                t_symbols_analyze.f_breakout_volume = float(latest['volume'])
                t_symbols_analyze.f_breakout_volume_rate = t_symbols_analyze.f_breakout_volume / t_symbols_analyze.f_volume_mean
                t_symbols_analyze.f_breakout_price_highest_ts =  t_symbols_analyze.f_breakout_ts
                t_symbols_analyze.f_breakout_rate_max = t_symbols_analyze.f_breakout_rate
        elif t_symbols_analyze.f_breakout_trend == 1:
            if t_symbols_analyze.f_channel_up > t_symbols_analyze.f_breakout_price_highest:
                t_symbols_analyze.f_breakout_price_highest = max(t_symbols_analyze.f_breakout_price_highest, t_symbols_analyze.f_channel_up)
                t_symbols_analyze.f_breakout_price_highest_ts = date_ms
            t_symbols_analyze.f_breakout_rate_max = max(t_symbols_analyze.f_breakout_rate_max, t_symbols_analyze.f_breakout_rate)
        elif t_symbols_analyze.f_breakout_trend == -1:
            if t_symbols_analyze.f_channel_low < t_symbols_analyze.f_breakout_price_highest:
                t_symbols_analyze.f_breakout_price_highest = min(t_symbols_analyze.f_breakout_price_highest, t_symbols_analyze.f_channel_low)
                t_symbols_analyze.f_breakout_price_highest_ts = date_ms
            t_symbols_analyze.f_breakout_rate_max = min(t_symbols_analyze.f_breakout_rate_max, t_symbols_analyze.f_breakout_rate)
        s.merge(t_symbols_analyze)
        s.flush()
        logger.debug(self.to_string() + "update_db() t_symbols_analyze  flush  ")

        self.pub_topic(t_symbols_analyze)

    '''
    ['f_ex_id', 'f_symbol', 'f_timeframe', 'f_bid', 'f_ask', 'f_spread', 'f_bar_trend', 'f_volume_mean', 'f_volume', 'f_ma_period', 'f_ma_up', 'f_ma_low', 'f_ma_trend', 'f_channel_period', 'f_channel_up', 'f_channel_low', 'f_breakout_trend', 'f_breakout_ts', 'f_breakout_price', 'f_breakout_volume', 'f_breakout_volume_rate', 'f_breakout_price_highest', 'f_breakout_price_highest_ts', 'f_breakout_rate', 'f_breakout_rate_max', 'f_ts_update']
    '''
    def pub_topic(self, t_symbols_analyze):
        #logger.debug(self.to_string() + "pub_topic() t_symbols_analyze={0}".format(t_symbols_analyze))
        if self.datahub is None:
            return
        topic_name = "t_symbols_analyze"
        topic, shards = self.datahub.get_topic(topic_name)
        record = TupleRecord(schema=topic.record_schema)
        record.values = [
            t_symbols_analyze.f_ex_id,
            t_symbols_analyze.f_symbol,
            t_symbols_analyze.f_timeframe,
            t_symbols_analyze.f_bid,
            t_symbols_analyze.f_ask,
            t_symbols_analyze.f_spread,
            t_symbols_analyze.f_bar_trend,
            t_symbols_analyze.f_volume_mean,
            t_symbols_analyze.f_volume,
            t_symbols_analyze.f_ma_period,
            t_symbols_analyze.f_ma_up,
            t_symbols_analyze.f_ma_low,
            t_symbols_analyze.f_ma_trend,
            t_symbols_analyze.f_channel_period,
            t_symbols_analyze.f_channel_up,
            t_symbols_analyze.f_channel_low,
            t_symbols_analyze.f_breakout_trend,
            t_symbols_analyze.f_breakout_ts,
            t_symbols_analyze.f_breakout_price,
            t_symbols_analyze.f_breakout_volume,
            t_symbols_analyze.f_breakout_volume_rate,
            t_symbols_analyze.f_breakout_price_highest,
            t_symbols_analyze.f_breakout_price_highest_ts,
            t_symbols_analyze.f_breakout_rate,
            t_symbols_analyze.f_breakout_rate_max,
            arrow.utcnow().timestamp * 1000
        ]
        record.shard_id = shards[randint(1, 1000) % len(shards)].shard_id
        self.datahub.pub_topic(topic_name, [record])
        




