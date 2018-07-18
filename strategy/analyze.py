import os
import sys
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Tuple
import arrow
from random import randint
from pandas import DataFrame, to_datetime
from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
from strategy_interface import IStrategy
from strategy_breakout import strategy_breakout
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf
import util
import db
from db.datahub import datahub
from exchange.exchange import exchange
logger = util.get_log(__name__)


class SignalType(Enum):
    BUY = "buy"
    SELL = "sell"



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

        db.init()
        self.ohlcv_list = self.load_ohlcv_from_db()
        self.dataframe = None
        
        self.datahub = None

    def to_string(self):
        return "analyze[{0},{1},{2},{3}] ".format(self.userid, self.ex_id, self.symbol, self.timeframe)

    def load_ohlcv_from_db(self):
        list_ohlcv = []
        for t_ohlcv in db.t_ohlcv.query.filter(
            db.t_ohlcv.f_ex_id == self.ex_id,
            db.t_ohlcv.f_symbol == self.symbol,
            db.t_ohlcv.f_timeframe == self.timeframe
            ).order_by(db.t_ohlcv.f_ts).limit(1000):
            list_ohlcv.append([t_ohlcv.f_ts, t_ohlcv.f_o, t_ohlcv.f_h, t_ohlcv.f_l, t_ohlcv.f_c, t_ohlcv.f_v])
        return list_ohlcv

    @staticmethod
    def parse_ohlcv_dataframe(list_ohlcv: list) -> DataFrame:
        cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        frame = DataFrame(list_ohlcv, columns=cols)
        frame['date'] = to_datetime(frame['date'],
                                    unit='ms',
                                    utc=True,
                                    infer_datetime_format=True)
        # group by index and aggregate results to eliminate duplicate ticks
        frame = frame.groupby(by='date', as_index=False, sort=True).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'max',
        })
        frame.drop(frame.tail(1).index, inplace=True)     # eliminate partial candle
        return frame

    def calc_signal(self, ohlcv : List[Dict]) -> Tuple[bool, bool]:
        self.ohlcv_list.extend(ohlcv)
        if self.ohlcv_list is None or len(self.ohlcv_list) <= 0:
            return False, False
        try:
            self.dataframe = self.parse_ohlcv_dataframe(self.ohlcv_list)
            self.dataframe = self.strategy.calc_indicators(self.dataframe)
            self.dataframe = self.strategy.buy(self.dataframe)
            self.dataframe = self.strategy.sell(self.dataframe)
        except Exception as e:
            logger.warning(self.to_string() +  'run() Exception= {0}'.format(e))
            return False, False
        if self.dataframe.empty:
            return False, False
        latest = self.dataframe.iloc[-1]
        signal_date = arrow.get(latest['date'])
        if signal_date < (arrow.utcnow().shift(minutes=-(self.timeframe * 2 + 5))):
            logger.warning(self.to_string() +  'run() ohlcv is old = {0}m'.format((arrow.utcnow() - signal_date).seconds // 60))
            return False, False
        if (self.userid == 0):
            self.update_db()
        (buy, sell) = latest[SignalType.BUY.value] == 1, latest[SignalType.SELL.value] == 1
        return buy, sell

    def update_db(self):
        t_symbols_analyze = db.t_symbols_analyze(
            f_ex_id = self.ex_id,
            f_symbol = self.symbol,
            f_timeframe = self.timeframe,
            f_bar_trend = self.dataframe['ha_open'] < self.dataframe['ha_close'] and 1 or -1,
            f_volume_mean = self.dataframe['volume'].mean(),
            f_volume = self.dataframe['volume'],
            f_ma_period = self.strategy._ma_period,
            f_ma_up = self.dataframe['ma_high'],
            f_ma_low = self.dataframe['ma_low'],
            f_ma_trend = self.dataframe['ma_close'].shift(1) < self.dataframe['ma_close'] and 1 or -1,
            f_channel_period = self.strategy._channel_period,
            f_channel_up = self.dataframe['max'].shfit(1),
            f_channel_low = self.dataframe['min']
        )
        db.t_symbols_analyze.session.merge(t_symbols_analyze)
        db.t_symbols_analyze.session.flush()

        t_symbols_analyze = db.t_symbols_analyze.query.filter(
            db.t_symbols_analyze.f_ex_id == self.ex_id,
            db.t_symbols_analyze.f_symbol == self.symbol,
            db.t_symbols_analyze.f_timeframe == self.timeframe
            ).first()
        if t_symbols_analyze is None:
            raise Exception(self.to_string() + "t_symbols_analyze = None")

        if t_symbols_analyze.f_breakout_trend == 0 or self.dataframe['date'] - t_symbols_analyze.f_breakout_ts > 3*60*60*1000:
            if self.dataframe['close'] > t_symbols_analyze.f_channel_up:
                t_symbols_analyze.f_breakout_trend = 1
                t_symbols_analyze.f_breakout_price = self.dataframe['close']
                t_symbols_analyze.f_breakout_price_highest = t_symbols_analyze.f_channel_up
                t_symbols_analyze.f_breakout_rate = t_symbols_analyze.f_channel_up / t_symbols_analyze.f_breakout_price
            elif self.dataframe['close'] < t_symbols_analyze.f_channel_low:
                t_symbols_analyze.f_breakout_trend = -1
                t_symbols_analyze.f_breakout_price = self.dataframe['close']
                t_symbols_analyze.f_breakout_price_highest = t_symbols_analyze.f_channel_low
                t_symbols_analyze.f_breakout_rate = t_symbols_analyze.f_channel_low / t_symbols_analyze.f_breakout_price
            if t_symbols_analyze.f_breakout_trend != 0:
                t_symbols_analyze.f_breakout_ts = self.dataframe['date']
                t_symbols_analyze.f_breakout_volume = self.dataframe['volume']
                t_symbols_analyze.f_breakout_volume_rate = t_symbols_analyze.f_breakout_volume / t_symbols_analyze.f_volume_mean
                t_symbols_analyze.f_breakout_price_highest_ts =  t_symbols_analyze.f_breakout_ts
                t_symbols_analyze.f_breakout_rate_max = t_symbols_analyze.f_breakout_rate
        elif t_symbols_analyze.f_breakout_trend == 1:
            if t_symbols_analyze.f_channel_up > t_symbols_analyze.f_breakout_price_highest:
                t_symbols_analyze.f_breakout_price_highest = max(t_symbols_analyze.f_breakout_price_highest, t_symbols_analyze.f_channel_up)
                t_symbols_analyze.f_breakout_price_highest_ts = self.dataframe['date']
            t_symbols_analyze.f_breakout_rate_max = max(t_symbols_analyze.f_breakout_rate_max, t_symbols_analyze.f_breakout_rate)
        elif t_symbols_analyze.f_breakout_trend == -1:
            if t_symbols_analyze.f_channel_low < t_symbols_analyze.f_breakout_price_highest:
                t_symbols_analyze.f_breakout_price_highest = min(t_symbols_analyze.f_breakout_price_highest, t_symbols_analyze.f_channel_low)
                t_symbols_analyze.f_breakout_price_highest_ts = self.dataframe['date']
            t_symbols_analyze.f_breakout_rate_max = min(t_symbols_analyze.f_breakout_rate_max, t_symbols_analyze.f_breakout_rate)
        db.t_symbols_analyze.session.merge(t_symbols_analyze)
        db.t_symbols_analyze.session.flush()

        self.pub_topic(t_symbols_analyze)


    def pub_topic(self, t_symbols_analyze):
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
            t_symbols_analyze.f_ts_update
        ]
        record.shard_id = shards[randint(1, 1000) % len(shards)].shard_id
        self.datahub.pub_topic(topic_name, [record])
                
