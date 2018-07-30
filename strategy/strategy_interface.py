import os
import sys
import numpy
import talib.abstract as ta
from functools import reduce
from abc import ABC, abstractmethod
from typing import Dict, List, NamedTuple, Tuple
from pandas import DataFrame, DatetimeIndex, merge
from datetime import datetime
from enum import Enum
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import vendor.qtpylib.indicators as qtpylib
from strategy.indicator_helpers import fishers_inverse
import util
logger = util.get_log(__name__)



class IStrategy(ABC):
    def __init__(self)-> None:
        self.exchange : str
        self.symbol : str
        self.timeframe : int = 60
        self.ma_period : int = 34
        self.channel_period : int = 40
        self.atr_period : int = 14

        self.stoploss: float = -0.5
        self.stoploss_absolute: float = 0.0

    def get_strategy_name(self) -> str:
        return self.__class__.__name__

    def reset_para(self):
        self.ma_period : int = 34
        self.channel_period : int = 40
        self.atr_period = 14
    
    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        dataframe['min'] = ta.MIN(dataframe, timeperiod=self.channel_period, price='low').shift(1)
        dataframe['max'] = ta.MAX(dataframe, timeperiod=self.channel_period, price='high').shift(1)

        dataframe['ma_high'] = ta.EMA(dataframe, timeperiod=self.ma_period, price='high')
        dataframe['ma_low'] = ta.EMA(dataframe, timeperiod=self.ma_period, price='low')
        dataframe['ma_close'] = ta.EMA(dataframe, timeperiod=self.ma_period, price='close')
        dataframe.loc[(dataframe['ma_close'].shift(1) < dataframe['ma_close']), 'ma_trend'] = 1
        dataframe.loc[(dataframe['ma_close'].shift(1) > dataframe['ma_close']), 'ma_trend'] = -1
        dataframe.loc[(dataframe['ma_close'].shift(1) == dataframe['ma_close']), 'ma_trend'] = 0

        heikinashi = qtpylib.heikinashi(dataframe)
        dataframe['ha_open'] = heikinashi['open']
        dataframe['ha_close'] = heikinashi['close']
        dataframe['ha_high'] = heikinashi['high']
        dataframe['ha_low'] = heikinashi['low']

        dataframe['atr'] = qtpylib.atr(dataframe, self.atr_period)
        dataframe['volume_mean'] = qtpylib.rolling_mean(dataframe['volume'], self.atr_period).shift(1)
        dataframe['stoploss'] = qtpylib.rolling_min(dataframe['ha_low'], 2).shift(1)

        bollinger = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=20, stds=2)
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_upperband'] = bollinger['upper']
        dataframe['bb_middleband'] = bollinger['mid']

        return dataframe
    
    def buy(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['ha_open'] < dataframe['ha_close']) &
                (dataframe['close'] > dataframe['max']) 
            )
            , 'buy'] = 1
        return dataframe

    def sell(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['min']) 
            ),
            'sell'] = 1
        return dataframe

    def resample(self, dataframe: DataFrame, period: int) -> DataFrame:
        df = dataframe.copy()
        df = df.set_index(DatetimeIndex(df['date']))
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        }
        #df = df.resample(str(int(period)) + 'min', how=ohlc_dict).dropna(how='any')
        df = df.resample(str(int(period)) + 'min').apply(ohlc_dict).dropna(how='any')

        df['{}_min'.format(period)] = ta.MIN(df, timeperiod=self.channel_period, price='low').shift(1)
        df['{}_max'.format(period)] = ta.MAX(df, timeperiod=self.channel_period, price='high').shift(1)

        df['{}_ma_high'.format(period)] = ta.EMA(df, timeperiod=self.ma_period, price='high')
        df['{}_ma_low'.format(period)] = ta.EMA(df, timeperiod=self.ma_period, price='low')
        df['{}_ma_close'.format(period)] = ta.EMA(df, timeperiod=self.ma_period, price='close')

        heikinashi = qtpylib.heikinashi(df)
        df['{}_ha_open'.format(period)] = heikinashi['open']
        df['{}_ha_close'.format(period)] = heikinashi['close']
        df['{}_ha_high'.format(period)] = heikinashi['high']
        df['{}_ha_low'.format(period)] = heikinashi['low']

        df['{}_stoploss'.format(period)] = qtpylib.rolling_min(df['{}_ha_low'.format(period)], 2).shift(1)

        df = df.drop(columns=['open', 'high', 'low', 'close', 'volume'])
        df = df.resample(str(self.timeframe) + 'min')
        df = df.interpolate(method='time')
        df['date'] = df.index
        df.index = range(len(df))
        dataframe = merge(dataframe, df, on='date', how='left')

        return dataframe
