import os
import sys
import numpy
import talib.abstract as ta
from typing import Dict, List
from functools import reduce
from pandas import DataFrame, DatetimeIndex, merge
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import vendor.qtpylib.indicators as qtpylib
from strategy.indicator_helpers import fishers_inverse
from strategy.strategy_interface import IStrategy
import util
logger = util.get_log(__name__)




class reaper_trend(IStrategy):
    resample_period_60 = 60
    resample_period_240 = 240
    resample_channel_period = 40
    resample_ma_period = 34

    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        dataframe = self.resample(dataframe)

        dataframe['min'] = ta.MIN(dataframe, timeperiod=self.channel_period)
        dataframe['max'] = ta.MAX(dataframe, timeperiod=self.channel_period)

        heikinashi = qtpylib.heikinashi(dataframe)
        dataframe['ha_open'] = heikinashi['open']
        dataframe['ha_close'] = heikinashi['close']
        dataframe['ha_high'] = heikinashi['high']
        dataframe['ha_low'] = heikinashi['low']

        dataframe['atr'] = qtpylib.atr(dataframe)

        bollinger = qtpylib.bollinger_bands(qtpylib.typical_price(dataframe), window=20, stds=2)
        dataframe['bb_lowerband'] = bollinger['lower']
        dataframe['bb_upperband'] = bollinger['upper']
        dataframe['bb_middleband'] = bollinger['mid']

        return dataframe

    def buy(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['240_ma_close'].shift(1) < dataframe['240_ma_close']) &
                (dataframe['240_ha_close'] > dataframe['240_ma_high']) &
                (dataframe['240_ha_open'] < dataframe['240_ha_close']) &
                (dataframe['ha_open'] < dataframe['ha_close']) &
                (dataframe['ha_high'] > dataframe['max'].shift(1)) 
                #(dataframe['volume'] > dataframe['volume'].mean() * 4)
            )
            , 'buy'] = 1
        if dataframe.empty:
            return dataframe
        latest = dataframe.iloc[-1]
        if latest['buy'] == 1:
            self.stoploss_absolute = latest['min'] - latest['atr']
        return dataframe

    def sell(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['low'] < dataframe['240_min'].shift(1))
            ),
            'sell'] = 1
        return dataframe

    def resample(self, dataframe: DataFrame) -> DataFrame:
        df = dataframe.copy()
        df = df.set_index(DatetimeIndex(df['date']))
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
            #'volume': 'sum'
        }
        df = df.resample(str(int(self.resample_period_240)) + 'min', how=ohlc_dict).dropna(how='any')

        df['240_ma_high'] = ta.EMA(df, timeperiod=self.resample_ma_period, price='high')
        df['240_ma_low'] = ta.EMA(df, timeperiod=self.resample_ma_period, price='low')
        df['240_ma_close'] = ta.EMA(df, timeperiod=self.resample_ma_period, price='close')

        df['240_min'] = ta.MIN(df, timeperiod=self.resample_channel_period)
        df['240_max'] = ta.MAX(df, timeperiod=self.resample_channel_period)

        heikinashi = qtpylib.heikinashi(df)
        df['240_ha_open'] = heikinashi['open']
        df['240_ha_close'] = heikinashi['close']
        df['240_ha_high'] = heikinashi['high']
        df['240_ha_low'] = heikinashi['low']

        #df['240_volume'] = df['volume']

        df = df.drop(columns=['open', 'high', 'low', 'close'])

        df = df.resample(self.ticker_interval[:-1] + 'min')
        df = df.interpolate(method='time')
        df['date'] = df.index
        df.index = range(len(df))
        dataframe = merge(dataframe, df, on='date', how='left')
        return dataframe

