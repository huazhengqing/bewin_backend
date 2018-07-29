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


class breakout_trend(IStrategy):
    def __init__(self)-> None:
        super(breakout_trend, self).__init__()
        self.timeframe : int = 60
        self.resample_period_240 = 240

    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        dataframe = super().resample(dataframe, self.resample_period_240)
        dataframe = super().calc_indicators(dataframe)
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

        latest = dataframe.iloc[-1]
        if latest['buy'] == 1:
            self.stoploss_absolute = latest['min'] - latest['atr']
        return dataframe

    def sell(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['low'] < ta.MIN(dataframe, timeperiod=2, price='ha_low')) &
                (dataframe['ha_open'] > dataframe['ha_close'])
            ),
            'sell'] = 1
        return dataframe
