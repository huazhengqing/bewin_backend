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
        self.timeframe : int = 15
        self.resample_tf_240 : int = 240

    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        dataframe = super().resample(dataframe, self.resample_tf_240)
        dataframe = super().calc_indicators(dataframe)
        return dataframe

    def buy(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['240_ma_close'].shift(1) < dataframe['240_ma_close']) &
                (dataframe['240_ha_close'] > dataframe['240_ma_high']) &
                (dataframe['240_ha_open'] < dataframe['240_ha_close']) &
                (dataframe['ha_open'] < dataframe['ha_close']) &
                (dataframe['close'] > dataframe['max']) 
                #(dataframe['volume'] > dataframe['volume'].mean() * 4)
            )
            , 'buy'] = 1
        return dataframe

    def sell(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            #(
            #    (dataframe['ha_open'] > dataframe['ha_close']) &
            #    (dataframe['low'] <= dataframe['stoploss']) 
            #) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['ma_low']) 
            ) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['240_stoploss']) 
            ) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['min']) 
            ) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['240_ma_low']) 
            ) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] < dataframe['240_min'])
            ),
            'sell'] = 1
        return dataframe
