# pragma pylint: disable=missing-docstring, invalid-name, pointless-string-statement
import os
import sys
import talib.abstract as ta
from pandas import DataFrame
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import vendor.qtpylib.indicators as qtpylib
from strategy.indicator_helpers import fishers_inverse
from strategy.strategy_interface import IStrategy
import conf.conf_aliyun
import conf
import util
import db
from exchange.exchange import exchange
logger = util.get_log(__name__)


class strategy_breakout(IStrategy):
    def __init__(self)-> None:
        super(strategy_breakout, self).__init__()

    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        self.reset_para()
        len_df = len(dataframe.index)
        if len_df < self.ma_period + 2:
            self.ma_period = int(len_df * 0.8)
        if len_df < self.channel_period + 2:
            self.channel_period = int(len_df * 0.8)
        if len_df < self.atr_period:
            self.atr_period = int(len_df -1)

        return super().calc_indicators(dataframe)

    def buy(self, dataframe: DataFrame) -> DataFrame:
        return super().buy(dataframe)

    def sell(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['stoploss']) 
            ) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['ma_low']) 
            ) |
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] <= dataframe['min']) 
            ),
            'sell'] = 1
        return dataframe
        


