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
    
    def reset_para(self):
        self.ma_period : int = 34
        self.channel_period : int = 40
        self.atr_period = 14

    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        self.reset_para()
        len_df = len(dataframe.index)
        if len_df < self.ma_period + 2:
            self.ma_period = int(len_df * 0.8)
        if len_df < self.channel_period + 2:
            self.channel_period = int(len_df * 0.8)
        if len_df < self.atr_period:
            self.atr_period = int(len_df -1)

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

        dataframe['volume_mean'] = dataframe['volume'].shift(1).tail(self.atr_period).mean()
        
    
        #logger.debug("strategy_breakout() end  dataframe={0} ".format(dataframe))
        return dataframe

    def buy(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['ha_open'] < dataframe['ha_close']) &
                (dataframe['ha_high'] > dataframe['max']) 
            )
            , 'buy'] = 1
        return dataframe

    def sell(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['ha_open'] > dataframe['ha_close']) &
                (dataframe['low'] < dataframe['min']) 
            )
            , 'sell'] = 1
        return dataframe


