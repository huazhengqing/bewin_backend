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
    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        #logger.debug("strategy_breakout()  ")
        dataframe['min'] = ta.MIN(dataframe, timeperiod=self._channel_period)
        dataframe['max'] = ta.MAX(dataframe, timeperiod=self._channel_period)

        dataframe['ma_high'] = ta.EMA(dataframe, timeperiod=self._ma_period, price='high')
        dataframe['ma_low'] = ta.EMA(dataframe, timeperiod=self._ma_period, price='low')
        dataframe['ma_close'] = ta.EMA(dataframe, timeperiod=self._ma_period, price='close')

        heikinashi = qtpylib.heikinashi(dataframe)
        dataframe['ha_open'] = heikinashi['open']
        dataframe['ha_close'] = heikinashi['close']
        dataframe['ha_high'] = heikinashi['high']
        dataframe['ha_low'] = heikinashi['low']

        dataframe['atr'] = qtpylib.atr(dataframe)

        #logger.debug("strategy_breakout() end  dataframe={0} ".format(dataframe))
        return dataframe

    def long(self, dataframe: DataFrame) -> DataFrame:
        dataframe.loc[
            (
                (dataframe['ha_high'] >= dataframe['max']) &
                (dataframe['max'] >= 0) 
            )
            , 'long'] = 1
        #logger.debug("long() dataframe={0} ".format(dataframe))
        return dataframe

    def short(self, dataframe: DataFrame) -> DataFrame:
        #logger.debug("short()  ")
        dataframe.loc[
            (
                (dataframe['ha_low'] <= dataframe['min']) &
                (dataframe['min'] >= 0) 
            )
            , 'short'] = 1
        #dataframe.loc[False, 'short'] = 1
        return dataframe

    def close(self, dataframe: DataFrame) -> DataFrame:
        #logger.debug("close()  ")
        #dataframe.loc[False, 'close'] = 1
        return dataframe


