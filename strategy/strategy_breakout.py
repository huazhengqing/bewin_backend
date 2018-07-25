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
        dataframe['min'] = ta.MIN(dataframe, timeperiod=self._channel_period).shift(1)
        dataframe['max'] = ta.MAX(dataframe, timeperiod=self._channel_period).shift(1)

        dataframe['ma_high'] = ta.EMA(dataframe, timeperiod=self._ma_period, price='high')
        dataframe['ma_low'] = ta.EMA(dataframe, timeperiod=self._ma_period, price='low')
        dataframe['ma_close'] = ta.EMA(dataframe, timeperiod=self._ma_period, price='close')
        dataframe.loc[(dataframe['ma_close'].shift(1) < dataframe['ma_close']), 'ma_trend'] = 1
        dataframe.loc[(dataframe['ma_close'].shift(1) > dataframe['ma_close']), 'ma_trend'] = -1

        heikinashi = qtpylib.heikinashi(dataframe)
        dataframe['ha_open'] = heikinashi['open']
        dataframe['ha_close'] = heikinashi['close']
        dataframe['ha_high'] = heikinashi['high']
        dataframe['ha_low'] = heikinashi['low']

        dataframe['atr'] = qtpylib.atr(dataframe)

        dataframe['volume_mean'] = dataframe['volume'].mean().shift(1)
    
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


