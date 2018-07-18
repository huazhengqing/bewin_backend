from abc import ABC, abstractmethod
from typing import Dict
from pandas import DataFrame


class IStrategy(ABC):
    _exchange : str
    _symbol : str
    _timeframe : int = 1
    _ma_period : int = 34
    _channel_period : int = 40

    _max_open_trades : int = 1

    _stoploss_rate : float = -0.3
    _trailing_stop_rate : float = -0.3
    _trailing_stop_rate_positive : float = -0.3
    _trailing_stop_channel : float = 0.0

    _stoploss: float = 0.0

    @abstractmethod
    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def buy(self, dataframe: DataFrame) -> DataFrame:
        pass
        
    @abstractmethod
    def sell(self, dataframe: DataFrame) -> DataFrame:
        pass
        