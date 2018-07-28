from abc import ABC, abstractmethod
from typing import Dict, List, NamedTuple, Tuple
from pandas import DataFrame
from datetime import datetime
from enum import Enum



class IStrategy(ABC):
    def __init__(self)-> None:
        self.exchange : str
        self.symbol : str
        self.timeframe : int = 30
        self.ma_period : int = 34
        self.channel_period : int = 40
        self.atr_period : int = 14

        self.stoploss: float = -0.5
        self.stoploss_absolute: float = 0

    @abstractmethod
    def calc_indicators(self, dataframe: DataFrame) -> DataFrame:
        pass
    
    @abstractmethod
    def buy(self, dataframe: DataFrame) -> DataFrame:
        pass
        
    @abstractmethod
    def sell(self, dataframe: DataFrame) -> DataFrame:
        pass

    def get_strategy_name(self) -> str:
        return self.__class__.__name__
