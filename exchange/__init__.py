import os
import sys
from pandas import DataFrame, to_datetime
from typing import List, Dict, Any, Optional
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import util
logger = util.get_log(__name__)



def parse_ohlcv_dataframe(list_ohlcv: list) -> DataFrame:
    logger.debug("parse_ohlcv_dataframe() start  len={0} ".format(len(list_ohlcv)))
    cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    frame = DataFrame(list_ohlcv, columns=cols)
    frame['date'] = to_datetime(
        frame['date'],
        unit='ms',
        utc=True,
        infer_datetime_format=True
    )
    frame = frame.groupby(by='date', as_index=False, sort=True).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'max',
    })
    frame.drop(frame.tail(1).index, inplace=True) 
    logger.debug("parse_ohlcv_dataframe() end  len(frame)={0} ".format(len(frame)))
    return frame





