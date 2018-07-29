import os
import sys
import ccxt.async_support as ccxt
from pandas import DataFrame, to_datetime
from typing import List, Dict, Any, Optional
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import db
import util
logger = util.get_log(__name__)




_EXCHANGE_URLS = {
    ccxt.bittrex.__name__: '/Market/Index?MarketName={quote}-{base}',
    ccxt.binance.__name__: '/tradeDetail.html?symbol={base}_{quote}'
}




#======================================

g_ex_markets = util.nesteddict()

def load_markets_db(id):
    global g_ex_markets
    t_markets = db.Session().query(db.t_markets).filter(db.t_markets.f_ex_id == id).all()
    for t_market in t_markets:
        g_ex_markets[id][t_market.f_symbol] = t_market

def load_markets_all():
    for id in ccxt.exchanges:
        load_markets_db(id)

#======================================

g_symbol_ex_ticker = util.nesteddict()





#======================================

def parse_ohlcv_dataframe(list_ohlcv: list) -> DataFrame:
    #logger.debug("parse_ohlcv_dataframe() start  len={0} ".format(len(list_ohlcv)))
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
    #frame.drop(frame.tail(1).index, inplace=True) 
    if len(frame) < 60:
        logger.info("parse_ohlcv_dataframe() end  len={0} ".format(len(frame)))
    #logger.debug("parse_ohlcv_dataframe() end  len(frame)={0} ".format(len(frame)))
    return frame



#======================================

def safe_string(dictionary, key, default_value=None):
    return ccxt.Exchange.safe_string(dictionary, key, default_value)

def safe_lower_string(dictionary, key, default_value=None):
    value = safe_string(dictionary, key, default_value)
    if value is not None:
        value = value.lower()
    return value

def safe_float(dictionary, key, default_value=None):
    return ccxt.Exchange.safe_float(dictionary, key, default_value)

def safe_int(dictionary, key, default_value=None):
    return ccxt.Exchange.safe_integer(dictionary, key, default_value)

def safe_value(dictionary, key, default_value=None):
    return ccxt.Exchange.safe_value(dictionary, key, default_value)

def safe_iso8601(value):
    return ccxt.Exchange.iso8601(value)

def split_symbol(symbol):
    splitted = symbol.split('/')
    return splitted[0], splitted[1]

def merge_symbol(symbol):
    return symbol.replace('/', "")

def merge_base_quote(base, quote):
    return "{0}/{1}".format(base, quote)

def symbol_2_ccxt(symbol):
    if "/" in symbol:
        return symbol.upper()
    base = symbol[:-3]
    quote = symbol[-3:]
    return "{0}/{1}".format(base, quote).upper()

def symbol_2_string(symbol):
    splitted = symbol.split('/')
    return "{0}_{1}".format(splitted[0], splitted[1]).upper()
