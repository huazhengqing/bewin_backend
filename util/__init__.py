import os
import sys
import math
import asyncio
import logging
import traceback
from collections import defaultdict
import ccxt.async_support as ccxt
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf




_EXCHANGE_URLS = {
    ccxt.bittrex.__name__: '/Market/Index?MarketName={quote}-{base}',
    ccxt.binance.__name__: '/tradeDetail.html?symbol={base}_{quote}'
}



TimeFrame_Minutes = {
    '1m': 1,
    #'3m': 3,
    '5m': 5,
    '15m': 15,
    '30m': 30,
    '1h': 60,
    #'2h': 120,
    '4h': 240,
    #'6h': 360,
    #'8h': 480,
    #'12h': 720,
    '1d': 1440,
    #'3d': 4320,
    '1w': 10080,
}


Minutes_TimeFrame = {
    1: '1m',
    5: '5m',
    15: '15m',
    30: '30m',
    60: '1h',
    240: '4h',
    1440: '1d',
    10080: '1w',
}





def get_log(name = __name__):
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)-8s: %(message)s')
    #file_handler = logging.FileHandler(conf.dir_log + name + "_{0}.log".format(int(arrow.utcnow().timestamp * 1000)), mode="w", encoding="utf-8")
    file_handler = logging.FileHandler(conf.dir_log + name + ".log", mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    return logger
logger = get_log(__name__)



API_RETRY_COUNT = 4
def retry(f):
    def wrapper(*args, **kwargs):
        count = kwargs.pop('count', API_RETRY_COUNT)
        try:
            return f(*args, **kwargs)
        except Exception as ex:
            if count > 0:
                count -= 1
                kwargs.update({'count': count})
                return wrapper(*args, **kwargs)
            else:
                raise ex
    return wrapper



def nesteddict(): 
  return defaultdict(nesteddict)


def symbol_2_string(symbol):
    base = symbol.split('/')[0]       # BTC
    quote = symbol.split('/')[1]       # USD
    s = base + '_' + quote
    return s



def downRound(qty, decimal_places):
    return int(qty * math.pow(10, decimal_places)) / int(math.pow(10, decimal_places))





def sub_loop(func, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    tasks = []
    tasks.append(asyncio.ensure_future(func(*args, **kwargs)))
    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    except:
        logger.error(traceback.format_exc())
    loop.close()


def thread_loop(executor, func, *args, **kwargs):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(loop.run_in_executor(executor, func, *args, **kwargs))
    except:
        logger.error(traceback.format_exc())
    loop.close()
    





