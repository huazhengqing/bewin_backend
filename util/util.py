#!/usr/bin/python
import io
import os
import sys
import uuid
import math
import time
import logging
import datetime
import traceback
import ccxt.async as ccxt
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_ex
import conf.conf_aliyun


def get_log(name = __name__):
    logger = logging.getLogger(name)
    if logger.hasHandlers():
        return logger
    formatter = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)-8s: %(message)s')
    #file_handler = logging.FileHandler(conf.conf_ex.dir_log + name + "_{0}.log".format(int(time.time())), mode="w", encoding="utf-8")
    file_handler = logging.FileHandler(conf.conf_ex.dir_log + name + ".log", mode="w", encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.DEBUG)
    return logger
logger = get_log(__name__)


def get_exchange(id, use_private_interface):
    if id in ccxt.exchanges:
        exchange = getattr(ccxt, id)({
            #'rateLimit': 1000,
            'enableRateLimit': True,
            #'timeout': 20000,
            #'verbose': True,
            #'session': cfscrape.create_scraper(),
        })
        if use_private_interface:
            if id in conf.conf_ex.conf_ex:
                if 'apiKey' in conf.conf_ex.conf_ex[id]:
                    exchange.apiKey = conf.conf_ex.conf_ex[id]['apiKey']
                if 'secret' in conf.conf_ex.conf_ex[id]:
                    exchange.secret = conf.conf_ex.conf_ex[id]['secret']
                if 'password' in conf.conf_ex.conf_ex[id]:
                    exchange.password = conf.conf_ex.conf_ex[id]['password']      # GDAX requires a password!
                    #exchange.urls['api'] = 'https://api-public.sandbox.gdax.com'       # use the testnet for GDAX
                if 'uid' in conf.conf_ex.conf_ex[id]:
                    exchange.uid = conf.conf_ex.conf_ex[id]['uid']                # QuadrigaCX requires uid!
        
        if id in conf.conf_ex.conf_ex:
            if 'rateLimit' in conf.conf_ex.conf_ex[id] and conf.conf_ex.conf_ex[id]['rateLimit'] > exchange.rateLimit:
                exchange.rateLimit = conf.conf_ex.conf_ex[id]['rateLimit']

        if conf.conf_aliyun.dev_or_product == 1:
            #if id in ['binance', 'huobi', 'huobicny', 'huobipro', 'okcoincny', 'okcoinusd', 'okex', 'zb']:
            exchange.aiohttp_proxy = conf.conf_ex.proxies_aiohttp[0]
        
        #exchange.proxy = proxies_cors[0]
        #exchange.aiohttp_proxy = proxies_aiohttp[0]
        #exchange.proxies = proxies_sync

        '''
        print('Cfscraping...')
        url = exchange.urls['www']
        tokens, user_agent = cfscrape.get_tokens(url)
        exchange.headers = {
            'cookie': '; '.join([key + '=' + tokens[key] for key in tokens]),
            'user-agent': user_agent,
        }
        pprint(exchange.headers)
        '''
        return exchange
    else:
        print_supported_exchanges()
        raise Exception("[" + id +"] not found")
    return None

def to_str(*args):
    return ' '.join([str(arg) for arg in args])

def print_supported_exchanges():
    s = to_str('Supported exchanges:', ', '.join(ccxt.exchanges))
    logger.info(s)

def symbol_2_string(symbol):
    base = symbol.split('/')[0]       # BTC
    quote = symbol.split('/')[1]       # USD
    s = base + '_' + quote
    return s

def downRound(qty, decimal_places):
    return int(qty * math.pow(10, decimal_places)) / int(math.pow(10, decimal_places))



# 获取当前时间，返回字符串，格式为：'YYYYMMDD_hhmmss'
def current_time_str():
    current_time = datetime.datetime.now()
    time_string = current_time.strftime('%Y%m%d_%H%M%S')
    return time_string

# 将时间戳转化为可读时间
def timestamp_to_timestr(timestamp):
    time_struct = time.localtime(timestamp)
    time_string = time.strftime("%Y%m%d_%H%M%S", time_struct)
    return time_string

# 计算时间差
def diff_times_in_seconds(t1, t2):
    # caveat emptor - assumes t1 & t2 are python times, on the same day and t2 is after t1
    h1, m1, s1 = t1.hour, t1.minute, t1.second
    h2, m2, s2 = t2.hour, t2.minute, t2.second
    t1_secs = s1 + 60 * (m1 + 60*h1)
    t2_secs = s2 + 60 * (m2 + 60*h2)
    return( t2_secs - t1_secs)









































