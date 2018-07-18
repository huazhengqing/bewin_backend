#!/usr/bin/python
import os
import sys
import math
import time
import arrow
import random
import typing
import logging
import asyncio
import datetime
import traceback
import multiprocessing
import sqlalchemy as sql
import ccxt.async as ccxt
from random import randint
from typing import List, Dict, Any, Optional
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf
import util
import db
logger = util.get_log(__name__)



class exchange(object):
    def __init__(self, ex_id, userid = 1):
        self.ex_id = ex_id
        self.userid = userid
        if ex_id in ccxt.exchanges:
            self.ex = getattr(ccxt, ex_id)({
                'enableRateLimit': True,
                #'verbose': True,
                #'session': cfscrape.create_scraper(),
            })
        else:
            raise Exception(self.to_string() + "ex_id error")
        if conf.dev_or_product == 1:
            self.ex.aiohttp_proxy = "http://127.0.0.1:1080"
        t_user_exchange_info = db.t_user_exchange.query.filter(
            sql.and_(
                db.t_user_exchange.f_userid == self.userid,
                db.t_user_exchange.f_ex_id == self.ex_id
            )
        ).first()
        if (t_user_exchange_info is not None):
            if conf.dev_or_product == 1:
                self.ex.aiohttp_proxy = t_user_exchange_info.f_aiohttp_proxy
                logger.debug(self.to_string() + "aiohttp_proxy={0}".format(self.ex.aiohttp_proxy))

        '''
        self.ex.markets['ETH/BTC']['limits']['amount']['min']   # 最小交易量 0.000001
        self.ex.markets['ETH/BTC']['limits']['price']['min']    # 最小价格 0.000001
        self.ex.markets['ETH/BTC']['precision']['amount']   # 精度 8
        self.ex.markets['ETH/BTC']['precision']['price']    # 精度 2
        '''

        # 手续费 百分比
        self.fee_taker = 0.001


        '''
        self.balance['BTC']['free']     # 还有多少钱
        self.balance['BTC']['used']
        self.balance['BTC']['total']
        '''
        self.balance = None
        
        '''
        {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': iso8601,
            'high': self.safe_float(ticker, 'highPrice'),
            'low': self.safe_float(ticker, 'lowPrice'),
            'bid': self.safe_float(ticker, 'bidPrice'),
            'bidVolume': self.safe_float(ticker, 'bidQty'),
            'ask': self.safe_float(ticker, 'askPrice'),
            'askVolume': self.safe_float(ticker, 'askQty'),
            'vwap': self.safe_float(ticker, 'weightedAvgPrice'),
            'open': self.safe_float(ticker, 'openPrice'),
            'close': self.safe_float(ticker, 'prevClosePrice'),
            'first': None,
            'last': self.safe_float(ticker, 'lastPrice'),
            'change': self.safe_float(ticker, 'priceChange'),
            'percentage': self.safe_float(ticker, 'priceChangePercent'),
            'average': None,
            'baseVolume': self.safe_float(ticker, 'volume'),
            'quoteVolume': self.safe_float(ticker, 'quoteVolume'),
            'info': ticker,
        }
        '''
        self.ticker = None
        self.ticker_time = 0

        '''
        self.order_book[symbol]['bids'][0][0]    # buy_1_price
        self.order_book[symbol]['bids'][0][1]    # buy_1_quantity
        self.order_book[symbol]['asks'][0][0]    # sell_1_price
        self.order_book[symbol]['asks'][0][1]    # sell_1_quantity
        '''
        self.order_book = dict()
        self.order_book_time = 0
        self.buy_1_price = 0.0
        self.buy_1_quantity = 0.0
        self.sell_1_price = 0.0
        self.sell_1_quantity = 0.0
        self.slippage_value = 0.0
        self.slippage_ratio  = 0.0

        self.symbol_cur = ''
        self.base_cur = ''
        self.quote_cur = ''


    '''
    async def __del__(self):
        if not self.ex is None:
            await self.ex.close()
    '''

    def to_string(self):
        return "exchange[{0},{1}] ".format(self.ex_id, self.userid)
    
    async def close(self):
        if not self.ex is None:
            await self.ex.close()

    def set_symbol(self, symbol):
        self.symbol_cur = symbol         # BTC/USD
        self.base_cur = symbol.split('/')[0]       # BTC
        self.quote_cur = symbol.split('/')[1]       # USD

    '''
    self.ex.markets['ETH/BTC']['limits']['amount']['min']   # 最小交易量 0.000001
    self.ex.markets['ETH/BTC']['limits']['price']['min']    # 最小价格 0.000001
    self.ex.markets['ETH/BTC']['precision']['amount']   # 精度 8
    self.ex.markets['ETH/BTC']['precision']['price']    # 精度 2
    '''
    async def load_markets(self):
        #logger.debug(self.to_string() + "load_markets() start")
        if self.ex.markets is None:
            await self.ex.load_markets()
            #logger.debug(self.to_string() + "load_markets() markets={0}".format(self.ex.markets))
            #logger.debug(self.to_string() + "load_markets() symbols={0}".format(self.ex.symbols))
            #logger.debug(self.to_string() + "load_markets() fees={0}".format(self.ex.fees))
            #logger.debug(self.to_string() + "load_markets() api={0}".format(self.ex.api))
            #logger.debug(self.to_string() + "load_markets() has={0}".format(self.ex.has))
            #logger.debug(self.to_string() + "load_markets() urls={0}".format(self.ex.urls))
            #logger.debug(self.to_string() + "load_markets() currencies={0}".format(self.ex.currencies))
        #logger.debug(self.to_string() + "load_markets() end ")
        return self.ex.markets

    def check_symbol(self, symbol):
        if symbol not in self.ex.symbols:
            raise Exception(self.to_string() + "check_symbol({0}) error".format(symbol))

    '''
    self.balance['BTC']['free']     # 还有多少钱
    self.balance['BTC']['used']
    self.balance['BTC']['total']
    '''
    async def fetch_balances(self):
        #logger.debug(self.to_string() + "fetch_balances() start")
        p = {}
        if self.ex.id == 'binance':
            p = {
                'recvWindow' : 60000,
            }
        self.balance = await self.ex.fetch_balances(p)
        #logger.debug(self.to_string() + "fetch_balances() end balance={0}".format(self.balance))
        #logger.debug(self.to_string() + "fetch_balances() end")
        return self.balance

    '''
    {
        'symbol': symbol,
        'timestamp': timestamp,
        'datetime': iso8601,
        'high': self.safe_float(ticker, 'highPrice'),
        'low': self.safe_float(ticker, 'lowPrice'),
        'bid': self.safe_float(ticker, 'bidPrice'),
        'bidVolume': self.safe_float(ticker, 'bidQty'),
        'ask': self.safe_float(ticker, 'askPrice'),
        'askVolume': self.safe_float(ticker, 'askQty'),
        'vwap': self.safe_float(ticker, 'weightedAvgPrice'),
        'open': self.safe_float(ticker, 'openPrice'),
        'close': self.safe_float(ticker, 'prevClosePrice'),
        'first': None,
        'last': self.safe_float(ticker, 'lastPrice'),
        'change': self.safe_float(ticker, 'priceChange'),
        'percentage': self.safe_float(ticker, 'priceChangePercent'),
        'average': None,
        'baseVolume': self.safe_float(ticker, 'volume'),
        'quoteVolume': self.safe_float(ticker, 'quoteVolume'),
        'info': ticker,
    }
    '''
    async def fetch_ticker(self, symbol):
        #logger.debug(self.to_string() + "fetch_ticker({0}) start".format(symbol))
        self.set_symbol(symbol)
        self.ticker = await self.ex.fetch_ticker(symbol)
        self.ticker_time = int(time.time())
        #logger.debug(self.to_string() + "fetch_ticker({0}) end ticker={1}".format(symbol, self.ticker))
        return self.ticker

    '''
    self.order_book[symbol]['bids'][0][0]    # buy_1_price
    self.order_book[symbol]['bids'][0][1]    # buy_1_quantity
    self.order_book[symbol]['asks'][0][0]    # sell_1_price
    self.order_book[symbol]['asks'][0][1]    # sell_1_quantity
    '''
    async def fetch_order_book(self, symbol, i = 5):
        #logger.debug(self.to_string() + "fetch_order_book({0}) start".format(symbol))
        if symbol == '':
            return
        self.order_book[symbol] = await self.ex.fetch_order_book(symbol, i)
        self.order_book_time = int(time.time())
        self.buy_1_price = self.order_book[symbol]['bids'][0][0]
        self.buy_1_quantity = self.order_book[symbol]['bids'][0][1]
        self.sell_1_price = self.order_book[symbol]['asks'][0][0]
        self.sell_1_quantity = self.order_book[symbol]['asks'][0][1]
        self.slippage_value = self.sell_1_price - self.buy_1_price
        self.slippage_ratio = (self.sell_1_price - self.buy_1_price) / self.buy_1_price
        self.set_symbol(symbol)
        #logger.debug(self.to_string() + "fetch_order_book({0}) end order_book[{1}]={2}".format(symbol, symbol, self.order_book[symbol]))
        return self.order_book

    async def fetch_ohlcv(self, symbol, timeframe, since_ms = None):
        #logger.debug(self.to_string() + "fetch_ohlcv({0}, {1}, {2}) start".format(symbol, period, since_ms))
        if timeframe not in self.ex.timeframes:
            return []

        # last item should be in the time interval [now - tick_interval, now]
        till_time_ms = arrow.utcnow().shift(minutes=-1).timestamp * 1000
        # it looks as if some exchanges return cached data
        # and they update it one in several minute, so 10 mins interval
        # is necessary to skeep downloading of an empty array when all
        # chached data was already downloaded
        till_time_ms = min(till_time_ms, arrow.utcnow().shift(minutes=-10).timestamp * 1000)

        data = []
        while not since_ms or since_ms < till_time_ms:
            data_part = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms)

            # Because some exchange sort Tickers ASC and other DESC.
            # Ex: Bittrex returns a list of tickers ASC (oldest first, newest last)
            # when GDAX returns a list of tickers DESC (newest first, oldest last)
            data_part = sorted(data_part, key=lambda x: x[0])

            if not data_part:
                break

            data.extend(data_part)
            since_ms = data[-1][0] + 1

        logger.debug(self.to_string() + "fetch_ohlcv({0},{1},{2}) end  len(data)={3}".format(symbol, period, since_ms, len(data)))
        return data
        
        


    # 异常处理
    async def run(self, func, *args, **kwargs):
        logger.info(self.to_string() + "run() start")
        err_timeout = 0
        err_ddos = 0
        err_auth = 0
        err_not_available = 0
        err_exchange = 0
        err_network = 0
        err = 0
        while True:
            try:
                logger.info(self.to_string() + "run() func")
                await func(*args, **kwargs)
                err_timeout = 0
                err_ddos = 0
                err_auth = 0
                err_not_available = 0
                err_exchange = 0
                err_network = 0
                err = 0
            except ccxt.RequestTimeout:
                err_timeout = err_timeout + 1
                logger.info(traceback.format_exc())
                time.sleep(30)
            except ccxt.DDoSProtection:
                err_ddos = err_ddos + 1
                logger.error(traceback.format_exc())
                time.sleep(15)
            except ccxt.AuthenticationError:
                err_auth = err_auth + 1
                logger.error(traceback.format_exc())
                time.sleep(5)
                if err_auth > 5:
                    break
            except ccxt.ExchangeNotAvailable:
                err_not_available = err_not_available + 1
                logger.error(traceback.format_exc())
                time.sleep(30)
                if err_not_available > 5:
                    break
            except ccxt.ExchangeError:
                err_exchange = err_exchange + 1
                logger.error(traceback.format_exc())
                time.sleep(5)
                if err_exchange > 5:
                    break
            except ccxt.NetworkError:
                err_network = err_network + 1
                logger.error(traceback.format_exc())
                time.sleep(5)
                if err_network > 5:
                    break
            except Exception:
                err = err + 1
                logger.info(traceback.format_exc())
                break
            except:
                logger.error(traceback.format_exc())
                break
        if not self.ex is None:
            await self.ex.close()
        logger.info(self.to_string() + "run() end")

