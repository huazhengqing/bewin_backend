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
import sqlalchemy as sql
import ccxt.async_support as ccxt
from random import randint
from typing import List, Dict, Any, Optional
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf
import util
from util import retry
import db
logger = util.get_log(__name__)



class exchange(object):
    def __init__(self, ex_id, userid = 0):
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

        #if conf.dev_or_product == 1:
        #    self.ex.aiohttp_proxy = "http://127.0.0.1:1080"
        
        '''
        self.ex.markets['ETH/BTC']['limits']['amount']['min']   # 最小交易量 0.000001
        self.ex.markets['ETH/BTC']['limits']['price']['min']    # 最小价格 0.000001
        self.ex.markets['ETH/BTC']['precision']['amount']   # 精度 8
        self.ex.markets['ETH/BTC']['precision']['price']    # 精度 2
        '''

        # 手续费 百分比
        self.fee_taker = 0.001

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

    def to_string(self):
        return "exchange[{0},{1}] ".format(self.ex_id, self.userid)
    
    async def close(self):
        if not self.ex is None:
            await self.ex.close()

    def set_symbol(self, symbol):
        self.symbol_cur = symbol
        self.base_cur, self.quote_cur = symbol.split('/')
        

    def get_symbol_detail_url(self, symbol: str) -> str:
        try:
            url_base = self.ex.urls.get('www')
            base, quote = symbol.split('/')
            return url_base + util._EXCHANGE_URLS[self.ex.id].format(base=base, quote=quote)
        except:
            logger.warning(self.to_string() + 'get_symbol_detail_url({0}) no url'.format(symbol))
            return ""
        return ""


    def has_api(self, api_name: str) -> bool:
        return api_name in self.ex.has and self.ex.has[api_name]

    def is_support_timeframes(self, timeframe_str: str) -> None:
        return timeframe_str in self.ex.timeframes
        


    '''
    self.ex.markets['ETH/BTC']['limits']['amount']['min']   # 最小交易量 0.000001
    self.ex.markets['ETH/BTC']['limits']['price']['min']    # 最小价格 0.000001
    self.ex.markets['ETH/BTC']['precision']['amount']   # 精度 8
    self.ex.markets['ETH/BTC']['precision']['price']    # 精度 2
    '''
    async def load_markets(self):
        #logger.debug(self.to_string() + "load_markets() start")
        if self.ex.markets is None or len(self.ex.markets) <= 0:
            await self.ex.load_markets()
            #await self.ex.fetch_markets()
            #logger.debug(self.to_string() + "load_markets() markets={0}".format(self.ex.markets))
            #logger.debug(self.to_string() + "load_markets() symbols={0}".format(self.ex.symbols))
            #logger.debug(self.to_string() + "load_markets() fees={0}".format(self.ex.fees))
            #logger.debug(self.to_string() + "load_markets() api={0}".format(self.ex.api))
            #logger.debug(self.to_string() + "load_markets() has={0}".format(self.ex.has))
            #logger.debug(self.to_string() + "load_markets() urls={0}".format(self.ex.urls))
            #logger.debug(self.to_string() + "load_markets() currencies={0}".format(self.ex.currencies))
            self.fee_taker = self.ex.fees['trading']['taker'] if self.ex.fees['trading'].get('taker') is not None else 0
            logger.debug(self.to_string() + "load_markets() fee_taker={0}".format(self.fee_taker))
        #logger.debug(self.to_string() + "load_markets() end ")
        return self.ex.markets

    def check_symbol(self, symbol):
        if symbol not in self.ex.symbols:
            raise Exception(self.to_string() + "check_symbol({0}) error".format(symbol))

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
        self.ticker_time = arrow.utcnow().timestamp * 1000
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
        self.order_book_time = arrow.utcnow().timestamp * 1000
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
        data = []
        while True:
            data_part = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms)
            if not data_part:
                break
            data_part = sorted(data_part, key=lambda x: x[0])
            data.extend(data_part)
            if since_ms is None:
                break
            since_ms = data[-1][0] + 1
            if since_ms >= arrow.utcnow().shift(minutes=-util.TimeFrame_Minutes[timeframe]).timestamp * 1000:
                break
        logger.debug(self.to_string() + "fetch_ohlcv({0},{1},{2}) end  len(data)={3}".format(symbol, timeframe, since_ms, len(data)))
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
                await asyncio.sleep(30)
            except ccxt.DDoSProtection:
                err_ddos = err_ddos + 1
                logger.error(traceback.format_exc())
                await asyncio.sleep(15)
            except ccxt.AuthenticationError:
                err_auth = err_auth + 1
                logger.error(traceback.format_exc())
                await asyncio.sleep(5)
                if err_auth > 5:
                    break
            except ccxt.ExchangeNotAvailable:
                err_not_available = err_not_available + 1
                logger.error(traceback.format_exc())
                await asyncio.sleep(30)
                if err_not_available > 5:
                    break
            except ccxt.ExchangeError:
                err_exchange = err_exchange + 1
                logger.error(traceback.format_exc())
                await asyncio.sleep(5)
                if err_exchange > 5:
                    break
            except ccxt.NetworkError:
                err_network = err_network + 1
                logger.error(traceback.format_exc())
                await asyncio.sleep(5)
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

