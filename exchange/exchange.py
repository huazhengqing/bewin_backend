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
from exchange.ex_util import *
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

        if conf.dev_local == 1:
            self.ex.aiohttp_proxy = "http://127.0.0.1:1080"
        
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

        self.symbol_tickers = dict()

        '''
        self.order_book[symbol]['bids'][0][0]    # buy_1_price
        self.order_book[symbol]['bids'][0][1]    # buy_1_quantity
        self.order_book[symbol]['asks'][0][0]    # sell_1_price
        self.order_book[symbol]['asks'][0][1]    # sell_1_quantity
        '''
        self.order_book = dict()
        self.order_book_time = 0
        self.order_book_time_pre = 0
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
        return "exchange[{0},{1}] ".format(self.userid, self.ex_id)
    
    async def close(self):
        if self.ex:
            try:
                await self.ex.close()
            except:
                pass

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
        if not self.ex.markets or len(self.ex.markets) <= 0:
            await self.ex.load_markets()
            #await self.ex.fetch_markets()
            #logger.debug(self.to_string() + "load_markets() markets={0}".format(self.ex.markets))
            #logger.debug(self.to_string() + "load_markets() symbols={0}".format(self.ex.symbols))
            #logger.debug(self.to_string() + "load_markets() fees={0}".format(self.ex.fees))
            #logger.debug(self.to_string() + "load_markets() api={0}".format(self.ex.api))
            #logger.debug(self.to_string() + "load_markets() has={0}".format(self.ex.has))
            #logger.debug(self.to_string() + "load_markets() urls={0}".format(self.ex.urls))
            #logger.debug(self.to_string() + "load_markets() currencies={0}".format(self.ex.currencies))
            #self.fee_taker = self.ex.fees['trading']['taker'] if self.ex.fees['trading'].get('taker') is not None else 0
            #logger.debug(self.to_string() + "load_markets() fee_taker={0}".format(self.fee_taker))
        #logger.debug(self.to_string() + "load_markets() end ")
        return self.ex.markets

    def check_symbol(self, symbol):
        if symbol not in self.ex.symbols:
            raise Exception(self.to_string() + "check_symbol({0}) error".format(symbol))





    async def fetch_ticker(self, symbol):
        self.set_symbol(symbol)
        self.ticker = await self.ex.fetch_ticker(symbol)
        self.ticker_time = arrow.utcnow().timestamp * 1000
        return self.ticker

    async def fetch_tickers(self):
        if self.has_api('fetchTickers'):
            tickers = await self.ex.fetch_tickers()
            for ticker in tickers:
                self.symbol_tickers[ticker['symbol']] = ticker
        else:
            for symbol in self.ex.symbols:
                self.symbol_tickers[symbol] = await self.ex.fetch_ticker(symbol)
        return self.symbol_tickers





    '''
    self.order_book[symbol]['bids'][0][0]    # buy_1_price
    self.order_book[symbol]['bids'][0][1]    # buy_1_quantity
    self.order_book[symbol]['asks'][0][0]    # sell_1_price
    self.order_book[symbol]['asks'][0][1]    # sell_1_quantity
    '''
    async def fetch_order_book(self, symbol, i = 5):
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
        #if timeframe not in self.ex.timeframes:
        #    return []
        data = []
        while True:
            data_part = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms)
            if not data_part:
                break
            data_part = sorted(data_part, key=lambda x: x[0])
            data.extend(data_part)
            if not since_ms:
                break
            since_ms = data[-1][0] + 1
            if since_ms >= arrow.utcnow().shift(minutes=-util.TimeFrame_Minutes[timeframe] * 2).timestamp * 1000:
                break
            logger.debug(self.to_string() + "fetch_ohlcv({0},{1},{2}) len(data)={3}".format(symbol, timeframe, since_ms, len(data)))
        #logger.debug(self.to_string() + "fetch_ohlcv({0},{1},{2}) end  len(data)={3}".format(symbol, timeframe, since_ms, len(data)))
        return data





    def get_limits_amount_min_price(self, symbol: str, price: float) -> Optional[float]:
        market = self.ex.markets[symbol]
        if 'limits' not in market:
            return None

        min_amounts = []
        limits = market['limits']
        if ('cost' in limits and 'min' in limits['cost'] and limits['cost']['min'] is not None):
            min_amounts.append(limits['cost']['min'])

        if ('amount' in limits and 'min' in limits['amount'] and limits['amount']['min'] is not None):
            min_amounts.append(limits['amount']['min'] * price)

        if not min_amounts:
            return None

        return max(min_amounts) * 2

    def calculate_fee(self, symbol='ETH/BTC', type='', side='', amount=1, price=1, taker_or_maker='maker') -> float:
        return self.ex.calculate_fee(
            symbol=symbol, 
            type=type, 
            side=side, 
            amount=amount, 
            price=price, 
            takerOrMaker=taker_or_maker
            )['rate']

    def amount_to_lots(self, symbol: str, amount: float) -> float:
        return self.ex.amount_to_lots(symbol, amount)






















    # 3角套利，查找可以套利的币
    async def triangle_find_best_profit(self, quote1 = "BTC", quote2 = "ETH"):
        btc_coin = dict()
        eth_coin = dict()
        await self.fetch_tickers()
        for symbol, ticker in self.symbol_tickers.items():
            coin, quote = split_symbol(symbol)
            if quote == quote1:
                btc_coin[coin] = ticker
            if quote == quote2:
                eth_coin[coin] = ticker

        ethprice = float(btc_coin[quote2]["bid"])

        #print("币种------ETH记价---ETH/BTC价---转成BTC价---直接BTC价--价差比")
        find_coin = ''
        find_profit = 0.0
        for k, v in btc_coin.items():
            if k in eth_coin:
                coin2btc = float(ethprice) * float(eth_coin[k]["bid"])
                btcbuy  = float(btc_coin[k]["bid"])
                profit = (btcbuy - coin2btc) / coin2btc
                if abs(profit) > 0.008:
                    #print("%s\t%10.8f  %10.8f  %10.8f  %10.8f  %s"%(k, float(eth_coin[k]["price"]), round(ethprice,8), round(coin2btc, 8), btcbuy, profit))
                    if abs(profit) > abs(find_profit):
                        find_coin = k
                        find_profit = profit

        if find_coin != '':
            coin_btc = find_coin + '/' + quote1
            coin_eth = find_coin + '/' + quote2
            fee = 0.003
            await self.fetch_order_book(coin_btc)
            fee += self.slippage_ratio
            #print(coin_btc, ': bids=', self.buy_1_price, '|asks=', self.sell_1_price, '|spread%=', round(self.slippage_ratio, 4))
            await self.fetch_order_book(coin_eth) 
            fee += self.slippage_ratio
            #print(coin_eth, ': bids=', self.buy_1_price, '|asks=', self.sell_1_price, '|spread%=', round(self.slippage_ratio, 4))
            if abs(find_profit) > fee:
                logger.info("%s \t %10.4f"%(find_coin, abs(find_profit) - fee))



    async def run(self, func, *args, **kwargs):
        while True:
            try:
                await func(*args, **kwargs)
            except ccxt.RequestTimeout:
                logger.info(self.to_string() + "run() ccxt.RequestTimeout ")
                await asyncio.sleep(3)
            except ccxt.DDoSProtection:
                logger.info(self.to_string() + "run() ccxt.DDoSProtection ")
                await asyncio.sleep(3)
            except ccxt.AuthenticationError:
                logger.info(self.to_string() + "run() ccxt.AuthenticationError ")
                await asyncio.sleep(3)
            except ccxt.ExchangeNotAvailable:
                logger.info(self.to_string() + "run() ccxt.ExchangeNotAvailable ")
                await asyncio.sleep(3)
            except ccxt.ExchangeError:
                logger.info(self.to_string() + "run() ccxt.ExchangeError ")
                await asyncio.sleep(3)
            except ccxt.NetworkError:
                logger.info(self.to_string() + "run() ccxt.NetworkError ")
                await asyncio.sleep(3)
            except Exception as e:
                logger.info(self.to_string() + "run() {}".format(e))
                await asyncio.sleep(3)
            except:
                logger.error(traceback.format_exc())




