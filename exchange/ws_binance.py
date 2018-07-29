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
import ccxt.async_support as ccxt
from typing import List, Dict, Any, Optional
from binance.client import Client, BinanceAPIException
from binance.websockets import BinanceSocketManager
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import db
from db.datahub import g_datahub
from .ex_util import *
import util
logger = util.get_log(__name__)



KLINE_INTERVAL_1MINUTE = '1m'
KLINE_INTERVAL_3MINUTE = '3m'
KLINE_INTERVAL_5MINUTE = '5m'
KLINE_INTERVAL_15MINUTE = '15m'
KLINE_INTERVAL_30MINUTE = '30m'
KLINE_INTERVAL_1HOUR = '1h'
KLINE_INTERVAL_2HOUR = '2h'
KLINE_INTERVAL_4HOUR = '4h'
KLINE_INTERVAL_6HOUR = '6h'
KLINE_INTERVAL_8HOUR = '8h'
KLINE_INTERVAL_12HOUR = '12h'
KLINE_INTERVAL_1DAY = '1d'
KLINE_INTERVAL_3DAY = '3d'
KLINE_INTERVAL_1WEEK = '1w'
KLINE_INTERVAL_1MONTH = '1M'


class ws_binance():
    ex_id = 'binance'
    __TICKER_KEY = "@ticker"
    __KLINE_KEY = "@kline"
    __MULTIPLEX_SOCKET_NAME = "multiplex"
    __USER_SOCKET_NAME = "user"
    __STATUSES = {
        'NEW': 'open',
        'PARTIALLY_FILLED': 'open',
        'FILLED': 'closed',
        'CANCELED': 'canceled',
    }
    def __init__(self, key = "", secret = ""):
        self.client = Client(key, secret)
        self.bm = BinanceSocketManager(self.client)
        self.conn = dict()

        self.topic = dict()
        self.topic['t_ticker'] = g_datahub.get_topic('t_ticker')
        self.topic['t_ohlcv'] = g_datahub.get_topic('t_ohlcv')

        load_markets_db(self.ex_id)

        
    def to_string(self):
        return "ws_binance[] "

    '''
    def start_multiplex_socket(self, streams, callback):
    def start_kline_socket(self, symbol, callback, interval=Client.KLINE_INTERVAL_1MINUTE):
    '''
    def start(self, symbols = [], time_frames = []):
        if len(symbols) > 0 and len(time_frames) > 0:
            streams = ["{}{}_{}".format(merge_symbol(symbol).lower(), self.__KLINE_KEY, time_frame) for time_frame in time_frames for symbol in symbols]
            #for symbol in symbols:
            #    streams.append("{}{}".format(merge_symbol(symbol).lower(), self.__TICKER_KEY))
        if len(symbols) <= 0:
            streams = ["{}{}_{}".format(merge_symbol(symbol).lower(), self.__KLINE_KEY, time_frame) for time_frame in time_frames for symbol in g_ex_markets[self.ex_id].keys()]
        if len(symbols) > 0:
            logger.debug(self.to_string() + 'start() streams={0}'.format(streams))
            self.conn["start_multiplex_socket"] = self.bm.start_multiplex_socket(streams, self.callback_multiplex)
        self.conn["start_ticker_socket"] = self.bm.start_ticker_socket(self.fetch_ticker_2_datahub)
        self.bm.start()

    def close(self):
        self.bm.close()

    def fetch_ticker_2_datahub(self, msg):
        #logger.debug(self.to_string() + 'callback_ticker() msg={0}'.format(msg))
        topic, shards = self.topic['t_ticker']
        records = []
        for t in msg:
            ticker = self.ticker_2_ccxt(t)
            symbol = ticker['symbol']
            f_ts = ticker['timestamp'] and ticker['timestamp'] or int(arrow.utcnow().timestamp * 1000)
            f_bid = ticker['bid'] and ticker['bid'] or 0
            f_bid_volume = ticker['bidVolume'] and ticker['bidVolume'] or 0
            f_ask = ticker['ask'] and ticker['ask'] or 0
            f_ask_volume = ticker['askVolume'] and ticker['askVolume'] or 0
            f_vwap = ticker['vwap'] and ticker['vwap'] or 0
            f_open = ticker['open'] and ticker['open'] or 0
            f_high = ticker['high'] and ticker['high'] or 0
            f_low = ticker['low'] and ticker['low'] or 0
            f_close = ticker['close'] and ticker['close'] or 0
            f_last = ticker['last'] and ticker['last'] or 0
            f_previous_close = ticker['previousClose'] and ticker['previousClose'] or 0
            f_change = ticker['change'] and ticker['change'] or 0
            f_percentage = ticker['percentage'] and ticker['percentage'] or 0
            f_average = ticker['average'] and ticker['average'] or 0
            f_base_volume = ticker['baseVolume'] and ticker['baseVolume'] or 0
            f_quote_volume = ticker['quoteVolume'] and ticker['quoteVolume'] or 0
            f_ts_update = arrow.utcnow().timestamp
            v = [ws_binance.ex_id, symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume, f_ts_update]
            record = TupleRecord(schema=topic.record_schema)
            record.values = v
            i = random.randint(1,100) % len(shards)
            record.shard_id = shards[i].shard_id
            #logger.debug(self.to_string() + 'callback_ticker()  record={}'.format(record))
            records.append(record)

        logger.debug(self.to_string() + 'callback_ticker() len(records)={0}'.format(len(records)))
        try:
            g_datahub.pub_topic('t_ticker', records)
        except Exception as e:
            logger.info(self.to_string() + 'callback_ticker() e={0}'.format(e))
            

    # ORDER BOOK : Stream Name: <symbol>@depth<levels>
    # Recent trades : Stream Name: <symbol>@trade
    def callback_multiplex(self, msg):
        if msg['data']['e'] == 'error':
            logger.debug(self.to_string() + 'callback_multiplex() error={0}'.format(msg['m']))
            #self.bm.stop_socket(self.conn["start_multiplex_socket"])
            self.bm.start()
        else:
            msg_stream_type = msg["stream"]
            symbol = symbol_2_ccxt(msg["data"]["s"])
            logger.debug(self.to_string() + 'callback_multiplex() {},{}'.format(msg_stream_type, symbol))
            if self.__TICKER_KEY in msg_stream_type:
                ticker = self.ticker_2_ccxt(msg["data"])
                logger.debug(self.to_string() + 'callback_multiplex() {},{},ticker={}'.format(msg_stream_type, symbol, ticker))
            elif self.__KLINE_KEY in msg_stream_type:
                time_frame = msg["data"]["k"]["i"]
                logger.debug(self.to_string() + 'callback_multiplex() {},{},time_frame={}'.format(msg_stream_type, symbol, time_frame))
                ohlcv_list = self.ohlcv_2_ccxt(msg["data"]["k"])
                logger.debug(self.to_string() + 'callback_multiplex() {},{},ohlcv_list={}'.format(msg_stream_type, symbol, ohlcv_list))
                









    '''
    {
        "e": "kline",					# event type
        "E": 1499404907056,				# event time
        "s": "ETHBTC",					# symbol
        "k": {
            "t": 1499404860000, 		# start time of this bar
            "T": 1499404919999, 		# end time of this bar
            "s": "ETHBTC",				# symbol
            "i": "1m",					# interval
            "f": 77462,					# first trade id
            "L": 77465,					# last trade id
            "o": "0.10278577",			# open
            "c": "0.10278645",			# close
            "h": "0.10278712",			# high
            "l": "0.10278518",			# low
            "v": "17.47929838",			# volume
            "n": 4,						# number of trades
            "x": false,					# whether this bar is final
            "q": "1.79662878",			# quote volume
            "V": "2.34879839",			# volume of active buy
            "Q": "0.24142166",			# quote volume of active buy
            "B": "13279784.01349473"	# can be ignored
            }
    }
    '''
    @staticmethod
    def ohlcv_2_ccxt(kline_data):
        return [
            kline_data["t"],  # time
            safe_float(kline_data, "o"),  # open
            safe_float(kline_data, "h"),  # high
            safe_float(kline_data, "l"),  # low
            safe_float(kline_data, "c"),  # close
            safe_float(kline_data, "v"),  # vol
        ]

    '''
    {
        "e": "trade",     # Event type
        "E": 123456789,   # Event time
        "s": "BNBBTC",    # Symbol
        "t": 12345,       # Trade ID
        "p": "0.001",     # Price
        "q": "100",       # Quantity
        "b": 88,          # Buyer order Id
        "a": 50,          # Seller order Id
        "T": 123456785,   # Trade time
        "m": true,        # Is the buyer the market maker?
        "M": true         # Ignore.
    }
    '''
    @staticmethod
    def parse_order_status(status):
        return ws_binance.__STATUSES[status] if status in ws_binance.__STATUSES else status.lower()

    @staticmethod
    def order_2_ccxt(order):
        status = safe_value(order, 'X')
        if status is not None:
            status = ws_binance.parse_order_status(status)
        price = safe_float(order, "p")
        amount = safe_float(order, "q")
        filled = safe_float(order, "z", 0.0)
        cost = None
        remaining = None
        if filled is not None:
            if amount is not None:
                remaining = max(amount - filled, 0.0)
            if price is not None:
                cost = price * filled
        return {
            'info': order,
            'id': safe_string(order, "i"),
            'timestamp': order["T"],
            'datetime': safe_iso8601(order["T"]),
            'lastTradeTimestamp': None,
            'symbol': symbol_2_ccxt(safe_string(order, "s")),
            'type': safe_lower_string(order, "o"),
            'side': safe_lower_string(order, "S"),
            'price': price,
            'amount': amount,
            'cost': cost,
            'filled': filled,
            'remaining': remaining,
            'status': status,
            'fee': safe_float(order, "n", None),
        }

    '''
    [
        {
            'F': 278610,
            'o': '0.07393000',
            's': 'BCCBTC',
            'C': 1509622420916,
            'b': '0.07800800',
            'l': '0.07160300',
            'h': '0.08199900',
            'L': 287722,
            'P': '6.694',
            'Q': '0.10000000',
            'q': '1202.67106335',
            'p': '0.00494900',
            'O': 1509536020916,
            'a': '0.07887800',
            'n': 9113,
            'B': '1.00000000',
            'c': '0.07887900',
            'x': '0.07399600',
            'w': '0.07639068',
            'A': '2.41900000',
            'v': '15743.68900000'
        }
    ]
    '''
    @staticmethod
    def ticker_2_ccxt(ticker):
        timestamp = safe_int(ticker, 'C')
        iso8601 = None if (timestamp is None) else safe_iso8601(timestamp)
        symbol = symbol_2_ccxt(safe_string(ticker, 's'))
        last = safe_float(ticker, 'c')
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': iso8601,
            'high': safe_float(ticker, 'h'),
            'low': safe_float(ticker, 'l'),
            'bid': safe_float(ticker, 'b'),
            'bidVolume': safe_float(ticker, 'B'),
            'ask': safe_float(ticker, 'a'),
            'askVolume': safe_float(ticker, 'A'),
            'vwap': safe_float(ticker, 'w'),
            'open': safe_float(ticker, 'o'),
            'close': last,
            'last': last,
            'previousClose': safe_float(ticker, 'x'), 
            'change': safe_float(ticker, 'p'),
            'percentage': safe_float(ticker, 'P'),
            'average': None,
            'baseVolume': safe_float(ticker, 'v'),
            'quoteVolume': safe_float(ticker, 'q'),
            'info': ticker,
        }

