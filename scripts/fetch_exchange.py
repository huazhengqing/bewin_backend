#!/usr/bin/python
import os
import sys
import time
#import queue
import json
import arrow
import random
import asyncio
import logging
import traceback
#import threading
#import multiprocessing
#import concurrent.futures
#from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import ccxt.async_support as ccxt
from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf
import util
import db
from db.datahub import datahub
from exchange.exchange import exchange
logger = util.get_log(__name__)



class fetch_exchange(datahub):
    def __init__(self):
        super(fetch_exchange, self).__init__()
        self.exchanges = dict()
        self.symbol_ex_ticker = dict()
        self.queue_task_spread = asyncio.Queue()
        #self.executor_max_works = 5
        #self.executor = ThreadPoolExecutor(self.executor_max_works)
        self.init_exchanges()
        db.init()

    def init_exchanges(self):
        for id in ccxt.exchanges:
            if self.exchanges.get(id) is None:
                self.exchanges[id] = exchange(id)
    
    async def close(self):
        for ex in self.exchanges.values():
            await ex.close()


    '''
    ['f_ex_id', 'f_ex_name', 'f_countries', 'f_url_www', 'f_url_logo', 'f_url_referral', 'f_url_api', 'f_url_doc', 'f_url_fees', 'f_timeframes', 'f_ts', 'f_ts_update']
    '''
    async def fetch_exchanges(self, ex_id, topic, shards):
        self.init_exchanges()
        records = []
        i = 0
        for id, ex in self.exchanges.items():
            logger.debug(self.to_string() + "fetch_exchanges({0})".format(id))
            try:
                await ex.load_markets()
            except Exception:
                logger.warn(traceback.format_exc())
                logger.warn(self.to_string() + "fetch_exchanges({0})".format(id))
                continue
            f_ex_id = id
            f_ex_name = self.exchanges[ex_id].ex.name
            f_countries = json.dumps(self.exchanges[ex_id].ex.countries)

            f_url_www = json.dumps(self.exchanges[ex_id].ex.urls['www'])
            f_url_logo = json.dumps(self.exchanges[ex_id].ex.urls['logo'])
            f_url_referral = json.dumps(self.exchanges[ex_id].ex.urls['referral'])
            f_url_api = json.dumps(self.exchanges[ex_id].ex.urls['api'])
            f_url_doc = json.dumps(self.exchanges[ex_id].ex.urls['doc'])
            f_url_fees = json.dumps(self.exchanges[ex_id].ex.urls['fees'])

            f_timeframes = ''
            try:
                if self.exchanges[ex_id].ex.timeframes is not None:
                    f_timeframes = json.dumps(self.exchanges[ex_id].ex.timeframes)
            except Exception:
                logger.info(traceback.format_exc())

            f_ts = arrow.utcnow().timestamp * 1000
            f_ts_update = arrow.utcnow().timestamp
            record = TupleRecord(schema=topic.record_schema)
            record.values = [f_ex_id, f_ex_name, f_countries, f_url_www, f_url_logo, f_url_referral, f_url_api, f_url_doc, f_url_fees, f_timeframes, f_ts, f_ts_update]
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
        logger.debug(self.to_string() + "fetch_exchanges({0}) records count={1}".format(id, len(records)))
        return records

    '''
    ['f_ex_id', 'f_symbol', 'f_base', 'f_quote', 'f_active', 'f_url', 'f_fee_maker', 'f_fee_taker', 'f_precision_amount', 'f_precision_price', 'f_limits_amount_min', 'f_limits_price_min', 'f_ts_create', 'f_ts', 'f_ts_update']
    '''
    async def fetch_markets(self, ex_id, topic, shards):
        self.init_exchanges()
        if self.exchanges.get(ex_id) is None:
            logger.warn(self.to_string() + "fetch_markets({0}) no ex".format(ex_id))
            return []
        if self.exchanges[ex_id].ex.has['fetchMarkets'] is False:
            logger.warn(self.to_string() + "fetch_markets({0}) NOT has interface".format(ex_id))
            return []
        logger.debug(self.to_string() + "fetch_markets({0})".format(ex_id))
        await self.exchanges[ex_id].load_markets()
        records = []
        i = 0
        f_ex_id = ex_id
        for symbol in self.exchanges[ex_id].ex.symbols:
            f_symbol = symbol
            f_base = self.exchanges[ex_id].ex.markets[symbol]['base']
            f_quote = self.exchanges[ex_id].ex.markets[symbol]['quote']
            f_active = self.exchanges[ex_id].ex.markets[symbol]['active'] and 1 or 0
            f_url = ""
            f_fee_maker = self.exchanges[ex_id].ex.markets[symbol]['maker']
            f_fee_taker = self.exchanges[ex_id].ex.markets[symbol]['taker']
            f_precision_amount = self.exchanges[ex_id].ex.markets[symbol]['precision']['amount']
            f_precision_price = self.exchanges[ex_id].ex.markets[symbol]['precision']['price']
            f_limits_amount_min = self.exchanges[ex_id].ex.markets[symbol]['limits']['amount']['min']
            f_limits_price_min = self.exchanges[ex_id].ex.markets[symbol]['limits']['price']['min']
            f_ts_create = self.exchanges[ex_id].ex.markets[symbol]['info']['Created']
            f_ts = arrow.utcnow().timestamp * 1000
            f_ts_update = arrow.utcnow().timestamp
            record = TupleRecord(schema=topic.record_schema)
            record.values = [f_ex_id, f_symbol, f_base, f_quote, f_active, f_url, f_fee_maker, f_fee_taker, f_precision_amount, f_precision_price, f_limits_amount_min, f_limits_price_min, f_ts_create, f_ts, f_ts_update]
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
        return records

    '''
    ['f_ex_id', 'f_symbol', 'f_ts', 'f_bid', 'f_bid_volume', 'f_ask', 'f_ask_volume', 'f_vwap', 'f_open', 'f_high', 'f_low', 'f_close', 'f_last', 'f_previous_close', 'f_change', 'f_percentage', 'f_average', 'f_base_volume', 'f_quote_volume', 'f_ts_update']
    '''
    async def fetch_tickers(self, ex_id, topic, shards):
        self.init_exchanges()
        if self.exchanges.get(ex_id) is None:
            logger.warn(self.to_string() + "fetch_tickers({0}) no ex".format(ex_id))
            return []
        logger.debug(self.to_string() + "fetch_tickers({0})".format(ex_id))
        records = []
        if self.exchanges[ex_id].ex.has['fetchTickers'] is False:
            await self.exchanges[ex_id].load_markets()
            for symbol in self.exchanges[ex_id].ex.symbols:
                try:
                    rs = await self.fetch_ticker(ex_id, topic, shards, symbol)
                    records.extend(rs)
                except ccxt.RequestTimeout:
                    #logger.info(traceback.format_exc())
                    await asyncio.sleep(10)
                except ccxt.DDoSProtection:
                    #logger.error(traceback.format_exc())
                    await asyncio.sleep(10)
                except Exception:
                    logger.error(self.to_string() + "fetch_tickers() fetch_ticker({0},{1})".format(ex_id, symbol))
                    logger.error(traceback.format_exc())
                    #await asyncio.sleep(10)
                except:
                    logger.error(self.to_string() + "fetch_tickers() fetch_ticker({0},{1})".format(ex_id, symbol))
                    logger.error(traceback.format_exc())
                    #await asyncio.sleep(10)
            return records
        tickers = await self.exchanges[ex_id].ex.fetch_tickers()
        i = 0
        f_ex_id = ex_id
        for symbol, ticker in tickers.items():
            f_symbol = symbol
            f_ts = ticker['timestamp'] is not None and ticker['timestamp'] or int(arrow.utcnow().timestamp * 1000)
            f_bid = ticker['bid'] is not None and ticker['bid'] or 0
            f_bid_volume = ticker['bidVolume'] is not None and ticker['bidVolume'] or 0
            f_ask = ticker['ask'] is not None and ticker['ask'] or 0
            f_ask_volume = ticker['askVolume'] is not None and ticker['askVolume'] or 0
            f_vwap = ticker['vwap'] is not None and ticker['vwap'] or 0
            f_open = ticker['open'] is not None and ticker['open'] or 0
            f_high = ticker['high'] is not None and ticker['high'] or 0
            f_low = ticker['low'] is not None and ticker['low'] or 0
            f_close = ticker['close'] is not None and ticker['close'] or 0
            f_last = ticker['last'] is not None and ticker['last'] or 0
            f_previous_close = ticker['previousClose'] is not None and ticker['previousClose'] or 0
            f_change = ticker['change'] is not None and ticker['change'] or 0
            f_percentage = ticker['percentage'] is not None and ticker['percentage'] or 0
            f_average = ticker['average'] is not None and ticker['average'] or 0
            f_base_volume = ticker['baseVolume'] is not None and ticker['baseVolume'] or 0
            f_quote_volume = ticker['quoteVolume'] is not None and ticker['quoteVolume'] or 0
            f_ts_update = arrow.utcnow().timestamp
            v = [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume, f_ts_update]
            record = TupleRecord(schema=topic.record_schema)
            record.values = v
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
            if self.symbol_ex_ticker.get(f_symbol) is None:
                self.symbol_ex_ticker[f_symbol] = {
                    f_ex_id:{
                        "f_ts": f_ts,
                        "f_bid": f_bid,
                        "f_ask": f_ask,
                    }
                }
            else:
                self.symbol_ex_ticker[f_symbol][f_ex_id] = {
                    "f_ts": f_ts,
                    "f_bid": f_bid,
                    "f_ask": f_ask,
                }
            await self.queue_task_spread.put(v)
        return records

    async def fetch_ticker(self, ex_id, topic, shards, symbol):
        self.init_exchanges()
        if self.exchanges.get(ex_id) is None:
            logger.warn(self.to_string() + "fetch_ticker({0}) no ex".format(ex_id))
            return []
        logger.debug(self.to_string() + "fetch_ticker({0})".format(ex_id))
        records = []
        ticker = await self.exchanges[ex_id].ex.fetch_ticker(symbol)
        f_ex_id = self.exchanges[ex_id].ex.id
        f_symbol = symbol
        f_ts = ticker['timestamp'] is not None and ticker['timestamp'] or int(arrow.utcnow().timestamp * 1000)
        f_bid = ticker['bid'] is not None and ticker['bid'] or 0
        f_bid_volume = ticker['bidVolume'] is not None and ticker['bidVolume'] or 0
        f_ask = ticker['ask'] is not None and ticker['ask'] or 0
        f_ask_volume = ticker['askVolume'] is not None and ticker['askVolume'] or 0
        f_vwap = ticker['vwap'] is not None and ticker['vwap'] or 0
        f_open = ticker['open'] is not None and ticker['open'] or 0
        f_high = ticker['high'] is not None and ticker['high'] or 0
        f_low = ticker['low'] is not None and ticker['low'] or 0
        f_close = ticker['close'] is not None and ticker['close'] or 0
        f_last = ticker['last'] is not None and ticker['last'] or 0
        f_previous_close = ticker['previousClose'] is not None and ticker['previousClose'] or 0
        f_change = ticker['change'] is not None and ticker['change'] or 0
        f_percentage = ticker['percentage'] is not None and ticker['percentage'] or 0
        f_average = ticker['average'] is not None and ticker['average'] or 0
        f_base_volume = ticker['baseVolume'] is not None and ticker['baseVolume'] or 0
        f_quote_volume = ticker['quoteVolume'] is not None and ticker['quoteVolume'] or 0
        f_ts_update = arrow.utcnow().timestamp
        v = [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume, f_ts_update]
        record = TupleRecord(schema=topic.record_schema)
        record.values = v
        i = random.randint(1,100) % len(shards)
        record.shard_id = shards[i].shard_id
        if self.symbol_ex_ticker.get(f_symbol) is None:
            self.symbol_ex_ticker[f_symbol] = {
                f_ex_id:{
                    "f_ts": f_ts,
                    "f_bid": f_bid,
                    "f_ask": f_ask,
                }
            }
        else:
            self.symbol_ex_ticker[f_symbol][f_ex_id] = {
                "f_ts": f_ts,
                "f_bid": f_bid,
                "f_ask": f_ask,
            }
        await self.queue_task_spread.put(v)
        records.append(record)
        return records

    '''
    ['f_symbol', 'f_ex1', 'f_ex1_name', 'f_ex1_bid', 'f_ex1_ts', 'f_ex1_fee', 'f_ex2', 'f_ex2_name', 'f_ex2_ask', 'f_ex2_ts', 'f_ex2_fee', 'f_ts', 'f_spread', 'f_fee', 'f_profit', 'f_profit_p', 'f_ts_update']
    '''
    async def run_calc_spread(self, topic_name="t_spread"):
        logger.debug(self.to_string() + "run_calc_spread()")
        topic, shards = self.get_topic(topic_name)
        shard_count = len(shards)
        while True:
            try:
                # 数据太多，处理不完
                qsize = self.queue_task_spread.qsize()
                if qsize >= 5000:
                    logger.warn(self.to_string() + "run_calc_spread() qsize={0}".format(qsize))
                    '''
                    for i in range(10000):
                        self.queue_task_spread.get()
                        self.queue_task_spread.task_done()
                    continue
                    '''
                # [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume]
                task_record = await self.queue_task_spread.get()
                symbol = task_record[1]
                ex1 = task_record[0]
                ex1_name = self.exchanges[ex1].ex.name
                ex1_bid = task_record[3]
                ex1_ask = task_record[5]
                ex1_ts = task_record[2]
                ex1_fee = self.exchanges[ex1].ex.markets[symbol]['taker'] if self.exchanges[ex1].ex.markets is not None and self.exchanges[ex1].ex.markets.get(symbol) is not None and self.exchanges[ex1].ex.markets[symbol].get('taker') is not None else 0.0
                record2s = self.symbol_ex_ticker[symbol] if self.symbol_ex_ticker.get(symbol) is not None else {}
                records = []
                for ex2, v in record2s.items():
                    if ex2 == ex1:
                        continue
                    ex2_name = self.exchanges[ex2].ex.name
                    ex2_bid = v["f_bid"]
                    ex2_ask = v["f_ask"]
                    ex2_ts = v["f_ts"]
                    ex2_fee = self.exchanges[ex2].ex.markets[symbol]['taker'] if self.exchanges[ex2].ex.markets is not None and self.exchanges[ex2].ex.markets.get(symbol) is not None and self.exchanges[ex2].ex.markets[symbol].get('taker') is not None else 0.0
                    if abs(ex1_ts - ex2_ts) > 30000:
                        logger.info(self.to_string() + "run_calc_spread() abs(ex1_ts - ex2_ts)={0}".format(abs(ex1_ts - ex2_ts)))
                        continue
                    spread_ts = ex1_ts if ex1_ts > ex2_ts else ex2_ts
                    f_fee = (ex1_bid * ex1_fee + ex2_ask * ex2_fee)
    
                    f_spread = ex1_bid-ex2_ask
                    f_profit=(f_spread - f_fee)
                    f_profit_p=(f_profit / ex1_bid) if ex1_bid > 0.0 else 0.0
                    f_ts_update = arrow.utcnow().timestamp
                    record1 = TupleRecord(schema=topic.record_schema)
                    record1.values = [symbol, ex1, ex1_name, ex1_bid, ex1_ts, ex1_fee, ex2, ex2_name, ex2_ask, ex2_ts, ex2_fee, spread_ts, f_spread, f_fee, f_profit, f_profit_p, f_ts_update]
                    i = random.randint(1,100) % shard_count
                    record1.shard_id = shards[i].shard_id
                    records.append(record1)

                    f_spread = ex2_bid-ex1_ask
                    f_profit=(f_spread - f_fee)
                    f_profit_p=(f_profit / ex2_bid) if ex2_bid > 0.0 else 0.0
                    record2 = TupleRecord(schema=topic.record_schema)
                    record2.values = [symbol, ex2, ex2_name, ex2_bid, ex2_ts, ex2_fee, ex1, ex1_name, ex1_ask, ex1_ts, ex1_fee, spread_ts, f_spread, f_fee, f_profit, f_profit_p, f_ts_update]
                    i = random.randint(1,100) % shard_count
                    record2.shard_id = shards[i].shard_id
                    records.append(record2)
                self.pub_topic(topic_name, records)
            except DatahubException as e:
                logger.error(traceback.format_exc(e))
            except Exception as e:
                logger.error(traceback.format_exc(e))
            except:
                logger.error(traceback.format_exc())

    '''
    def execute_calc_spread(self, topic_name="t_spread"):
        all_task = []
        for i in range(self.executor_max_works):
            future = self.executor.submit(self.run_calc_spread, topic_name)
            all_task.append(future)
        wait(all_task, return_when=ALL_COMPLETED)
    '''

    '''
    ['f_ex_id', 'f_symbol', 'f_timeframe', 'f_ts', 'f_o', 'f_h', 'f_l', 'f_c', 'f_v', 'f_ts_update']
    '''
    async def run_fetch_ohlcv(self, ex_id, topic_name, symbols, timeframe_str, since_ms):
        self.init_exchanges()
        if self.exchanges.get(ex_id) is None:
            logger.warn(self.to_string() + "run_fetch_ohlcv({0}) no ex".format(ex_id))
            return
        if self.exchanges[ex_id].ex.has['fetchOHLCV'] is False:
            logger.warn(self.to_string() + "run_fetch_ohlcv({0}) NOT has interface".format(ex_id))
            return
        logger.debug(self.to_string() + "run_fetch_ohlcv({0})".format(ex_id))
        try:
            await self.exchanges[ex_id].load_markets()
        except:
            logger.error(traceback.format_exc())
        if self.exchanges[ex_id].ex.timeframes is None or timeframe_str not in self.exchanges[ex_id].ex.timeframes:
            logger.info(self.to_string() + "run_fetch_ohlcv({0}) NOT has timeframe={1}".format(ex_id, timeframe_str))
            return
        if symbols is None or len(symbols) <= 0:
            symbols = self.exchanges[ex_id].ex.symbols
        topic, shards = self.get_topic(topic_name)
        f_ex_id = ex_id
        f_timeframe = util.TimeFrame_Minutes[timeframe_str]
        while True:
            ts_start = arrow.utcnow().shift(minutes=-10).timestamp * 1000
            i = 0
            for symbol in symbols:
                f_symbol = symbol
                try:
                    data = await self.exchanges[ex_id].fetch_ohlcv(symbol, timeframe_str, since_ms)
                except:
                    logger.error(traceback.format_exc())
                    continue
                f_ts_update = arrow.utcnow().timestamp
                #logger.debug(self.to_string() + "run_fetch_ohlcv() f_ts_update={0}".format(f_ts_update))
                records = []
                for d in data:
                    f_ts = d[0]
                    f_o = d[1]
                    f_h = d[2]
                    f_l = d[3]
                    f_c = d[4]
                    f_v = d[5]
                    record = TupleRecord(schema=topic.record_schema)
                    record.values = [f_ex_id, f_symbol, f_timeframe, f_ts, f_o, f_h, f_l, f_c, f_v, f_ts_update]
                    record.shard_id = shards[i % len(shards)].shard_id
                    records.append(record)
                    i = i + 1
                logger.debug(self.to_string() + "run_fetch_ohlcv({0},{1},{2},{3})len(records) = {4}".format(ex_id, topic_name, symbol, timeframe_str, len(records)))
                self.pub_topic(topic_name, records)
            since_ms = ts_start
                