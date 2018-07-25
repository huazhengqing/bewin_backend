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
from db.datahub import g_datahub
from exchange.exchange import exchange
logger = util.get_log(__name__)



class fetch_base():

    __ex_symbol_fee = util.nesteddict()
    __symbol_ex_ticker = util.nesteddict()
    __queue_task_spread = asyncio.Queue()

    def __init__(self):
        #super(fetch_base, self).__init__()
        self.exchanges = dict()
        #self.executor_max_works = 5
        #self.executor = ThreadPoolExecutor(self.executor_max_works)
        
    def to_string(self):
        return "fetch_base[] "

    def init_exchange(self, id):
        if id in ccxt.exchanges:
            if not self.exchanges.get(id):
                self.exchanges[id] = exchange(id)
                if not self.exchanges[id]:
                    logger.error(self.to_string() + "init_exchange({0}) Error".format(id))
                    raise Exception("fetch_base::init_exchange()")
                t_markets = db.Session().query(db.t_markets).filter(db.t_markets.f_ex_id == id).all()
                for t_market in t_markets:
                    fetch_base.__ex_symbol_fee[t_market.f_ex_id][t_market.f_symbol] = t_market.f_fee_taker

    def init_exchanges(self):
        for id in ccxt.exchanges:
            self.init_exchange(id)

    async def close(self):
        for ex in self.exchanges.values():
            await ex.close()

    async def fetch_markets_to_db(self, ids = []):
        if len(ids) <= 0:
            ids = ccxt.exchanges
        for id in ids:
            logger.debug(self.to_string() + "fetch_markets_to_db({0})".format(id))
            ex = exchange(id)
            try:
                logger.debug(self.to_string() + "fetch_markets_to_db({0}) load_markets()".format(id))
                await ex.load_markets()
            except Exception:
                logger.warn(traceback.format_exc())
                logger.warn(self.to_string() + "fetch_exchanges({0}) Exception ".format(id))
                continue
            t_exchanges = db.t_exchanges(
                f_ex_id = id,
                f_ex_name = ex.ex.name,
                #f_countries = json.dumps(ex.ex.countries),
                #f_url_www = ex.ex.urls['www'] if ex.ex.urls.get('www') else '',
                #f_url_logo = ex.ex.urls['logo'] if ex.ex.urls.get('logo') else '',
                #f_url_referral = ex.ex.urls['referral'] if ex.ex.urls.get('referral') else '',
                #f_url_api = ex.ex.urls['api'] if ex.ex.urls.get('api') else '',
                #f_url_doc = ex.ex.urls['doc'] if ex.ex.urls.get('doc') else '',
                #f_url_fees = json.dumps(ex.ex.urls['fees']) if ex.ex.urls.get('fees') else '',
                f_timeframes = json.dumps(ex.ex.timeframes) if ex.has_api('fetchOHLCV')  else ''
            )
            logger.debug(self.to_string() + "fetch_markets_to_db({0}) db.t_exchanges()".format(id))
            db.Session().merge(t_exchanges)

            for symbol, v in ex.ex.markets.items():
                _active = 0
                if v.get('active') is not None:
                    if v['active']:
                        _active = 1
                    else:
                        _active = 0
                t_markets = db.t_markets(
                    f_ex_id = id,
                    f_symbol = symbol,
                    f_base = v['base'],
                    f_quote = v['quote'],
                    f_active = _active,
                    f_url = "",
                    f_fee_maker = v['maker'] if v.get('maker') else 0,
                    f_fee_taker = v['taker'] if v.get('taker') else 0,
                    f_precision_amount = v['precision']['amount'] if v['precision'].get('amount') else 0,
                    f_precision_price = v['precision']['price'] if v['precision'].get('price') else 0,
                    f_limits_amount_min = v['limits']['amount']['min'] if v['limits'].get('amount') else 0,
                    f_limits_price_min = v['limits']['price']['min'] if v['limits'].get('price') else 0,
                    #f_ts_create = v['info']['Created'] if (v.get('info') and v['info'].get('Created')) else 0,
                )
                logger.debug(self.to_string() + "fetch_markets_to_db({0}) db.t_markets({1})".format(id, symbol))
                db.Session().merge(t_markets)
            try:
                await ex.close()
            except Exception:
                pass

    async def run_fetch_markets_to_db(self, ids = []):
        while True:
            await self.fetch_markets_to_db(ids)
            await asyncio.sleep(12*60*60)
            





    '''
    ['f_ex_id', 'f_symbol', 'f_ts', 'f_bid', 'f_bid_volume', 'f_ask', 'f_ask_volume', 'f_vwap', 'f_open', 'f_high', 'f_low', 'f_close', 'f_last', 'f_previous_close', 'f_change', 'f_percentage', 'f_average', 'f_base_volume', 'f_quote_volume', 'f_ts_update']
    '''
    async def fetch_tickers(self, ex_id, topic, shards):
        # 降低 CPU ，暂时性
        await asyncio.sleep(10)

        self.init_exchange(ex_id)
        #logger.debug(self.to_string() + "fetch_tickers({0})".format(ex_id))
        records = []
        if not self.exchanges[ex_id].has_api('fetchTickers'):
            for symbol in fetch_base.__ex_symbol_fee[ex_id].keys():
                try:
                    rs = await self.fetch_ticker(ex_id, topic, shards, symbol)
                    records.extend(rs)
                except ccxt.RequestTimeout:
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(10)
                except ccxt.DDoSProtection:
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(10)
                except Exception:
                    logger.error(traceback.format_exc())
                    logger.error(self.to_string() + "fetch_tickers() fetch_ticker({0},{1})".format(ex_id, symbol))
                    await asyncio.sleep(10)
                except:
                    logger.error(traceback.format_exc())
                    logger.error(self.to_string() + "fetch_tickers() fetch_ticker({0},{1})".format(ex_id, symbol))
                    await asyncio.sleep(10)
            logger.debug(self.to_string() + "fetch_tickers({0}) len(records)={1}".format(ex_id, len(records)))
            return records
        tickers = await self.exchanges[ex_id].ex.fetch_tickers()
        i = 0
        for symbol, ticker in tickers.items():
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
            v = [ex_id, symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume, f_ts_update]
            record = TupleRecord(schema=topic.record_schema)
            record.values = v
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
            fetch_base.__symbol_ex_ticker[symbol][ex_id] = {
                "f_ts": f_ts,
                "f_bid": f_bid,
                "f_ask": f_ask,
            }
            await fetch_base.__queue_task_spread.put(v)
        logger.debug(self.to_string() + "fetch_tickers({0}) len(records)={1}".format(ex_id, len(records)))
        return records

    async def fetch_ticker(self, ex_id, topic, shards, symbol):
        self.init_exchange(ex_id)
        #logger.debug(self.to_string() + "fetch_ticker({0})".format(ex_id))
        records = []
        ticker = await self.exchanges[ex_id].ex.fetch_ticker(symbol)
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
        v = [ex_id, symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume, f_ts_update]
        record = TupleRecord(schema=topic.record_schema)
        record.values = v
        i = random.randint(1,100) % len(shards)
        record.shard_id = shards[i].shard_id
        fetch_base.__symbol_ex_ticker[symbol][ex_id] = {
            "f_ts": f_ts,
            "f_bid": f_bid,
            "f_ask": f_ask,
        }
        await fetch_base.__queue_task_spread.put(v)
        records.append(record)
        return records

    '''
    ['f_symbol', 'f_ex1', 'f_ex1_name', 'f_ex1_bid', 'f_ex1_ts', 'f_ex1_fee', 'f_ex2', 'f_ex2_name', 'f_ex2_ask', 'f_ex2_ts', 'f_ex2_fee', 'f_ts', 'f_spread', 'f_fee', 'f_profit', 'f_profit_p', 'f_ts_update']
    '''
    async def run_calc_spread(self, topic_name="t_spread"):
        logger.debug(self.to_string() + "run_calc_spread()")
        topic, shards = g_datahub.get_topic(topic_name)
        shard_count = len(shards)
        while True:
            try:
                # 数据太多，处理不完
                qsize = fetch_base.__queue_task_spread.qsize()
                if qsize >= 100:
                    logger.warn(self.to_string() + "run_calc_spread() qsize={0}".format(qsize))
                    '''
                    for i in range(10000):
                        fetch_base.__queue_task_spread.get()
                        fetch_base.__queue_task_spread.task_done()
                    continue
                    '''
                # [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume]
                task_record = await fetch_base.__queue_task_spread.get()
                symbol = task_record[1]
                ex1 = task_record[0]
                ex1_name = self.exchanges[ex1].ex.name
                ex1_bid = task_record[3]
                ex1_ask = task_record[5]
                ex1_ts = task_record[2]
                ex1_fee = fetch_base.__ex_symbol_fee[ex1][symbol] if fetch_base.__ex_symbol_fee[ex1][symbol] else 0
                record2s = fetch_base.__symbol_ex_ticker[symbol] if fetch_base.__symbol_ex_ticker[symbol] else {}
                records = []
                for ex2, v in record2s.items():
                    if ex2 == ex1:
                        continue
                    ex2_name = self.exchanges[ex2].ex.name
                    ex2_bid = v["f_bid"]
                    ex2_ask = v["f_ask"]
                    ex2_ts = v["f_ts"]
                    ex2_fee = fetch_base.__ex_symbol_fee[ex2][symbol] if fetch_base.__ex_symbol_fee[ex2][symbol] else 0
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
                g_datahub.pub_topic(topic_name, records)
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
    async def run_fetch_ohlcv(self, ex_id, topic_name, symbols, timeframe_str, since_ms, split_i, max_split_count):
        self.init_exchange(ex_id)
        if not self.exchanges[ex_id].has_api('fetchOHLCV'):
            logger.warn(self.to_string() + "run_fetch_ohlcv({0}) NOT has interface".format(ex_id))
            return
        logger.debug(self.to_string() + "run_fetch_ohlcv({0})".format(ex_id))
        '''
        if not self.exchanges[ex_id].ex.timeframes or timeframe_str not in self.exchanges[ex_id].ex.timeframes:
            logger.info(self.to_string() + "run_fetch_ohlcv({0}) NOT has timeframe={1}".format(ex_id, timeframe_str))
            return
        '''
        if not symbols or len(symbols) <= 0:
            symbols = [k for k in fetch_base.__ex_symbol_fee[ex_id].keys()]
        logger.debug(self.to_string() + "run_fetch_ohlcv({0},{1},{2}) len(symbols)={3}".format(ex_id, topic_name, timeframe_str, len(symbols)))
        
        symbols_todu = []
        s = 0
        for symbol in symbols:
            if s % max_split_count == split_i:
                symbols_todu.append(symbol)
            s = s + 1
        logger.debug(self.to_string() + "run_fetch_ohlcv({0},{1},{2}) len(symbols_todu)={3}".format(ex_id, topic_name, timeframe_str, len(symbols_todu)))
        if len(symbols_todu) <= 0:
            return

        topic, shards = g_datahub.get_topic(topic_name)
        f_timeframe = util.TimeFrame_Minutes[timeframe_str]
        while True:
            ts_start = arrow.utcnow().shift(minutes=-f_timeframe).timestamp * 1000
            i = 0
            for symbol in symbols_todu:
                try:
                    data = await self.exchanges[ex_id].fetch_ohlcv(symbol, timeframe_str, since_ms)
                except:
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(15)
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
                    record.values = [ex_id, symbol, f_timeframe, f_ts, f_o, f_h, f_l, f_c, f_v, f_ts_update]
                    record.shard_id = shards[i % len(shards)].shard_id
                    records.append(record)
                    i = i + 1
                #logger.debug(self.to_string() + "run_fetch_ohlcv({0},{1},{2},{3})len(records) = {4}".format(ex_id, topic_name, symbol, timeframe_str, len(records)))
                g_datahub.pub_topic(topic_name, records)
                await asyncio.sleep(3)
            since_ms = ts_start
            await asyncio.sleep(1)



