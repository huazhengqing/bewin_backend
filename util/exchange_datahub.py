#!/usr/bin/python
import os
import sys
import time
#import queue
import random
import asyncio
import logging
import traceback
#import threading
#import multiprocessing
#import concurrent.futures
#from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import ccxt.async as ccxt
from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf.conf_ex
import util.util
from util.exchange_base import exchange_base
logger = util.util.get_log(__name__)



class exchange_datahub():
    def __init__(self):
        access_id = conf.conf_aliyun.conf_aliyun_datahub['dev_access_id']
        access_key = conf.conf_aliyun.conf_aliyun_datahub['dev_access_key']
        endpoint = conf.conf_aliyun.conf_aliyun_datahub['dev_endpoint']
        self.project_name = conf.conf_aliyun.conf_aliyun_datahub['dev_project']
        if conf.conf_aliyun.dev_or_product == 2:
            access_id = conf.conf_aliyun.conf_aliyun_datahub['product_access_id']
            access_key = conf.conf_aliyun.conf_aliyun_datahub['product_access_key']
            endpoint = conf.conf_aliyun.conf_aliyun_datahub['product_endpoint']
            self.project_name = conf.conf_aliyun.conf_aliyun_datahub['product_project']
        #self.datahub = DataHub(access_id, access_key, endpoint, enable_pb=True)
        self.datahub = DataHub(access_id, access_key, endpoint)
        self.topic = None
        self.shards = []
        self.cursor_type = CursorType.LATEST
        self.get_limit_num = 30
        self.exchanges = dict()
        self.symbol_ex_ticker = dict()
        self.queue_task_spread = asyncio.Queue()
        #self.executor_max_works = 5
        #self.executor = ThreadPoolExecutor(self.executor_max_works)

    async def close(self):
        for id, ex in self.exchanges.items():
            await ex.close()

    def to_string(self):
        return "exchange_datahub({0}) ".format(self.project_name)

    def init_exchanges(self):
        for id in ccxt.exchanges:
            if self.exchanges.get(id) is None:
                self.exchanges[id] = exchange_base(util.util.get_exchange(id, False))
    
    def create_project(self):
        try:
            self.datahub.create_project(self.project_name, self.project_name)
            logger.debug(self.to_string() + "create project success!")
        except ResourceExistException:
            logger.debug(self.to_string() + "project already exist!")
        except Exception:
            logger.info(traceback.format_exc())
            raise

    def create_all_topic(self):
        for k,v in conf.conf_aliyun.conf_aliyun_datahub['topics'].items():
            topic_name = k
            shard_count = v['shard_count']
            life_cycle = v['life_cycle']
            record_schema = RecordSchema.from_lists(v['record_schema'][0], v['record_schema'][1], v['record_schema'][2])
            try:
                self.datahub.create_tuple_topic(self.project_name, topic_name, shard_count, life_cycle, record_schema, topic_name)
                logger.debug(self.to_string() + "create topic success!")
            except ResourceExistException:
                logger.debug(self.to_string() + "topic already exist!")
            except Exception:
                logger.info(traceback.format_exc())
                raise

    def get_topic(self, topic_name):
        # block等待所有shard状态ready
        self.datahub.wait_shards_ready(self.project_name, topic_name)
        topic = self.datahub.get_topic(self.project_name, topic_name)
        #logger.debug(self.to_string() + "get_topic() topic={0}".format(topic))
        if topic.record_type != RecordType.TUPLE:
            raise Exception(self.to_string() + "get_topic({0}) topic.record_type != RecordType.TUPLE".format(topic_name))
        shards_result = self.datahub.list_shard(self.project_name, topic_name)
        shards = shards_result.shards
        self.topic = topic
        self.shards = shards
        return (topic, shards)
    
    def pub_topic(self, topic_name, records):
        if len(records) <= 0:
            return
        #logger.debug(self.to_string() + "pub_topic({0}) len(records) = {1}".format(topic_name, len(records)))
        failed_indexs = self.datahub.put_records(self.project_name, topic_name, records)
        #logger.debug(self.to_string() + "pub_topic() failed_indexs = {0}".format(failed_indexs))
        i = 0
        while failed_indexs.failed_record_count > 0 :
            logger.debug(self.to_string() + "pub_topic() put failed = {0}".format(failed_indexs))
            failed_indexs = self.datahub.put_records(self.project_name, topic_name, failed_indexs.failed_records)
            i = i + 1
            if i > 3:
                break

    async def pub_topic_once(self, ex_id, topic_name, func, *args, **kwargs):
        topic, shards = self.get_topic(topic_name)
        c = 0
        while True:
            try:
                records = await func(ex_id, topic, shards, *args, **kwargs)
                logger.debug(self.to_string() + "pub_topic_once({0}, {1}) len(records) = {2}".format(ex_id, topic_name, len(records)))
                self.pub_topic(topic_name, records)
                return
            except ccxt.RequestTimeout:
                #logger.info(traceback.format_exc())
                await asyncio.sleep(10)
            except ccxt.DDoSProtection:
                #logger.error(traceback.format_exc())
                await asyncio.sleep(10)
            except:
                logger.error(self.to_string() + "pub_topic_once({0}, {1})".format(ex_id, topic_name))
                #logger.error(traceback.format_exc())
                await asyncio.sleep(10)
                c = c + 1
                if c > 10:
                    return

    async def run_pub_topic(self, ex_id, topic_name, func, *args, **kwargs):
        topic, shards = self.get_topic(topic_name)
        while True:
            try:
                records = await func(ex_id, topic, shards, *args, **kwargs)
                logger.debug(self.to_string() + "run_pub_topic({0}, {1}) len(records) = {2}".format(ex_id, topic_name, len(records)))
                self.pub_topic(topic_name, records)
            except DatahubException:
                logger.error(traceback.format_exc())
                #await asyncio.sleep(10)
            except ccxt.RequestTimeout:
                #logger.info(traceback.format_exc())
                await asyncio.sleep(10)
            except ccxt.DDoSProtection:
                #logger.error(traceback.format_exc())
                await asyncio.sleep(10)
            except ccxt.AuthenticationError:
                logger.error(traceback.format_exc())
                #await asyncio.sleep(10)
            except ccxt.ExchangeNotAvailable:
                logger.error(traceback.format_exc())
                #await asyncio.sleep(10)
            except ccxt.ExchangeError:
                logger.error(traceback.format_exc())
                #await asyncio.sleep(10)
            except ccxt.NetworkError:
                logger.error(traceback.format_exc())
                #await asyncio.sleep(10)
            except Exception:
                logger.info(traceback.format_exc())
                #await asyncio.sleep(10)
            except:
                logger.error(traceback.format_exc())
                #await asyncio.sleep(10)

    def run_get_topic(self, topic_name, func, *args, **kwargs):
        topic, shards = self.get_topic(topic_name)
        try:
            dict_shared_id_cursor = {}
            for shard_id in shards:
                cursor = self.datahub.get_cursor(self.project_name, topic_name, shard_id, self.cursor_type).cursor
                dict_shared_id_cursor[shard_id] = cursor
            while True:
                for shard_id, cursor in dict_shared_id_cursor.items():
                    get_result = self.datahub.get_tuple_records(self.project_name, topic_name, shard_id, self.topic.record_schema, cursor, self.get_limit_num)
                    func(get_result.records, *args, **kwargs)
                    cursor = get_result.next_cursor
        except DatahubException as e:
            logger.error(traceback.format_exc(e))
            #await asyncio.sleep(10)
        except Exception:
            logger.info(traceback.format_exc())
            #await asyncio.sleep(10)
        except:
            logger.error(traceback.format_exc())
            #await asyncio.sleep(10)

    async def fetch_exchanges(self, ex_id, topic, shards):
        self.init_exchanges()
        records = []
        i = 0
        for id, ex in self.exchanges.items():
            logger.debug(self.to_string() + "fetch_exchanges({0})".format(id))
            f_ex_id = id
            f_ex_name = ex.ex.name
            f_countries = ""
            for country in ex.ex.countries:
                f_countries = country + ","
            f_url_www = ex.ex.urls['www'][0] if type(ex.ex.urls['www']) is list else ex.ex.urls['www']
            f_ts = int(round(time.time() * 1000))
            record = TupleRecord(schema=topic.record_schema)
            record.values = [f_ex_id, f_ex_name, f_countries, f_url_www, f_ts]
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
        return records

    async def fetch_markets(self, ex_id, topic, shards):
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        logger.debug(self.to_string() + "fetch_markets({0})".format(ex_id))
        records = []
        if ex.ex.has['fetchMarkets'] is False:
            logger.info(self.to_string() + "fetch_markets({0}) NOT has interface".format(ex_id))
            return records
        await ex.ex.load_markets()
        i = 0
        f_ex_id = ex.ex.id
        for symbol in ex.ex.symbols:
            f_symbol = symbol
            f_base = ex.ex.markets[symbol]['base'] if ex.ex.markets[symbol].get('base') is not None else ''
            f_quote = ex.ex.markets[symbol]['quote'] if ex.ex.markets[symbol].get('quote') is not None else ''
            f_fee_maker = ex.ex.markets[symbol]['maker'] if ex.ex.markets[symbol].get('maker') is not None else 0
            f_fee_taker = ex.ex.markets[symbol]['taker'] if ex.ex.markets[symbol].get('taker') is not None else 0
            f_precision_amount = ex.ex.markets[symbol]['precision']['amount'] if ex.ex.markets[symbol].get('precision') is not None and ex.ex.markets[symbol]['precision'].get('amount') is not None else 0
            f_precision_price = ex.ex.markets[symbol]['precision']['price'] if ex.ex.markets[symbol].get('precision') is not None and ex.ex.markets[symbol]['precision'].get('price') is not None else 0
            f_limits_amount_min = ex.ex.markets[symbol]['limits']['amount']['min'] if ex.ex.markets[symbol].get('limits') is not None and ex.ex.markets[symbol]['limits'].get('amount') is not None else 0
            f_limits_price_min = ex.ex.markets[symbol]['limits']['price']['min'] if ex.ex.markets[symbol].get('limits') is not None and ex.ex.markets[symbol]['limits'].get('price') is not None else 0
            f_ts = int(round(time.time() * 1000))
            record = TupleRecord(schema=topic.record_schema)
            record.values = [f_ex_id, f_symbol, f_base, f_quote, f_fee_maker, f_fee_taker, f_precision_amount, f_precision_price, f_limits_amount_min, f_limits_price_min, f_ts]
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
        return records

    async def fetch_tickers(self, ex_id, topic, shards):
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        logger.debug(self.to_string() + "fetch_tickers({0})".format(ex_id))
        records = []
        if ex.ex.has['fetchTickers'] is False:
            if ex.ex.symbols is None:
                await ex.ex.load_markets()
            for symbol in ex.ex.symbols:
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
        tickers = await ex.ex.fetch_tickers()
        i = 0
        f_ex_id = ex.ex.id
        for symbol, ticker in tickers.items():
            f_symbol = symbol
            f_ts = int(ticker['timestamp']) if ticker['timestamp'] is not None else int(round(time.time() * 1000))
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
            v = [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume]
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
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        logger.debug(self.to_string() + "fetch_ticker({0})".format(ex_id))
        records = []
        #if ex.ex.has['fetchTicker'] is False:
        #    return records
        ticker = await ex.ex.fetch_ticker(symbol)
        f_ex_id = ex.ex.id
        f_symbol = symbol
        f_ts = int(ticker['timestamp']) if ticker['timestamp'] is not None else int(round(time.time() * 1000))
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
        v = [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume]
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

    async def run_calc_spread(self, topic_name="t_spread"):
        logger.debug(self.to_string() + "run_calc_spread()")
        topic, shards = self.get_topic(topic_name)
        shard_count = len(shards)
        while True:
            try:
                # 数据太多，处理不完
                qsize = self.queue_task_spread.qsize()
                if qsize >= 5000:
                    logger.info(self.to_string() + "run_calc_spread() qsize={0}".format(qsize))
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
    
                    # ['f_symbol', 'f_ex1', 'f_ex1_name', 'f_ex1_bid', 'f_ex1_ts', 'f_ex1_fee', 'f_ex2', 'f_ex2_name', 'f_ex2_ask', 'f_ex2_ts', 'f_ex2_fee', 'f_ts', 'f_spread', 'f_fee', 'f_profit', 'f_profit_p']
                    f_spread = ex1_bid-ex2_ask
                    f_profit=(f_spread - f_fee)
                    f_profit_p=(f_profit / ex1_bid) if ex1_bid > 0.0 else 0.0
                    record1 = TupleRecord(schema=topic.record_schema)
                    record1.values = [symbol, ex1, ex1_name, ex1_bid, ex1_ts, ex1_fee, ex2, ex2_name, ex2_ask, ex2_ts, ex2_fee, spread_ts, f_spread, f_fee, f_profit, f_profit_p]
                    i = random.randint(1,100) % shard_count
                    record1.shard_id = shards[i].shard_id
                    records.append(record1)

                    f_spread = ex2_bid-ex1_ask
                    f_profit=(f_spread - f_fee)
                    f_profit_p=(f_profit / ex2_bid) if ex2_bid > 0.0 else 0.0
                    record2 = TupleRecord(schema=topic.record_schema)
                    record2.values = [symbol, ex2, ex2_name, ex2_bid, ex2_ts, ex2_fee, ex1, ex1_name, ex1_ask, ex1_ts, ex1_fee, spread_ts, f_spread, f_fee, f_profit, f_profit_p]
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






