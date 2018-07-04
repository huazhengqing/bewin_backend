#!/usr/bin/python
import os
import sys
import time
import random
import asyncio
import logging
import traceback
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
        self.project_name = ""
        dev_or_product = conf.conf_aliyun.dev_or_product
        if dev_or_product == 1:
            access_id = conf.conf_aliyun.conf_aliyun_datahub['access_id_dev']
            access_key = conf.conf_aliyun.conf_aliyun_datahub['access_key_dev']
            endpoint = conf.conf_aliyun.conf_aliyun_datahub['endpoint_dev']
            self.project_name = conf.conf_aliyun.conf_aliyun_datahub['project_dev']
        elif dev_or_product == 2:
            access_id = conf.conf_aliyun.conf_aliyun_datahub['access_id_product']
            access_key = conf.conf_aliyun.conf_aliyun_datahub['access_key_product']
            endpoint = conf.conf_aliyun.conf_aliyun_datahub['endpoint_product']
            self.project_name = conf.conf_aliyun.conf_aliyun_datahub['project_product']
        #self.datahub = DataHub(access_id, access_key, endpoint, enable_pb=True)
        self.datahub = DataHub(access_id, access_key, endpoint)
        self.topic = None
        self.shards = []
        self.cursor_type = CursorType.LATEST
        self.get_limit_num = 30
        self.exchanges = dict()

    def to_string(self):
        return "exchange_datahub({0}) ".format(self.project_name)

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
        #logger.debug(self.to_string() + "pub_topic() topic={0}".format(topic))
        if topic.record_type != RecordType.TUPLE:
            #logger.error(self.to_string() + "pub_topic() topic type illegal!")
            raise Exception(self.to_string() + "pub_topic() topic type illegal!")
        shards_result = self.datahub.list_shard(self.project_name, topic_name)
        shards = shards_result.shards
        self.topic = topic
        self.shards = shards
        return (topic, shards)
    
    def pub_topic(self, topic_name, records):
        if len(records) <= 0:
            return
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
        records = await func(ex_id, topic, shards, *args, **kwargs)
        logger.debug(self.to_string() + "pub_topic({0}, {1}) records len = {2}".format(ex_id, topic_name, len(records)))
        self.pub_topic(topic_name, records)

    async def run_pub_topic(self, ex_id, topic_name, func, *args, **kwargs):
        try:
            topic, shards = self.get_topic(topic_name)
            while True:
                records = await func(ex_id, topic, shards, *args, **kwargs)
                logger.debug(self.to_string() + "pub_topic({0}, {1}) records len = {2}".format(ex_id, topic_name, len(records)))
                if len(records) <= 0:
                    continue
                self.pub_topic(topic_name, records)
        except DatahubException:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
        except ccxt.RequestTimeout:
            logger.info(traceback.format_exc())
            await asyncio.sleep(10)
        except ccxt.DDoSProtection:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
        except ccxt.AuthenticationError:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
        except ccxt.ExchangeNotAvailable:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
        except ccxt.ExchangeError:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
        except ccxt.NetworkError:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)
        except Exception:
            logger.info(traceback.format_exc())
            await asyncio.sleep(10)
        except:
            logger.error(traceback.format_exc())
            await asyncio.sleep(10)

    def run_get_topic(self, topic_name, func, *args, **kwargs):
        try:
            topic, shards = self.get_topic(topic_name)
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
        except Exception:
            logger.info(traceback.format_exc())
        except:
            logger.error(traceback.format_exc())

    async def fetch_exchanges(self, ex_id, topic, shards):
        records = []
        i = 0
        for id in ccxt.exchanges:
            exchange = getattr(ccxt, id)
            ex = exchange()
            f_ex_id = id
            f_ex_name = ex.name
            f_countries = ""
            for country in ex.countries:
                f_countries = country + ","
            f_url_www = ex.urls['www'][0] if type(ex.urls['www']) is list else ex.urls['www']
            f_ts = int(round(time.time() * 1000))
            record = TupleRecord(schema=topic.record_schema)
            record.values = [f_ex_id, f_ex_name, f_countries, f_url_www, f_ts]
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
            await ex.close()
        return records

    async def fetch_markets(self, ex_id, topic, shards):
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        records = []
        if ex.ex.has['fetchMarkets'] is False:
            return records
        await ex.ex.load_markets()
        i = 0
        f_ex_id = ex.ex.id
        for symbol in ex.ex.symbols:
            f_symbol = symbol
            f_base = ex.ex.markets[symbol]['base']
            f_quote = ex.ex.markets[symbol]['quote']
            f_fee_maker = ex.ex.markets[symbol]['maker'] if ex.ex.markets[symbol].get('maker') is not None else 0
            f_fee_taker = ex.ex.markets[symbol]['taker'] if ex.ex.markets[symbol].get('taker') is not None else 0
            f_precision_amount = ex.ex.markets[symbol]['precision']['amount']
            f_precision_price = ex.ex.markets[symbol]['precision']['price']
            f_limits_amount_min = ex.ex.markets[symbol]['limits']['amount']['min']
            f_limits_price_min = ex.ex.markets[symbol]['limits']['price']['min']
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
        records = []
        if ex.ex.has['fetchTickers'] is False:
            return records
        tickers = await ex.ex.fetch_tickers()
        i = 0
        f_ex_id = ex.ex.id
        for symbol, ticker in tickers.items():
            f_symbol = symbol
            f_ts = ticker['timestamp'] is not None and ticker['timestamp'] or int(round(time.time() * 1000))
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
            record = TupleRecord(schema=topic.record_schema)
            record.values = [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume]
            record.shard_id = shards[i % len(shards)].shard_id
            records.append(record)
            i = i + 1
        return records

    async def fetch_ticker(self, ex_id, topic, shards, symbol):
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        records = []
        if ex.ex.has['fetchTicker'] is False:
            return records
        ticker = await ex.ex.fetch_ticker(symbol)
        f_ex_id = ex.ex.id
        f_symbol = symbol
        f_ts = ticker['timestamp'] is not None and ticker['timestamp'] or int(round(time.time() * 1000))
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
        record = TupleRecord(schema=topic.record_schema)
        record.values = [f_ex_id, f_symbol, f_ts, f_bid, f_bid_volume, f_ask, f_ask_volume, f_vwap, f_open, f_high, f_low, f_close, f_last, f_previous_close, f_change, f_percentage, f_average, f_base_volume, f_quote_volume]
        i = random.randint(1,100) % len(shards)
        record.shard_id = shards[i].shard_id
        records.append(record)
        return records




