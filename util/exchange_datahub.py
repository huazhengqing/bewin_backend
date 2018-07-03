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
        self.datahub = None
        self.exchanges = dict()

        access_id = conf.conf_aliyun.conf_aliyun_datahub['access_id']
        access_key = conf.conf_aliyun.conf_aliyun_datahub['access_key']
        endpoint = conf.conf_aliyun.conf_aliyun_datahub['endpoint']
        #self.datahub = DataHub(access_id, access_key, endpoint, enable_pb=True)
        self.datahub = DataHub(access_id, access_key, endpoint)

    def to_string(self):
        return "exchange_datahub() "

    def create_project(self):
        project_name = conf.conf_aliyun.conf_aliyun_datahub['project']
        try:
            self.datahub.create_project(project_name, project_name)
            logger.debug(self.to_string() + "create project success!")
        except ResourceExistException:
            logger.debug(self.to_string() + "project already exist!")
        except Exception:
            logger.info(traceback.format_exc())
            raise

    def create_all_topic(self):
        project_name = conf.conf_aliyun.conf_aliyun_datahub['project']
        for k,v in conf.conf_aliyun.conf_aliyun_datahub['topics'].items():
            topic_name = k
            shard_count = v['shard_count']
            life_cycle = v['life_cycle']
            record_schema = RecordSchema.from_lists(v['record_schema'][0], v['record_schema'][1], v['record_schema'][2])
            try:
                self.datahub.create_tuple_topic(project_name, topic_name, shard_count, life_cycle, record_schema, topic_name)
                logger.debug(self.to_string() + "create topic success!")
            except ResourceExistException:
                logger.debug(self.to_string() + "topic already exist!")
            except Exception:
                logger.info(traceback.format_exc())
                raise

    async def pub_topic(self, ex_id, topic_name, func, *args, **kwargs):
        project_name = conf.conf_aliyun.conf_aliyun_datahub['project']
        try:
            # block等待所有shard状态ready
            self.datahub.wait_shards_ready(project_name, topic_name)
            topic = self.datahub.get_topic(project_name, topic_name)
            #logger.debug(self.to_string() + "pub_topic() topic={0}".format(topic))
            if topic.record_type != RecordType.TUPLE:
                #logger.error(self.to_string() + "pub_topic() topic type illegal!")
                raise Exception(self.to_string() + "pub_topic() topic type illegal!")

            shards_result = self.datahub.list_shard(project_name, topic_name)
            shards = shards_result.shards
            #for shard in shards:
            #    logger.debug(shard)

            records = await func(ex_id, topic, shards, *args, **kwargs)
            logger.debug(self.to_string() + "pub_topic({0}, {1}) records len = {2}".format(ex_id, topic_name, len(records)))

            failed_indexs = self.datahub.put_records(project_name, topic_name, records)
            #logger.debug(self.to_string() + "pub_topic() failed_indexs = {0}".format(failed_indexs))

            i = 0
            while failed_indexs.failed_record_count > 0 :
                logger.debug(self.to_string() + "pub_topic() put failed = {0}".format(failed_indexs))
                failed_indexs = self.datahub.put_records(project_name, topic_name, failed_indexs.failed_records)
                i = i + 1
                if i > 3:
                    break
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

    async def run(self, func, *args, **kwargs):
        while True:
            await func(*args, **kwargs)

    async def fetch_markets(self, ex_id, topic, shards):
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        await ex.load_markets()
        records = []
        i = 0
        f_ex_id = ex.ex.id
        for symbol in ex.ex.symbols:
            f_symbol = symbol
            f_base = ex.ex.markets[symbol]['base']
            f_quote = ex.ex.markets[symbol]['quote']
            f_fee_maker = ex.ex.markets[symbol]['maker']
            f_fee_taker = ex.ex.markets[symbol]['taker']
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
        #await ex.close()
        return records

    async def fetch_ticker(self, ex_id, topic, shards):
        if self.exchanges.get(ex_id) is None:
            self.exchanges[ex_id] = exchange_base(util.util.get_exchange(ex_id, False))
        ex = self.exchanges[ex_id]
        #await asyncio.sleep(ex.ex.rateLimit / 1000)
        tickers = await ex.ex.fetch_tickers()
        records = []
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
        #await ex.close()
        return records





