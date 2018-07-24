#!/usr/bin/python
import os
import sys
import time
import arrow
import random
import asyncio
import logging
import traceback
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
from conf.conf_aliyun import conf_aliyun_datahub
from exchange.exchange import exchange
logger = util.get_log(__name__)



'''
shards=
[
    {
        'ShardId': '0',
        'State': 'ACTIVE',
        'ClosedTime': '',
        'BeginHashKey': '00000000000000000000000000000000',
        'EndHashKey': '55555555555555555555555555555555',
        'ParentShardIds': [
            
        ],
        'LeftShardId': '4294967295',
        'RightShardId': '1'
    },
    {
        'ShardId': '1',
        'State': 'ACTIVE',
        'ClosedTime': '',
        'BeginHashKey': '55555555555555555555555555555555',
        'EndHashKey': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        'ParentShardIds': [
            
        ],
        'LeftShardId': '0',
        'RightShardId': '2'
    },
    {
        'ShardId': '2',
        'State': 'ACTIVE',
        'ClosedTime': '',
        'BeginHashKey': 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
        'EndHashKey': 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
        'ParentShardIds': [
            
        ],
        'LeftShardId': '1',
        'RightShardId': '4294967295'
    }
]
'''


class datahub():
    def __init__(self):
        access_id = conf_aliyun_datahub['dev_access_id']
        access_key = conf_aliyun_datahub['dev_access_key']
        endpoint = conf_aliyun_datahub['dev_endpoint']
        self.project_name = conf_aliyun_datahub['dev_project']
        if conf.dev_or_product == 2:
            logger.debug(self.to_string() + "product !!!")
            access_id = conf_aliyun_datahub['product_access_id']
            access_key = conf_aliyun_datahub['product_access_key']
            endpoint = conf_aliyun_datahub['product_endpoint']
            self.project_name = conf_aliyun_datahub['product_project']
        #self.datahub = DataHub(access_id, access_key, endpoint, enable_pb=True)
        self.datahub = DataHub(access_id, access_key, endpoint)
        self.cursor_type = CursorType.LATEST
        self.get_limit_num = 30

        self.create_project()
        self.create_all_topic()

    def to_string(self):
        return "datahub[{0}] ".format(self.project_name)

    def create_project(self):
        try:
            self.datahub.create_project(self.project_name, self.project_name)
            logger.debug(self.to_string() + "create_project({0})".format(self.project_name))
        except ResourceExistException:
            logger.debug(self.to_string() + "project already exist!")
        except Exception:
            logger.info(traceback.format_exc())
            raise

    def create_all_topic(self):
        for k,v in conf_aliyun_datahub['topics'].items():
            topic_name = k
            shard_count = v['shard_count']
            life_cycle = v['life_cycle']
            record_schema = RecordSchema.from_lists(v['record_schema'][0], v['record_schema'][1], v['record_schema'][2])
            try:
                self.datahub.create_tuple_topic(self.project_name, topic_name, shard_count, life_cycle, record_schema, topic_name)
                logger.debug(self.to_string() + "create_tuple_topic({0}, {1})".format(self.project_name, topic_name))
            except ResourceExistException:
                logger.debug(self.to_string() + "create_tuple_topic({0}, {1}) ResourceExistException".format(self.project_name, topic_name))
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
                logger.error(traceback.format_exc())
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

    '''
    get_result=
    {
        'NextCursor': '30005b54925e000000000002cd180001',
        'RecordCount': 1,
        'StartSeq': 183576,
        'Records': [
            {
                'Data': [
                    'okex',
                    'DENT/BTC',
                    '15',
                    '1532268900000',
                    '4.6e-07',
                    '4.6e-07',
                    '4.6e-07',
                    '4.6e-07',
                    '4541051.5',
                    '1532269148'
                ],
                'Sequence': 183576,
                'SystemTime': 1532269150134
            }
        ]
    }
    get_result.records=
    [TupleRecord {
  Values {
    *name*                *type*            *value*
    f_ex_id               string            okex
    f_symbol              string            KEY/ETH
    f_timeframe           bigint            1
    f_ts                  bigint            1532269740000
    f_o                   double            2.578e-05
    f_h                   double            2.578e-05
    f_l                   double            2.578e-05
    f_c                   double            2.578e-05
    f_v                   double            0.0
    f_ts_update           timestamp         1532269853
  }
}
]
    '''
    def run_get_topic(self, topic_name, func, *args, **kwargs):
        logger.debug(self.to_string() + "run_get_topic({0},{1})".format(self.project_name, topic_name))
        topic, shards = self.get_topic(topic_name)
        #logger.debug(self.to_string() + "run_get_topic({0},{1})shards={2}".format(self.project_name, topic_name, shards))
        while True:
            for shard in shards:
                #logger.debug(self.to_string() + "run_get_topic({0},{1})shard_id={2}".format(self.project_name, topic_name, shard_id))
                try:
                    cursor = self.datahub.get_cursor(self.project_name, topic_name, shard.shard_id, self.cursor_type).cursor
                    get_result = self.datahub.get_tuple_records(self.project_name, topic_name, shard.shard_id, topic.record_schema, cursor, self.get_limit_num)
                    if get_result.record_count > 0:
                        #logger.debug(self.to_string() + "run_get_topic({0},{1})get_result={2}".format(self.project_name, topic_name, get_result.records))
                        func(get_result.records, *args, **kwargs)
                    #else:
                        #await asyncio.sleep(0)
                        #logger.debug(self.to_string() + "run_get_topic({0},{1})  sleep  ".format(self.project_name, topic_name))
                    cursor = get_result.next_cursor
                except DatahubException as e:
                    logger.error(traceback.format_exc(e))
                except Exception:
                    logger.info(traceback.format_exc())
                except:
                    logger.error(traceback.format_exc())
            #await asyncio.sleep(0)
            

