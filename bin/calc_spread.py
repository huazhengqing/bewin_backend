# -*- coding: utf-8 -*-
import os
import sys
import time
import json
import random
import asyncio
import logging
import traceback
import configparser
import pymysql
import pymysql.cursors
import DBUtils.PooledDB
from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf.conf_ex
import util.util
import util.exchange_datahub
import util.db_mysql
logger = util.util.get_log(__name__)


class calc_spread():
    def __init__(self):
        self.get_topic_name = "t_ticker"
        self.pub_topic_name = "t_spread"
        self.dh = util.exchange_datahub.exchange_datahub()
        self.dh.get_topic(self.get_topic_name)
        self.mysql = util.db_mysql.db_mysql()

    def pub_topic(self):
        self.dh.run_get_topic(self, self.get_topic_name, calc_spread)

    def calc_spread(self, tickers):
        if len(tickers) <= 0:
            return
        shard_count = len(self.dh.shards)
        records = []
        c = 0
        for ticker in tickers:
            ex1 = ticker[0]
            symbol = ticker[1]
            ex1_ts = ticker[2]
            ex1_bid = ticker[3]
            ex1_ask = ticker[5]
            sql_select = "SELECT f_ex_id,f_bid,f_ask,f_ts FROM t_ticker_crrent where f_symbol=\"{0}\" and f_ex_id!=\"{1}\" and f_ts > (UNIX_TIMESTAMP() - 30) * 1000;".format(symbol, ex1) 
            logger.debug("sql_select={0}".format(sql_select))
            rows = self.mysql.fetchall(sql_select)
            logger.debug("rows={0}".format(len(rows)))
            if len(rows) <= 0:
                logger.debug("len(rows) <= 0")
                continue
            for row in rows:
                ex2 = row[0]
                ex2_bid = row[1]
                ex2_ask = row[2]
                ex2_ts = row[3]
                spread_ts = ex1_ts if ex1_ts > ex2_ts else ex2_ts

                record1 = TupleRecord(schema=self.dh.topic.record_schema)
                record1.values = [symbol, ex1, ex2, ex1_bid-ex2_ask, spread_ts]
                record1.shard_id = self.dh.shards[c % shard_count].shard_id
                records.append(record1)
                c = c + 1

                record2 = TupleRecord(schema=self.dh.topic.record_schema)
                record2.values = [symbol, ex2, ex1, ex2_bid-ex1_ask, spread_ts]
                record2.shard_id = self.dh.shards[c % shard_count].shard_id
                records.append(record2)
        self.dh.pub_topic(self.pub_topic_name, records)
        

try:
    calc = calc_spread()
    calc.pub_topic()
except DatahubException as e:
    logger.error(traceback.format_exc(e))
except Exception:
    logger.info(traceback.format_exc())
except:
    logger.error(traceback.format_exc())

