# -*- coding: utf-8 -*-
import logging
import pymysql
import pymysql.cursors
import json
from datahub import DataHub
from datahub.exceptions import DatahubException
from datahub.models import TupleRecord
from conf_aliyun import conf_aliyun_mysql
from conf_aliyun import conf_aliyun_datahub
logger = logging.getLogger()

dev_or_product = 2

if dev_or_product == 1:
    host = conf_aliyun_mysql['dev_db_host']
    port = conf_aliyun_mysql['dev_db_port']
    user = conf_aliyun_mysql['dev_user']
    password = conf_aliyun_mysql['dev_password']
elif dev_or_product == 2:
    host = conf_aliyun_mysql['product_db_host']
    port = conf_aliyun_mysql['product_db_port']
    user = conf_aliyun_mysql['product_user']
    password = conf_aliyun_mysql['product_password']
db_name = conf_aliyun_mysql['db']

conn_read = pymysql.connect(
    host=host,
    user=user,
    password=password,
    db=db_name,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)

conn_write = pymysql.connect(
    host=host,
    user=user,
    password=password,
    db=db_name,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor)


if dev_or_product == 1:
    access_id = conf_aliyun_datahub['dev_access_id']
    access_key = conf_aliyun_datahub['dev_access_key']
    endpoint = conf_aliyun_datahub['dev_endpoint']
    project_name = conf_aliyun_datahub['dev_project']
elif dev_or_product == 2:
    access_id = conf_aliyun_datahub['product_access_id']
    access_key = conf_aliyun_datahub['product_access_key']
    endpoint = conf_aliyun_datahub['product_endpoint']
    project_name = conf_aliyun_datahub['product_project']

datahub = DataHub(access_id, access_key, endpoint)
topic_name = "t_spread"
datahub.wait_shards_ready(project_name, topic_name)
topic = datahub.get_topic(project_name, topic_name)
shards_result = datahub.list_shard(project_name, topic_name)
shards = shards_result.shards

'''
sql_select = "SELECT f_ex_id,f_bid,f_ask,f_ts FROM t_ticker_crrent where f_symbol=\"BTC/USDT\";"
cursor_reaed = conn_read.cursor()
cursor_reaed.execute(sql_select)
rows = cursor_reaed.fetchall()
logger.info(rows)
'''

def handler(event, context):
    logger.debug(event)
    str = event.decode()
    if str == "":
        logger.debug("str == \"\"")
        return
    evt = json.loads(str)
    if not evt:
        logger.debug("evt is None")
        return
    records = evt.get("records")
    if not records:
        logger.debug("records is None")
        return
    logger.debug("len(records)={0}".format(len(records)))
    logger.debug(records)
    for record in records:
        data = record["data"]
        ex1 = data[0]
        symbol = data[1]
        ex1_ts = data[2]
        ex1_bid = data[3]
        ex1_ask = data[5]
        sql_select = "SELECT f_ex_id,f_bid,f_ask,f_ts FROM t_ticker_crrent where f_symbol=\"{0}\" and f_ex_id!=\"{1}\" and f_ts > (UNIX_TIMESTAMP() - 30) * 1000;".format(symbol, ex1) 
        logger.debug("sql_select={0}".format(sql_select))
        cursor_reaed = conn_read.cursor()
        cursor_reaed.execute(sql_select)
        rows = cursor_reaed.fetchall()
        logger.debug("rows={0}".format(len(rows)))
        if len(rows) <= 0:
            logger.debug("len(rows) <= 0")
            continue
        records = []
        sql_insert = "replace into t_spread_current(f_symbol,f_ex1,f_ex2,f_spread,f_ts) values"
        c = 0
        cursor_write = conn_write.cursor()
        for row in rows:
            ex2 = row[0]
            ex2_bid = row[1]
            ex2_ask = row[2]
            ex2_ts = row[3]
            spread_ts = ex1_ts if ex1_ts > ex2_ts else ex2_ts
            sql_value1 = "('{0}','{1}','{2}',{3},{4})".format(symbol, ex1, ex2, ex1_bid-ex2_ask, spread_ts)
            sql_value2 = ",('{0}','{1}','{2}',{3},{4})".format(symbol, ex2, ex1, ex2_bid-ex1_ask, spread_ts)
            if c == 0:
                sql_insert = sql_insert + sql_value1 + sql_value2
            else:
                sql_insert = sql_insert + "," + sql_value1 + sql_value2
            c = c + 1

            record1 = TupleRecord(schema=topic.record_schema)
            record1.values = [symbol, ex1, ex2, ex1_bid-ex2_ask, spread_ts]
            record1.shard_id = shards[c % len(shards)].shard_id
            records.append(record1)
            c = c + 1

            record2 = TupleRecord(schema=topic.record_schema)
            record2.values = [symbol, ex2, ex1, ex2_bid-ex1_ask, spread_ts]
            record2.shard_id = shards[c % len(shards)].shard_id
            records.append(record2)

        sql_insert = sql_insert + ";"
        logger.debug("sql_insert={0}".format(sql_insert))
        count_write = cursor_write.execute(sql_insert)
        logger.debug(count_write)
        count_write.commit()

        logger.debug("datahub.put_records={0}".format(len(records)))
        datahub.put_records(project_name, topic_name, records)

