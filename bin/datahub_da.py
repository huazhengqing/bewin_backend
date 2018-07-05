#!/usr/bin/python
import io
import os
import sys
import asyncio
import logging
import traceback
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import util.exchange_datahub
import conf.conf_aliyun
import conf.conf_ex
logger = util.util.get_log(__name__)


ids = conf.conf_ex.product_ex_ids
if conf.conf_aliyun.dev_or_product == 2:
    ids = conf.conf_ex.product_ex_ids


dh = util.exchange_datahub.exchange_datahub()
dh.create_project()
dh.create_all_topic()
dh.init_exchanges()


tasks = []
tasks.append(asyncio.ensure_future(dh.pub_topic_once("", "t_exchanges", dh.fetch_exchanges)))
for id in ids:
    tasks.append(asyncio.ensure_future(dh.pub_topic_once(id, "t_markets", dh.fetch_markets)))
    tasks.append(asyncio.ensure_future(dh.run_pub_topic(id, "t_ticker", dh.fetch_tickers)))
    
tasks.append(asyncio.ensure_future(dh.run_calc_spread()))

pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())

tasks = []
tasks.append(asyncio.ensure_future(dh.close()))
pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(*pending))
loop.close()

