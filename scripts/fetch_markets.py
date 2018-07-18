#!/usr/bin/python
import io
import os
import sys
import asyncio
import logging
import traceback
import requests
from fetch_exchange import fetch_exchange
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf
import util
logger = util.get_log(__name__)


ids = conf.product_ex_ids
ids = [
    'binance',
]





if conf.dev_or_product == 2:
    ids = conf.product_ex_ids

fetcher = fetch_exchange()
tasks = []
tasks.append(asyncio.ensure_future(fetcher.pub_topic_once("", "t_exchanges", fetcher.fetch_exchanges)))
for id in ids:
    tasks.append(asyncio.ensure_future(fetcher.pub_topic_once(id, "t_markets", fetcher.fetch_markets)))



pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())



tasks = []
tasks.append(asyncio.ensure_future(fetcher.close()))
pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(*pending))
loop.close()

