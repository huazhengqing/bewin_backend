#!/usr/bin/python
import io
import os
import sys
import arrow
import asyncio
import logging
import traceback
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import util
from fetch_base import fetch_base
import conf
logger = util.get_log(__name__)



if conf.dev_or_product == 2:
    ids = conf.product_ex_ids


ids = [
    'okex',
    'huobipro',
    'binance',
]
symbols = []
#since_ms = arrow.utcnow().shift(days=-180).timestamp * 1000
since_ms = arrow.utcnow().shift(days=-1).timestamp * 1000

max_split_count = 5
fetcher = dict()
tasks = []
for i in range(max_split_count):
    fetcher[i] = fetch_base()
    for id in ids:
        #for tf in util.TimeFrame_Minutes.keys():
        for tf in conf.System_Strategy_TimeFrame_Minutes.keys():
            if id == 'huobipro' and tf == '4h':
                continue
            tasks.append(asyncio.ensure_future(fetcher[i].run_fetch_ohlcv(id, "t_ohlcv", symbols, tf, since_ms, i, max_split_count)))




pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
loop.close()


