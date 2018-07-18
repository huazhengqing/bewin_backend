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
from fetch_exchange import fetch_exchange
import conf.conf_aliyun
import conf
logger = util.get_log(__name__)


ids = conf.dev_ex_ids
ids = [
    'binance',
]

symbols = None
timeframe = None
#since_ms = arrow.utcnow().shift(days=-1).timestamp * 1000
since_ms = None


if conf.dev_or_product == 2:
    ids = conf.product_ex_ids

fetcher = fetch_exchange()

tasks = []
for id in ids:
    if timeframe is None or timeframe == "":
        for tf in util.TimeFrame_Minutes.keys():
            tasks.append(asyncio.ensure_future(fetcher.run_fetch_ohlcv(id, "t_ohlcv", symbols, tf, since_ms)))
    else:
        tasks.append(asyncio.ensure_future(fetcher.run_fetch_ohlcv(id, "t_ohlcv", symbols, timeframe, since_ms)))



pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())


