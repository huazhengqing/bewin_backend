#!/usr/bin/python
import io
import os
import sys
import asyncio
import logging
import traceback
import requests
from fetch_base import fetch_base
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf
import util
logger = util.get_log(__name__)


ids = []
fetcher = fetch_base()
tasks = []
tasks.append(asyncio.ensure_future(fetcher.run_fetch_markets_to_db(ids)))

pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
loop.close()

