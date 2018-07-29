import io
import os
import sys
import time
import arrow
import asyncio
import logging
import traceback
import requests
import threading
from concurrent.futures import ThreadPoolExecutor
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
from exchange.exchange_trade import exchange_trade
from arbitrage.triangle import triangle
import conf.conf_aliyun
import conf
import util
logger = util.get_log(__name__)


userid = 0
ex_id = 'binance'
base='EOS'
quote='ETH'
mid='USDT'


tr = triangle.create(userid, ex_id, base, quote, mid)
tasks = tr.add_async_task()


pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
loop.close()


