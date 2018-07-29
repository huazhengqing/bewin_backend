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
from arbitrage.stat_arbitrage import stat_arbitrage
import conf.conf_aliyun
import conf
import util
logger = util.get_log(__name__)


uesrid = 0
ex1_id = 'binance'
ex2_id = 'okex'
symbol = 'EOS/BTC'

ex1 = exchange_trade.create(uesrid, ex1_id)
ex2 = exchange_trade.create(uesrid, ex2_id)
sa = stat_arbitrage(symbol, ex1, ex2)
sa.rebalance_set(True, 0.5)

tasks = sa.add_async_task()
tasks.append(asyncio.ensure_future(sa.run(sa.run_arbitrage)))


pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
loop.close()


