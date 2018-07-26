#!/usr/bin/python
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
from db.datahub import g_datahub
import util
from strategy.strategy_bot import strategy_bot
import conf.conf_aliyun
import conf
logger = util.get_log(__name__)



bot = strategy_bot()
thread_fetch = threading.Thread(target=g_datahub.run_get_topic, args=("t_ohlcv", bot.topic_records_get))
thread_fetch.start()



tasks = []
executor = ThreadPoolExecutor()
tasks.append(asyncio.ensure_future(bot.run_update_config()))
for i in range(10):
    #tasks.append(asyncio.ensure_future(bot.topic_records_process()))
    tasks.append(asyncio.ensure_future(asyncio.get_event_loop().run_in_executor(executor, util.sub_loop, bot.topic_records_process)))



pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
    thread_fetch.join()
except:
    logger.error(traceback.format_exc())
loop.close()

