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
from db.db_ops import g_db_ops
import util
from strategy.strategy_bot import strategy_bot
import conf.conf_aliyun
import conf
logger = util.get_log(__name__)




thread_db = threading.Thread(target=g_db_ops.run_process_record)
thread_db.start()

bot = strategy_bot()
thread_fetch = threading.Thread(target=g_datahub.run_get_topic, args=("t_ohlcv", bot.topic_records_get))
thread_fetch.start()


max_split_count = 5
tasks = []
executor = ThreadPoolExecutor()
tasks.append(asyncio.ensure_future(bot.run_update_config()))

#tasks.append(asyncio.ensure_future(bot.topic_records_process(0, 1)))
for i in range(max_split_count):
    tasks.append(asyncio.ensure_future(bot.topic_records_process(i, max_split_count)))
    #tasks.append(asyncio.ensure_future(asyncio.get_event_loop().run_in_executor(executor, util.sub_loop, bot.topic_records_process, i, max_split_count)))



pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
    thread_fetch.join()
    thread_db.join()
except:
    logger.error(traceback.format_exc())
loop.close()

