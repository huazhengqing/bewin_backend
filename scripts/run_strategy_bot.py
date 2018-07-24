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
from strategy.strategy_bot import strategy_bot
import conf.conf_aliyun
import conf
logger = util.get_log(__name__)



bot = strategy_bot()


tasks = []
tasks.append(asyncio.ensure_future(bot.datahub.run_get_topic("t_ohlcv", bot.topic_records_get)))
for i in range(100):
    tasks.append(asyncio.ensure_future(bot.topic_records_process()))




pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
    loop.close()
finally:
    logger.error(traceback.format_exc())
    loop.close()





