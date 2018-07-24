#!/usr/bin/python
import os
import sys
from collections import defaultdict
from enum import Enum
import arrow
import asyncio
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import util
from strategy.strategy_bot import strategy_bot
import conf.conf_aliyun
import conf
logger = util.get_log(__name__)

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
import util
from strategy.strategy_bot import strategy_bot
import conf.conf_aliyun
import conf
logger = util.get_log(__name__)


queue_task_record = asyncio.Queue()

bot = strategy_bot()

executor = ThreadPoolExecutor()

queue_task_record = asyncio.Queue()

#queue_task_record.put(None)

async def t():
    while True:
        await asyncio.sleep(1)
        print("111")
        await queue_task_record.get()
        print("12222")
    

async def run():
    await asyncio.get_event_loop().run_in_executor(executor, util.sub_loop, t)



tasks = []
#tasks.append(asyncio.ensure_future(bot.topic_records_process()))
tasks.append(asyncio.ensure_future(t()))


pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
loop.close()


