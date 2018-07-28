import io
import os
import sys
import asyncio
import logging
import traceback
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
from db.datahub import g_datahub
from exchange.fetch_base import fetch_base
from exchange.ws_binance import ws_binance
import conf
import util
logger = util.get_log(__name__)


ids = conf.dev_ex_ids
if conf.dev_or_product == 2:
    ids = conf.product_ex_ids
ids = [
    'okex',
    'huobipro',
    #'binance', # 用ws实现，另一个程序执行
]


fetcher = fetch_base()
tasks = []
for id in ids:
    tasks.append(asyncio.ensure_future(g_datahub.run_pub_topic(id, "t_ticker", fetcher.fetch_tickers)))

#for i in range(5):
#    tasks.append(asyncio.ensure_future(fetcher.run_calc_spread()))



pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(asyncio.gather(*pending))
except:
    logger.error(traceback.format_exc())
loop.close()

ws.bm.join()

