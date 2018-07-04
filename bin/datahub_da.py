#!/usr/bin/python
import io
import os
import sys
import asyncio
import logging
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import util.exchange_datahub
#logger = util.util.get_log(__name__)


dh = util.exchange_datahub.exchange_datahub()
dh.create_project()
dh.create_all_topic()

ids = ['binance', 'okex']
tasks = []
tasks.append(asyncio.ensure_future(dh.pub_topic_once("", "t_exchanges", dh.fetch_exchanges)))

for id in ids:
    tasks.append(asyncio.ensure_future(dh.pub_topic_once(id, "t_markets", dh.fetch_markets)))
    tasks.append(asyncio.ensure_future(dh.run_pub_topic(id, "t_ticker", dh.fetch_tickers)))

pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(*pending))


