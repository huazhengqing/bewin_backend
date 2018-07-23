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

bot.datahub.run_get_topic("t_ohlcv", bot.process_topic_records)

