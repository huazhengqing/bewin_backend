import os
import sys
import time
#import queue
import json
import arrow
import random
import asyncio
import logging
import traceback
import requests
requests.packages.urllib3.disable_warnings()
#import threading
#import multiprocessing
#import concurrent.futures
#from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, ALL_COMPLETED, FIRST_COMPLETED
import ccxt.async_support as ccxt
from datahub import DataHub
from datahub.exceptions import DatahubException, ResourceExistException
from datahub.models import RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
import conf
import util
import db
from db.datahub import datahub
from exchange.exchange import exchange
logger = util.get_log(__name__)




dh = datahub()
dh.create_project()
dh.create_all_topic()


















