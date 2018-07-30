import os
import sys
import time
import queue
import traceback
from typing import Any, Callable, Dict, List, Optional
import requests
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import db
import util
logger = util.get_log(__name__)



class db_ops(object):
    def __init__(self)-> None:
        self.queue_thread = queue.Queue()

    def to_string(self):
        return "db_ops[] "

    def put(self, record):
        self.queue_thread.put(record)

    def run_process_record(self):
        c = 0
        while True:
            qsize = self.queue_thread.qsize()
            if qsize >= 10:
                logger.warn(self.to_string() + "run_process_record() qsize={0}".format(qsize))
            record = self.queue_thread.get()
            try:
                db.Session().merge(record)
                c += 1
                if c >= 10:
                    c = 0
                    db.Session().flush()
            except Exception as e:
                logger.debug(self.to_string() + "run_process_record() Exception={0}".format(e))
                



g_db_ops = db_ops()

