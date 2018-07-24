#!/usr/bin/python
import os
import sys
from collections import defaultdict
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






__ex_symbol_fee = util.nesteddict()




c = __ex_symbol_fee['a']['b'] if __ex_symbol_fee['a']['b'] else 0
print(c)
