import io
import os
import sys
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
from exchange.ws_binance import ws_binance




ws = ws_binance()
ws.start()
ws.bm.join()



