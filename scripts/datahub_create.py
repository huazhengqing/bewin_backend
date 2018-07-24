import os
import sys
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
from db.datahub import datahub





dh = datahub()
dh.create_project()
dh.create_all_topic()


















