import os
import sys
import requests
requests.packages.urllib3.disable_warnings()
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
from db.datahub import g_datahub




g_datahub.create_project()
g_datahub.create_all_topic()



