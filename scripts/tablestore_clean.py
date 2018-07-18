import io
import os
import sys
import time
import logging
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_aliyun
from tablestore import *


dev_or_product = conf.dev_or_product
if dev_or_product == 1:
    OTS_ID = conf.conf_aliyun.conf_aliyun_tablestore['dev_access_id']
    OTS_SECRET = conf.conf_aliyun.conf_aliyun_tablestore['dev_access_key']
    OTS_ENDPOINT = conf.conf_aliyun.conf_aliyun_tablestore['dev_endpoint']
    OTS_INSTANCE = conf.conf_aliyun.conf_aliyun_tablestore['dev_instance']
elif dev_or_product == 2:
    OTS_ID = conf.conf_aliyun.conf_aliyun_tablestore['product_access_id']
    OTS_SECRET = conf.conf_aliyun.conf_aliyun_tablestore['product_access_key']
    OTS_ENDPOINT = conf.conf_aliyun.conf_aliyun_tablestore['product_endpoint']
    OTS_INSTANCE = conf.conf_aliyun.conf_aliyun_tablestore['product_instance']
client = OTSClient(OTS_ENDPOINT, OTS_ID, OTS_SECRET, OTS_INSTANCE)


for table_name,v in conf.conf_aliyun.conf_aliyun_tablestore['tables'].items():
    try:
        client.delete_table(table_name)
    except:
        pass
    life_cycle = v['life_cycle']
    ver_count = v['ver_count']
    schema_of_primary_key = v['primary_key_schema']
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_option = TableOptions(life_cycle, ver_count)
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_option, reserved_throughput)
    time.sleep(1)


