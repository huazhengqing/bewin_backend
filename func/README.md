# aliyun 函数计算

## Python

### Python 用到的安装包

```shell
pip install PyMySQL --target="."
pip install pydatahub --target="."
```

linux (可以从linux copy 过来): 

```shell
pip install lz4 --target="."
pip install cprotobuf --target="."
```

### Python 参数定义

```python
class Credentials:
    def __init__(self, access_key_id, access_key_secret, security_token):
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.security_token = security_token
class ServiceMeta:
    def __init__(self, service_name, log_project, log_store):
        self.name = service_name
        self.log_project = log_project
        self.log_store = log_store
class FunctionMeta:
    def __init__(self, name, handler, memory, timeout):
        self.name = name
        self.handler = handler
        self.memory = memory
        self.timeout = timeout
class FCContext:
    def __init__(self, account_id, request_id, credentials, function_meta, service_meta, region):
        self.requestId = request_id
        self.credentials = credentials
        self.function = function_meta
        self.request_id = request_id
        self.service = service_meta
        self.region = region
        self.account_id = account_id
```

## DataHub 传过来的 event:

```json
{
  "eventSource": "acs:datahub",
  "eventName": "acs:datahub:putRecord",
  "eventSourceARN": "/projects/test_project_name/topics/test_topic_name",
  "region": "cn-hangzhou",
  "records": [
    {
      "eventId": "0:12345",
      "systemTime": 1463000123000,
      "data": "[\"f_ex_id\",\"f_symbol\"]"
    },
    {
      "eventId": "0:12346",
      "systemTime": 1463000156000,
      "data": "[\"f_ex_id\",\"f_symbol\"]"
    }
  ]
}
```

data 内容:

```json
['f_ex_id', 'f_symbol', 'f_ts', 'f_bid', 'f_bid_volume', 'f_ask', 'f_ask_volume', 'f_vwap', 'f_open', 'f_high', 'f_low', 'f_close', 'f_last', 'f_previous_close', 'f_change', 'f_percentage', 'f_average', 'f_base_volume', 'f_quote_volume']
```

## mysql 表结构:

见 sql_mysql
