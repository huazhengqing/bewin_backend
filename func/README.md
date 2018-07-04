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
      "data": "[\"col1's value\",\"col2's value\"]"
    },
    {
      "eventId": "0:12346",
      "systemTime": 1463000156000,
      "data": "[\"col1's value\",\"col2's value\"]"
    }
  ]
}
```

data 内容:

```json
['f_ex_id', 'f_symbol', 'f_ts', 'f_bid', 'f_bid_volume', 'f_ask', 'f_ask_volume', 'f_vwap', 'f_open', 'f_high', 'f_low', 'f_close', 'f_last', 'f_previous_close', 'f_change', 'f_percentage', 'f_average', 'f_base_volume', 'f_quote_volume']
```

## mysql 表结构:

```sql
CREATE TABLE `t_ticker_crrent` (
  `f_ex_id` varchar(64) NOT NULL,
  `f_symbol` varchar(32) NOT NULL,
  `f_ts` bigint(20) NOT NULL,
  `f_bid` double NOT NULL,
  `f_bid_volume` double NOT NULL,
  `f_ask` double NOT NULL,
  `f_ask_volume` double NOT NULL,
  `f_vwap` double NOT NULL,
  `f_open` double NOT NULL,
  `f_high` double NOT NULL,
  `f_low` double NOT NULL,
  `f_close` double NOT NULL,
  `f_last` double NOT NULL,
  `f_previous_close` double NOT NULL,
  `f_change` double NOT NULL,
  `f_percentage` double NOT NULL,
  `f_average` double NOT NULL,
  `f_base_volume` double NOT NULL,
  `f_quote_volume` double NOT NULL,
  PRIMARY KEY (`f_ex_id`,`f_symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `t_spread_current` (
  `f_symbol` varchar(32) NOT NULL,
  `f_ex1` varchar(64) NOT NULL,
  `f_ex2` varchar(64) NOT NULL,
  `f_ts` bigint(20) NOT NULL,
  `f_spread` double NOT NULL,
  PRIMARY KEY (`f_symbol`,`f_ex1`,`f_ex2`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
```