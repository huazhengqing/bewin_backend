# bewin

## ss

- [ss github](https://github.com/shadowsocks)
- [clients](https://shadowsocks.org/en/download/clients.html)
- [servers](https://shadowsocks.org/en/download/servers.html)

```shell
pip install --upgrade shadowsocks
```

## python3

- [python3.6.6](https://www.python.org)
- [Anaconda](https://www.anaconda.com/)


```shell
yum install -y openssl-devel libffi-devel
wget https://www.python.org/ftp/python/3.6.6/Python-3.6.6.tar.xz
python -m pip install --upgrade pip
```

## ccxt

- [ccxt](https://github.com/ccxt/ccxt)
- [talib安装(linux)](https://blog.csdn.net/fortiy/article/details/76531700)
- [talib安装(windows)](http://blog.chinaunix.net/uid-21519621-id-5757088.html)
- [talib windows 64位安装](https://blog.csdn.net/xiongjx3/article/details/80274678)

```shell
pip install --upgrade ccxt
pip install --upgrade cfscrape
pip install --upgrade numpy
pip install arrow
pip install talib
pip install sqlalchemy
pip install pandas
```



## aliyun 大数据总线(DataHub) 

- [console](https://datahub.console.aliyun.com/datahub)
- [mysql 白名单](https://help.aliyun.com/document_detail/62478.html)
- [数据类型 / 异常](https://help.aliyun.com/document_detail/47440.html)
- [访问控制 / 域名列表](https://help.aliyun.com/document_detail/47442.html)
- [Python SDK](https://github.com/aliyun/aliyun-datahub-sdk-python) ( 当前版本只支持 python2 )
- [修改后的支持 python3 的 SDK](https://github.com/huazhengqing/aliyun-datahub-sdk-python)
- [函数计算作为 DataHub 后端服务](https://help.aliyun.com/document_detail/60325.html)

python SDK 修改的内容：
```
aliyun-datahub-sdk-python/datahub/auth/aliyun_account.py : 129 行

auth_str = 'DATAHUB %s:%s' % (self._access_id, sign.decode())
```

安装:
```shell
pip install --upgrade pydatahub
```

地区|Region|外网Endpoint|经典网络ECS Endpoint|VPC ECS Endpoint
:-------------:|:----------------|:--------------------------------|:---------------------------------|:---------------------------------------
华东1(杭州)|cn-hangzhou|https://dh-cn-hangzhou.aliyuncs.com|http://dh-cn-hangzhou.aliyun-inc.com|http://dh-cn-hangzhou.aliyun-inc.com
华东2(上海)|cn-shanghai|https://dh-cn-shanghai.aliyuncs.com|http://dh-cn-shanghai.aliyun-inc.com|http://dh-cn-shanghai-int-vpc.aliyuncs.com
华北2(北京)|cn-beijing|https://dh-cn-beijing.aliyuncs.com|http://dh-cn-beijing.aliyun-inc.com|http://dh-cn-beijing-int-vpc.aliyuncs.com
华南1(深圳)|cn-shenzhen|https://dh-cn-shenzhen.aliyuncs.com|http://dh-cn-shenzhen.aliyun-inc.com|http://dh-cn-shenzhen-int-vpc.aliyuncs.com
亚太东南1(新加坡)|ap-southeast-1|https://dh-singapore.aliyuncs.com|http://dh-singapore.aliyun-inc.com|http://dh-singapore-int-vpc.aliyuncs.com

mysql 白名单，华南:
```
11.200.0.0/16
```

## aliyun 表格存储（Table Store / OTS）

- [console](https://www.aliyun.com/product/ots)
- [Python SDK](https://github.com/aliyun/aliyun-tablestore-python-sdk)
- [客户端 Windows版](https://market.aliyun.com/products/53690006/cmxz013097.html)

```sehll
pip install --upgrade tablestore
pip install --upgrade nose
```

## aliyun mysql

- [console](https://www.aliyun.com/product/rds/mysql)
- [文档](https://help.aliyun.com/document_detail/26125.html)
- [MySQL 5.1 参考手册](http://www.matools.com/manual/1300)

```shell
pip install --upgrade aliyun-python-sdk-rds
```

## aliyun 分析型数据库（AnalyticDB 原ADS）

- [console](https://www.aliyun.com/product/ads)
- [文档](https://help.aliyun.com/document_detail/26387.html)

## aliyun HybridDB for MySQL (原PetaData)

- [console](https://www.aliyun.com/product/petadata)
- [文档](https://help.aliyun.com/document_detail/64965.html)

## aliyun 流计算 / Blink SQL

- [console](https://data.aliyun.com/product/sc)
- [BlinkSQL 手册](https://help.aliyun.com/document_detail/62515.html)
- [数据类型](https://help.aliyun.com/document_detail/62497.html)
- [窗口函数](https://help.aliyun.com/document_detail/62510.html)
- [自定义函数(UDX)](https://help.aliyun.com/document_detail/69463.html)

## aliyun 函数计算

- [console](https://www.aliyun.com/product/fc)
- [服务地址](https://help.aliyun.com/document_detail/52984.html)
- [函数日志](https://help.aliyun.com/document_detail/73349.html)
- [Web Shell](http://fc-public.oss-cn-hangzhou.aliyuncs.com/demo/shell/index.html)
- [python SDK](https://github.com/aliyun/fc-python-sdk)
- [python 运行环境](https://help.aliyun.com/document_detail/56316.html)
- [nodejs SDK](https://github.com/aliyun/fc-nodejs-sdk)
- [nodejs 运行环境](https://help.aliyun.com/document_detail/58011.html)
- [fcli 命令行](https://github.com/aliyun/fcli)
- [fcli 手册](https://help.aliyun.com/document_detail/52995.html)
- [fcli 下载](https://github.com/aliyun/fcli/releases)
- [fun](https://github.com/aliyun/fun)
- [fun 手册](https://github.com/aliyun/fun/blob/master/README-zh.md)
- [十分钟上线-在函数计算上部署基于django开发的个人博客系统](https://yq.aliyun.com/articles/603249)
- [部署基于 python wsgi web 框架的工程到函数计算](https://yq.aliyun.com/articles/594300)

函数计算的 function 运行时的 IP 是不固定的，无法通过设置白名单的方式访问 RDS。支持 VPC。

```shell
pip install --upgrade requests
pip install --upgrade aliyun-fc2

npm install @alicloud/fc2 --save

npm install @alicloud/fun -g
fun -h
```

## aliyun DataV 数据可视化

- [console](https://data.aliyun.com/visual/datav)
- [服务器白名单](https://help.aliyun.com/document_detail/64686.html)
- [文档](https://help.aliyun.com/product/43570.html)
- [Linux下配置DataV Proxy](https://help.aliyun.com/document_detail/64141.html)
- [DataVProxy](https://github.com/ericdum/DataVProxy)
- [开发工具](https://help.aliyun.com/document_detail/70446.html)
- [项目发布地址:开发环境](https://datav.aliyun.com/share/7527c68399bdc18db99738a87d8693d9)
- [项目发布地址:生产环境](https://datav.aliyun.com/share/6f4b9a3b93a7b1beb5e049c5c6ce3afd)


mysql 白名单: datav

```
139.224.92.81/24,139.224.92.22/24,139.224.92.35/24,139.224.4.30/24,139.224.92.102/24,139.224.4.48/24,139.224.4.104/24,139.224.92.11/24,139.224.4.60/24,139.224.92.52/24,139.224.4.26/24,139.224.92.57/24,11.192.98.48/24,11.192.98.61/24,11.192.98.47/24,10.152.164.34/24,11.192.98.58/24,10.152.164.17/24,10.152.164.42/24,11.192.98.37/24,10.152.164.31/24
```

DataVProxy:
```sehll
wget https://codeload.github.com/ericdum/DataVProxy/zip/master
unzip master
cd DataVProxy-master
make install
node ./bin/info.js

pm2 logs
ls -al ./DataVProxy-master/logs

pm2 restart all
pm2  start app.js
```

开发工具:
```shell
npm install --registry=https://registry.npm.taobao.org datav-cli -g

datav init

datav run
```

## aliyun BI

- [console](https://data.aliyun.com/product/bi)
- [文档](https://help.aliyun.com/document_detail/67842.html)

同步 MySQL 数据源至 BI 探索空间需要切换至经典网络，VPC网络暂不支持。

mysql 白名单: bi
```
10.152.163.0/24,139.224.4.0/24,10.152.69.0/24
```

