#!/usr/bin/python
import os
import sys
import math
import time
import numpy
import random
import logging
import asyncio
import requests
import traceback
import threading
import urllib.request
import multiprocessing
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor, wait, as_completed
import ccxt.async as ccxt
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf.conf_ex
import util.util
from util.exchange_base import exchange_base
#logger = util.util.get_log(__name__)







def get_page(url):
    print('<%s> is getting [%s]'%(os.getpid(),url))
    response = requests.get(url)
    if response.status_code==200:  #200代表状态：下载成功了
        return {'url':url,'text':response.text}

def parse_page(res):
    res = res.result()
    print('<%s> is getting [%s]'%(os.getpid(),res['url']))
    with open('db.txt','a') as f:
        parse_res = 'url:%s size:%s\n'%(res['url'],len(res['text']))
        f.write(parse_res)

if __name__ == '__main__':
    # p = ThreadPoolExecutor()
    p = ProcessPoolExecutor()
    l = [
        'http://www.baidu.com',
        'http://www.baidu.com',
        'http://www.baidu.com',
        'http://www.baidu.com',
    ]
    for url in l:
        res = p.submit(get_page,url).add_done_callback(parse_page) #这里的回调函数拿到的是一个对象。得
        #  先把返回的res得到一个结果。即在前面加上一个res.result() #谁好了谁去掉回调函数
                                # 回调函数也是一种编程思想。不仅开线程池用，开线程池也用
    p.shutdown()  #相当于进程池里的close和join
    print('主',os.getpid())




def return_future_result(message):
    time.sleep(2)
    return message
pool = ThreadPoolExecutor(max_workers=2)  # 创建一个最大可容纳2个task的线程池
future1 = pool.submit(return_future_result, ("hello"))  # 往线程池里面加入一个task
future2 = pool.submit(return_future_result, ("world"))  # 往线程池里面加入一个task
print(future1.done())  # 判断task1是否结束
time.sleep(3)
print(future2.done())  # 判断task2是否结束
print(future1.result())  # 查看task1返回的结果
print(future2.result())  # 查看task2返回的结果




URLS = ['http://httpbin.org', 'http://example.com/', 'https://api.github.com/']
def load_url(url, timeout):
    with urllib.request.urlopen(url, timeout=timeout) as conn:
        return conn.read()
# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    # Start the load operations and mark each future with its URL
    future_to_url = {executor.submit(load_url, url, 60): url for url in URLS}
    for future in concurrent.futures.as_completed(future_to_url):
        url = future_to_url[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        else:
            print('%r page is %d bytes' % (url, len(data)))


URLS = ['http://httpbin.org', 'http://example.com/', 'https://api.github.com/']
def load_url(url):
    with urllib.request.urlopen(url, timeout=60) as conn:
        return conn.read()
# We can use a with statement to ensure threads are cleaned up promptly
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    for url, data in zip(URLS, executor.map(load_url, URLS)):
        print('%r page is %d bytes' % (url, len(data)))




def return_after_random_secs(num):
    sleep(randint(1, 5))
    return "Return of {}".format(num)
pool = ThreadPoolExecutor(5)
futures = []
for x in range(5):
    futures.append(pool.submit(return_after_random_secs, x))
print(wait(futures))









#! /usr/bin/env python
# -*- coding: utf-8 -*-

import threading
import Queue
import hashlib
import logging
from utils.progress import PrintProgress
from utils.save import SaveToSqlite


class ThreadPool(object):
    def __init__(self, thread_num, args):

        self.args = args
        self.work_queue = Queue.Queue()
        self.save_queue = Queue.Queue()
        self.threads = []
        self.running = 0
        self.failure = 0
        self.success = 0
        self.tasks = {}
        self.thread_name = threading.current_thread().getName()
        self.__init_thread_pool(thread_num)

    # 线程池初始化
    def __init_thread_pool(self, thread_num):
        # 下载线程
        for i in range(thread_num):
            self.threads.append(WorkThread(self))
        # 打印进度信息线程
        self.threads.append(PrintProgress(self))
        # 保存线程
        self.threads.append(SaveToSqlite(self, self.args.dbfile))

    # 添加下载任务
    def add_task(self, func, url, deep):
        # 记录任务，判断是否已经下载过
        url_hash = hashlib.new('md5', url.encode("utf8")).hexdigest()
        if not url_hash in self.tasks:
            self.tasks[url_hash] = url
            self.work_queue.put((func, url, deep))
            logging.info("{0} add task {1}".format(self.thread_name, url.encode("utf8")))

    # 获取下载任务
    def get_task(self):
        # 从队列里取元素，如果block=True,则一直阻塞到有可用元素为止。
        task = self.work_queue.get(block=False)

        return task

    def task_done(self):
        # 表示队列中的某个元素已经执行完毕。
        self.work_queue.task_done()

    # 开始任务
    def start_task(self):
        for item in self.threads:
            item.start()

        logging.debug("Work start")

    def increase_success(self):
        self.success += 1

    def increase_failure(self):
        self.failure += 1

    def increase_running(self):
        self.running += 1

    def decrease_running(self):
        self.running -= 1

    def get_running(self):
        return self.running

    # 打印执行信息
    def get_progress_info(self):
        progress_info = {}
        progress_info['work_queue_number'] = self.work_queue.qsize()
        progress_info['tasks_number'] = len(self.tasks)
        progress_info['save_queue_number'] = self.save_queue.qsize()
        progress_info['success'] = self.success
        progress_info['failure'] = self.failure

        return progress_info

    def add_save_task(self, url, html):
        self.save_queue.put((url, html))

    def get_save_task(self):
        save_task = self.save_queue.get(block=False)

        return save_task

    def wait_all_complete(self):
        for item in self.threads:
            if item.isAlive():
                # join函数的意义，只有当前执行join函数的线程结束，程序才能接着执行下去
                item.join()

# WorkThread 继承自threading.Thread
class WorkThread(threading.Thread):
    # 这里的thread_pool就是上面的ThreadPool类
    def __init__(self, thread_pool):
        threading.Thread.__init__(self)
        self.thread_pool = thread_pool

    #定义线程功能方法，即，当thread_1，...，thread_n，调用start（）之后，执行的操作。
    def run(self):
        print (threading.current_thread().getName())
        while True:
            try:
                # get_task()获取从工作队列里获取当前正在下载的线程，格式为func,url,deep
                do, url, deep = self.thread_pool.get_task()
                self.thread_pool.increase_running()

                # 判断deep，是否获取新的链接
                flag_get_new_link = True
                if deep >= self.thread_pool.args.deep:
                    flag_get_new_link = False

                # 此处do为工作队列传过来的func，返回值为一个页面内容和这个页面上所有的新链接
                html, new_link = do(url, self.thread_pool.args, flag_get_new_link)

                if html == '':
                    self.thread_pool.increase_failure()
                else:
                    self.thread_pool.increase_success()
                    # html添加到待保存队列
                    self.thread_pool.add_save_task(url, html)

                # 添加新任务，即，将新页面上的不重复的链接加入工作队列。
                if new_link:
                    for url in new_link:
                        self.thread_pool.add_task(do, url, deep + 1)

                self.thread_pool.decrease_running()
                # self.thread_pool.task_done()
            except Queue.Empty:
                if self.thread_pool.get_running() <= 0:
                    break
            except Exception, e:
                self.thread_pool.decrease_running()
                # print str(e)
                break









































'''
async def fun0():
    print("fun0()  111")
    await asyncio.sleep(1)
    print("fun0()  222")


def fun1():
    print("fun1()  111")
    loop2 = asyncio.new_event_loop()
    task = loop2.create_task(fun0())  
    loop2.run_until_complete(task)
    loop2.close()
    print("fun1()  222")



async def fun2():
    print("fun2()  111")
    await asyncio.sleep(1)
    print("fun2()  222")
    t = threading.Thread(target=fun1, args=())
    t.start()
    t.join()
    print("fun2()  333")



tasks = []
tasks.append(asyncio.ensure_future(fun2()))
pending = asyncio.Task.all_tasks()
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.gather(*pending))
loop.close()
'''




