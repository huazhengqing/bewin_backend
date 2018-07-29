import os
import sys
import math
import time
import arrow
import logging
import asyncio
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor
import ccxt.async_support as ccxt
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import db
from exchange.exchange_trade import exchange_trade
from exchange.ex_util import *
import util
logger = util.get_log(__name__)



'''
交易对(symbol):  (base/quote) 例如： LTC/BTC
基准资产(base currency): LTC
定价资产(quote currency: BTC

3角套利原理: 
用两个市场（比如 BTC/USD，LTC/USD）的价格（分别记为P1，P2），计算出一个公允的 LTC/BTC 价格（P2/P1），
如果该公允价格跟实际的LTC/BTC市场价格（记为P3）不一致，就产生了套利机会

['base=基准资产', 'quote=定价资产', 'mid=中间资产']
['XXX', 'BTC/ETH/BNB/HT/OKB/', 'USDT/USD/CNY']
['XXX', 'ETH/BNB/HT/OKB/', 'BTC']
['XXX', 'BNB/HT/OKB/', 'ETH']
'''
class triangle(exchange_trade):
    def __init__(self, t_user_exchange_info, base='EOS', quote='ETH', mid='USDT'):
        super(triangle, self).__init__(t_user_exchange_info)
        
        self.check(base, quote, mid)
        self.base = base
        self.quote = quote
        self.mid = mid
        self.base_quote = self.base + '/' + self.quote
        self.base_mid = self.base + '/' + self.mid
        self.quote_mid = self.quote + '/' + self.mid

        if self.base_quote not in g_ex_markets[self.ex_id].keys() or self.base_mid not in g_ex_markets[self.ex_id].keys() or self.quote_mid not in g_ex_markets[self.ex_id].keys():
            raise Exception(self.to_string() + "symbols error")

        self.base_quote_ask_1 = 0.0
        self.base_quote_bid_1 = 0.0
        self.base_quote_time = 0
        self.base_quote_time_pre = 0
        
        self.base_mid_ask_1 = 0.0
        self.base_mid_bid_1 = 0.0
        self.base_mid_time = 0
        self.base_mid_time_pre = 0
        
        self.quote_mid_ask_1 = 0.0
        self.quote_mid_bid_1 = 0.0
        self.quote_mid_time = 0
        self.quote_mid_time_pre = 0

        # 滑点 百分比，方便计算
        self.slippage_base_quote = 0.002  
        self.slippage_base_mid = 0.002
        self.slippage_quote_mid = 0.002

        # 吃单比例
        self.order_ratio_base_quote = 0.3
        self.order_ratio_base_mid = 0.3

        # 能用来操作的数量，用作限制。0表示不限制
        self.limit_base = 0.0
        self.limit_quote = 0.0
        self.limit_mid = 0.0

    @staticmethod
    def create(userid, ex_id, base='EOS', quote='ETH', mid='USDT'):
        t_user_exchange = db.Session().query(db.t_user_exchange).filter(
            db.t_user_exchange.f_userid == userid,
            db.t_user_exchange.f_ex_id == ex_id
        ).first()
        if not t_user_exchange:
            return None
        return triangle(t_user_exchange, base, quote, mid)

    '''
    ['base=基准资产', 'quote=定价资产', 'mid=中间资产']
    ['XXX', 'BTC/ETH/BNB/HT/OKB/', 'USDT/USD/CNY']
    ['XXX', 'ETH/BNB/HT/OKB/', 'BTC']
    ['XXX', 'BNB/HT/OKB/', 'ETH']
    '''
    def check(self, base, quote, mid):
        if mid in ['USD', 'USDT', 'CNY', 'JPY', 'EUR', 'CNH']:
            if quote not in ['BTC', 'ETH', 'BNB', 'HT', 'OKB']:
                raise Exception("check() base=" + base + ";quote=" + quote + ';mid=' + mid)
            return
        if mid in ['BTC']:
            if quote not in ['ETH', 'BNB', 'HT', 'OKB']:
                raise Exception("check() base=" + base + ";quote=" + quote + ';mid=' + mid)
            if base in ['USD', 'USDT', 'CNY', 'JPY', 'EUR', 'CNH']:
                raise Exception("check() base=" + base + ";quote=" + quote + ';mid=' + mid)
            return
        if mid in ['ETH']:
            if quote not in ['BNB', 'HT', 'OKB']:
                raise Exception("check() base=" + base + ";quote=" + quote + ';mid=' + mid)
            if base in ['BTC', 'USD', 'USDT', 'CNY', 'JPY', 'EUR', 'CNH']:
                raise Exception("check() base=" + base + ";quote=" + quote + ';mid=' + mid)
            return
        raise Exception("check() base=" + base + ";quote=" + quote + ';mid=' + mid)
    
    def to_string(self):
        return "triangle[{},{},{},{},{}] ".format(self.userid, self.ex.id, self.base, self.quote, self.mid)

    def add_async_task(self):
        tasks = []
        tasks.append(asyncio.ensure_future(self.fetch_balances()))
        tasks.append(asyncio.ensure_future(self.run(self.fetch_order_book_base_quote)))
        tasks.append(asyncio.ensure_future(self.run(self.fetch_order_book_base_mid)))
        tasks.append(asyncio.ensure_future(self.run(self.fetch_order_book_quote_mid)))
        tasks.append(asyncio.ensure_future(self.run(self.run_strategy)))
        return tasks

    async def fetch_order_book_base_quote(self):
        await self.fetch_order_book(self.base_quote, 5)
        self.base_quote_ask_1 = self.order_book[self.base_quote]['asks'][0][0]
        self.base_quote_bid_1 = self.order_book[self.base_quote]['bids'][0][0]
        self.slippage_base_quote = (self.base_quote_ask_1 - self.base_quote_bid_1)/self.base_quote_bid_1
        self.base_quote_time = arrow.utcnow().timestamp * 1000
        logger.debug(self.to_string() + "fetch_order_book_base_quote() slippage_base_quote={0}".format(self.slippage_base_quote))

    async def fetch_order_book_base_mid(self):
        await self.fetch_order_book(self.base_mid, 5)
        self.base_mid_ask_1 = self.order_book[self.base_mid]['asks'][0][0]
        self.base_mid_bid_1 = self.order_book[self.base_mid]['bids'][0][0]
        self.slippage_base_mid = (self.base_mid_ask_1 - self.base_mid_bid_1)/self.base_mid_bid_1
        self.base_mid_time = arrow.utcnow().timestamp * 1000
        logger.debug(self.to_string() + "fetch_order_book_base_mid() slippage_base_mid={0}".format(self.slippage_base_mid))

    async def fetch_order_book_quote_mid(self):
        await self.fetch_order_book(self.quote_mid, 5)
        self.quote_mid_ask_1 = self.order_book[self.quote_mid]['asks'][0][0]
        self.quote_mid_bid_1 = self.order_book[self.quote_mid]['bids'][0][0]
        self.slippage_quote_mid = (self.quote_mid_ask_1 - self.quote_mid_bid_1)/self.quote_mid_bid_1
        self.quote_mid_time = arrow.utcnow().timestamp * 1000
        logger.debug(self.to_string() + "fetch_order_book_quote_mid() slippage_quote_mid={0}".format(self.slippage_quote_mid))


    async def run_strategy(self):
        while self.base_quote_time <= self.base_quote_time_pre or self.base_mid_time <= self.base_mid_time_pre or self.quote_mid_time <= self.quote_mid_time_pre:
            await asyncio.sleep(0.3)

        self.base_quote_time_pre = self.base_quote_time
        self.base_mid_time_pre = self.base_mid_time
        self.quote_mid_time_pre = self.quote_mid_time

        '''
        3角套利原理: 
        用两个市场（比如 BTC/USD，LTC/USD）的价格（分别记为P1，P2），计算出一个公允的 LTC/BTC 价格（P2/P1），
        如果该公允价格跟实际的LTC/BTC市场价格（记为P3）不一致，就产生了套利机会
        当公允价和市场价的价差比例大于所有市场的费率总和再加上滑点总和时，做三角套利才是盈利的。
        
        对应的套利条件就是：
        ltc_cny_buy_1_price > btc_cny_sell_1_price*ltc_btc_sell_1_price*(1+btc_cny_slippage)*(1+ltc_btc_slippage) / [(1-btc_cny_fee)*(1-ltc_btc_fee)*(1-ltc_cny_fee)*(1-ltc_cny_slippage)]
        考虑到各市场费率都在千分之几的水平，做精度取舍后，该不等式可以进一步化简成：
        (ltc_cny_buy_1_price/btc_cny_sell_1_price-ltc_btc_sell_1_price)/ltc_btc_sell_1_price > sum_slippage_fee
        基本意思就是：只有当公允价和市场价的价差比例大于所有市场的费率总和再加上滑点总和时，做三角套利才是盈利的。
        '''
        # 检查是否有套利空间
        diff_price_pos = (self.base_mid_bid_1 / self.quote_mid_ask_1 - self.base_quote_ask_1)/self.base_quote_ask_1
        diff_price_neg = (self.base_quote_bid_1 - self.base_mid_ask_1 / self.quote_mid_bid_1)/self.base_quote_bid_1
        cost = self.sum_slippage_fee()
        logger.debug(self.to_string() +  "run_strategy() 正循环差价={0}, 滑点+手续费={1}".format(diff_price_pos, cost))
        logger.debug(self.to_string() +  "run_strategy() 逆循环差价={0}, 滑点+手续费={1}".format(diff_price_neg, cost))
        # 检查正循环套利
        if diff_price_pos > cost:
            logger.info(self.to_string() +  "run_strategy() pos_cycle() 正循环差价={0}, 滑点+手续费={1}".format(diff_price_pos, cost))
            await self.pos_cycle(self.get_base_quote_buy_size())
            await self.fetch_balances()
        # 检查逆循环套利
        elif diff_price_neg > cost:
            logger.info(self.to_string() +  "run_strategy() neg_cycle() 逆循环差价={0}, 滑点+手续费={1}".format(diff_price_neg, cost))
            await self.neg_cycle(self.get_base_quote_sell_size())
            await self.fetch_balances()
        #logger.debug(self.to_string() + "run_strategy() end")

    def sum_slippage_fee(self):
        return self.slippage_base_quote + self.slippage_base_mid + self.slippage_quote_mid + self.fee_taker * 3

    def get_base_quote_buy_size(self):
        can_use_amount_base = 0.0
        if self.limit_base <= 0.0:
            can_use_amount_base = self.balances[self.base]['free']
        else:
            can_use_amount_base = min(self.balances[self.base]['free'], self.limit_base)
        
        can_use_amount_quote = 0.0
        if self.limit_quote <= 0.0:
            can_use_amount_quote = self.balances[self.quote]['free']
        else:
            can_use_amount_quote = min(self.balances[self.quote]['free'], self.limit_quote)

        can_use_amount_mid = 0.0
        if self.limit_mid <= 0.0:
            can_use_amount_mid = self.balances[self.mid]['free']
        else:
            can_use_amount_mid = min(self.balances[self.mid]['free'], self.limit_mid)

        market_buy_size = self.order_book[self.base_quote]["asks"][0][1] * self.order_ratio_base_quote
        base_mid_sell_size = self.order_book[self.base_mid]["bids"][0][1] * self.order_ratio_base_mid

        base_quote_off_reserve_buy_size = (can_use_amount_quote) /  self.order_book[self.base_quote]["asks"][0][0]
        quote_mid_off_reserve_buy_size = (can_use_amount_mid) / self.order_book[self.quote_mid]["asks"][0][0] / self.order_book[self.base_quote]["asks"][0][0]
        base_mid_off_reserve_sell_size = can_use_amount_base

        logger.info(self.to_string() + "计算数量：{0}，{1}，{2}，{3}，{4}".format(
            market_buy_size
            , base_mid_sell_size
            , base_quote_off_reserve_buy_size
            , quote_mid_off_reserve_buy_size
            , base_mid_off_reserve_sell_size)
            )

        size = min(market_buy_size, base_mid_sell_size, base_quote_off_reserve_buy_size, quote_mid_off_reserve_buy_size, base_mid_off_reserve_sell_size)
        return size

    def get_base_quote_sell_size(self):
        can_use_amount_base = 0.0
        if self.limit_base <= 0.0:
            can_use_amount_base = self.balances[self.base]['free']
        else:
            can_use_amount_base = min(self.balances[self.base]['free'], self.limit_base)
        
        can_use_amount_quote = 0.0
        if self.limit_quote <= 0.0:
            can_use_amount_quote = self.balances[self.quote]['free']
        else:
            can_use_amount_quote = min(self.balances[self.quote]['free'], self.limit_quote)

        can_use_amount_mid = 0.0
        if self.limit_mid <= 0.0:
            can_use_amount_mid = self.balances[self.mid]['free']
        else:
            can_use_amount_mid = min(self.balances[self.mid]['free'], self.limit_mid)

        market_sell_size = self.order_book[self.base_quote]["bids"][0][1] * self.order_ratio_base_quote
        base_mid_buy_size = self.order_book[self.base_mid]["asks"][0][1] * self.order_ratio_base_mid

        base_quote_off_reserve_sell_size = can_use_amount_base
        quote_mid_off_reserve_sell_size = (can_use_amount_quote) / self.order_book[self.base_quote]["bids"][0][0]
        base_mid_off_reserve_buy_size = (can_use_amount_mid) / self.order_book[self.base_mid]["asks"][0][0]

        logger.info(self.to_string() + "计算数量：{0}，{1}，{2}，{3}，{4}".format(
            market_sell_size
            , base_mid_buy_size
            , base_quote_off_reserve_sell_size
            , quote_mid_off_reserve_sell_size
            , base_mid_off_reserve_buy_size)
            )

        return min(market_sell_size, base_mid_buy_size, base_quote_off_reserve_sell_size, quote_mid_off_reserve_sell_size, base_mid_off_reserve_buy_size)

    '''
    正循环:
    LTC/BTC 买, LTC/CNY 卖，BTC/CNY 买
    '''
    async def pos_cycle(self, base_quote_buy_amount):
        logger.debug(self.to_string() + "pos_cycle({0}) start".format(base_quote_buy_amount))
        ret = await self.buy_cancel(self.base_quote, base_quote_buy_amount)
        logger.debug(self.to_string() + "pos_cycle({0}) buy_cancel() ret={1}".format(base_quote_buy_amount, ret))
        if ret['filled'] is None or ret['filled'] <= 0:
            logger.debug(self.to_string() + "pos_cycle({0}) return ret['filled'] <= 0 ret={1}".format(base_quote_buy_amount, ret))
            return
        quote_to_be_hedged = ret['filled'] * ret['price']
        logger.debug(self.to_string() + "pos_cycle({0}) Process hedged_sell({1}, {2})".format(base_quote_buy_amount, self.base_mid, ret['filled']))
        p1 = threading.Thread(target=self.thread_hedged_sell, args=(self.base_mid, ret['filled']))
        p1.start()
        logger.debug(self.to_string() + "pos_cycle({0}) Process hedged_buy({1}, {2}={3}*{4})".format(base_quote_buy_amount, self.quote_mid, quote_to_be_hedged, ret['filled'], ret['price']))
        p2 = threading.Thread(target=self.thread_hedged_buy, args=(self.quote_mid, quote_to_be_hedged))
        p2.start()
        p1.join()
        p2.join()
        logger.debug(self.to_string() + "pos_cycle({0}) end".format(base_quote_buy_amount))

    '''
    逆循环:
    LTC/BTC 卖, LTC/CNY 买，BTC/CNY 卖
    '''
    async def neg_cycle(self, base_quote_sell_amount):
        logger.debug(self.to_string() + "neg_cycle({0}) start".format(base_quote_sell_amount))
        ret = await self.sell_cancel(self.base_quote, base_quote_sell_amount)
        logger.debug(self.to_string() + "neg_cycle({0}) sell_cancel() ret={1}".format(base_quote_sell_amount, ret))
        if ret['filled'] is None or ret['filled'] <= 0:
            logger.debug(self.to_string() + "neg_cycle({0}) return ret['filled'] <= 0 ret={1}".format(base_quote_sell_amount, ret))
            return
        quote_to_be_hedged = ret['filled'] * ret['price']
        logger.debug(self.to_string() + "neg_cycle({0}) hedged_buy({1}, {2}) Process".format(base_quote_sell_amount, self.base_mid, ret['filled']))
        p1 = threading.Thread(target=self.thread_hedged_buy, args=(self.base_mid, ret['filled']))
        p1.start()
        logger.debug(self.to_string() + "neg_cycle({0}) hedged_sell({1}, {2}={3}*{4}) Process".format(base_quote_sell_amount, self.quote_mid, quote_to_be_hedged, ret['filled'], ret['price']))
        p2 = threading.Thread(target=self.thread_hedged_sell, args=(self.quote_mid, quote_to_be_hedged))
        p2.start()
        p1.join()
        p2.join()
        logger.debug(self.to_string() + "neg_cycle({0}) end".format(base_quote_sell_amount))

    async def hedged_buy(self, symbol, amount):
        logger.debug(self.to_string() + "hedged_buy({0}, {1}) start ".format(symbol, amount))
        '''
        try:
            ret = await self.buy_cancel(self, symbol, amount)
            logger.debug(self.to_string() + "hedged_buy({0}, {1}) buy_cancel() ret={2} ".format(symbol, amount, ret))
            if amount > ret['filled']:
                logger.debug(self.to_string() + "hedged_buy({0}, {1}) buy_all({2}, {3})".format(symbol, amount, symbol, amount - ret['filled']))
                await self.buy_all(self, symbol, amount - ret['filled'])
        except:
            logger.error(traceback.format_exc())
        '''
        logger.debug(self.to_string() + "hedged_buy({0}, {1}) buy_all({2}, {3})".format(symbol, amount, symbol, amount))
        await self.buy_all(symbol, amount)
        logger.debug(self.to_string() + "hedged_buy({0}, {1}) end ".format(symbol, amount))

    def thread_hedged_buy(self, symbol, amount):
        logger.debug(self.to_string() + "thread_hedged_buy({0}, {1}) start ".format(symbol, amount))
        loop2 = asyncio.new_event_loop()
        task = loop2.create_task(self.hedged_buy(symbol, amount))  
        loop2.run_until_complete(task)
        loop2.close()
        logger.debug(self.to_string() + "thread_hedged_buy({0}, {1}) end ".format(symbol, amount))

    async def hedged_sell(self, symbol, amount):
        logger.debug(self.to_string() + "hedged_sell({0}, {1}) start ".format(symbol, amount))
        '''
        try:
            ret = await self.sell_cancel(self, symbol, amount)
            logger.debug(self.to_string() + "hedged_sell({0}, {1}) sell_cancel() ret={2} ".format(symbol, amount, ret))
            if amount > ret['filled']:
                logger.debug(self.to_string() + "hedged_sell({0}, {1}) sell_all({2}, {3})".format(symbol, amount, symbol, amount - ret['filled']))
                await self.sell_all(self, symbol, amount - ret['filled'])
        except:
            logger.error(traceback.format_exc())
        '''
        logger.debug(self.to_string() + "hedged_sell({0}, {1}) sell_all({2}, {3})".format(symbol, amount, symbol, amount))
        await self.sell_all(symbol, amount)
        logger.debug(self.to_string() + "hedged_sell({0}, {1}) end ".format(symbol, amount))
    
    def thread_hedged_sell(self, symbol, amount):
        logger.debug(self.to_string() + "thread_hedged_sell({0}, {1}) start ".format(symbol, amount))
        loop2 = asyncio.new_event_loop()
        task = loop2.create_task(self.hedged_sell(symbol, amount))  
        loop2.run_until_complete(task)
        loop2.close()
        logger.debug(self.to_string() + "thread_hedged_sell({0}, {1}) end ".format(symbol, amount))
    







