#!/usr/bin/python
import os
import sys
import math
import time
import arrow
import random
import typing
import logging
import asyncio
import datetime
import traceback
import sqlalchemy as sql
import ccxt.async_support as ccxt
from random import randint
from typing import List, Dict, Any, Optional
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf
import util
from util import retry
import db
from exchange.exchange import exchange
logger = util.get_log(__name__)


class exchange_trade(exchange):
    def __init__(self, t_user_exchange_info):
        super(exchange_trade, self).__init__(t_user_exchange_info.f_ex_id, t_user_exchange_info.f_userid)

        self.ex.apiKey = t_user_exchange_info.f_apikey
        self.ex.secret = t_user_exchange_info.f_secret
        self.ex.password = t_user_exchange_info.f_password
        self.ex.uid = t_user_exchange_info.f_uid

        if conf.dev_or_product == 2:
            self.ex.aiohttp_proxy = t_user_exchange_info.f_aiohttp_proxy        
            #self.ex.proxy = t_user_exchange_info.f_proxy
            #self.ex.proxies = t_user_exchange_info.f_proxies
            logger.debug(self.to_string() + "aiohttp_proxy={0}".format(self.ex.aiohttp_proxy))

            '''
            url = self.ex.urls['www']
            tokens, user_agent = cfscrape.get_tokens(url)
            self.ex.headers = {
                'cookie': '; '.join([key + '=' + tokens[key] for key in tokens]),
                'user-agent': user_agent,
            }
            '''
        '''
        self.balance['BTC']['free']     # 还有多少钱
        self.balance['BTC']['used']
        self.balance['BTC']['total']
        '''
        self.balance = None

        # 仓位再平衡
        self.rebalance_position_proportion = 0.5
        self.rebalance_time = 0

    def to_string(self):
        return "exchange_trade[{0},{1}] ".format(self.ex_id, self.userid)

    async def fetch_balances(self):
        #logger.debug(self.to_string() + "fetch_balances() start")
        p = {}
        if self.ex.id == 'binance':
            p = {
                'recvWindow' : 60000,
            }
        self.balances = await self.ex.fetch_balances(p)
        self.balances.pop("info", None)
        self.balances.pop("free", None)
        self.balances.pop("total", None)
        self.balances.pop("used", None)
        #logger.debug(self.to_string() + "fetch_balances() end balance={0}".format(self.balance))
        #logger.debug(self.to_string() + "fetch_balances() end")
        return self.balances

    def get_balance(self, currency: str) -> float:
        balances = self.fetch_balances()
        balance = balances.get(currency)
        if not balance:
            raise Exception(self.to_string() + "get_balance({0}) error ".format(currency))
        return balance['free']












    async def fetch_my_trades_for_order(self, order_id: str, symbol: str, since: datetime) -> List:
        if not self.has_api('fetchMyTrades'):
            return []
        my_trades = await self.ex.fetch_my_trades(symbol, since.timestamp())
        matched_trades = [trade for trade in my_trades if trade['order'] == order_id]
        return matched_trades

    '''
    async def get_real_amount(self, t_trade: db.t_trades, order: Dict) -> float:
        order_amount = order['amount']

        # Only run for closed orders
        if t_trade.f_fee_open == 0 or order['status'] == 'open':
            return order_amount

        # use fee from order-dict if possible
        if 'fee' in order and order['fee'] and (order['fee'].keys() >= {'currency', 'cost'}):
            if t_trade.f_symbol.startswith(order['fee']['currency']):
                new_amount = order_amount - order['fee']['cost']
                return new_amount

        # Fallback to Trades
        trades = await self.fetch_my_trades_for_order(t_trade.f_open_order_id, t_trade.f_symbol, t_trade.f_open_date)
        if len(trades) == 0:
            return order_amount
        
        amount = 0
        fee_abs = 0
        for exectrade in trades:
            amount += exectrade['amount']
            if "fee" in exectrade and (exectrade['fee'].keys() >= {'currency', 'cost'}):
                # only applies if fee is in quote currency!
                if t_trade.f_symbol.startswith(exectrade['fee']['currency']):
                    fee_abs += exectrade['fee']['cost']

        if amount != order_amount:
            logger.warning(self.to_string() + f"amount {amount} does not match amount {t_trade.f_amount}")
            raise Exception("Half bought? Amounts don't match")

        real_amount = amount - fee_abs
        if fee_abs != 0:
            logger.info(f"""Applying fee on amount for {t_trade} (from {order_amount} to {real_amount}) from t_trade""")

        return real_amount
    '''
    def get_limits_amount_min_price(self, symbol: str, price: float) -> Optional[float]:
        #await self.load_markets()
        self.check_symbol(symbol)

        market = self.ex.markets[symbol]
        if 'limits' not in market:
            return None

        min_amounts = []
        limits = market['limits']
        if ('cost' in limits and 'min' in limits['cost'] and limits['cost']['min'] is not None):
            min_amounts.append(limits['cost']['min'])

        if ('amount' in limits and 'min' in limits['amount'] and limits['amount']['min'] is not None):
            min_amounts.append(limits['amount']['min'] * price)

        if not min_amounts:
            return None

        return max(min_amounts) * 2

    def calculate_fee(self, symbol='ETH/BTC', type='', side='', amount=1, price=1, taker_or_maker='maker') -> float:
        #await self.load_markets()
        self.check_symbol(symbol)
        return self.ex.calculate_fee(
            symbol=symbol, 
            type=type, 
            side=side, 
            amount=amount, 
            price=price, 
            takerOrMaker=taker_or_maker
            )['rate']

    async def amount_to_lots(self, symbol: str, amount: float) -> float:
        await self.load_markets()
        self.check_symbol(symbol)
        return self.ex.amount_to_lots(symbol, amount)




    '''
    # 订单结构
    {
        'id': str(order['id']),
        'timestamp': timestamp,
        'datetime': self.iso8601(timestamp),
        'status': status,
        'symbol': symbol,
        'type': order['ord_type'],
        'side': order['side'],
        'price': float(order['price']),
        'amount': float(order['volume']),
        'filled': float(order['executed_volume']),
        'remaining': float(order['remaining_volume']),
        'trades': None,
        'fee': None,
        'info': order,
    }
    {
        'id':                '12345-67890:09876/54321', // string
        'datetime':          '2017-08-17 12:42:48.000', // ISO8601 datetime of 'timestamp' with milliseconds
        'timestamp':          1502962946216, // order placing/opening Unix timestamp in milliseconds
        'lastTradeTimestamp': 1502962956216, // Unix timestamp of the most recent trade on this order
        'status':     'open',         // 'open', 'closed', 'canceled'
        'symbol':     'ETH/BTC',      // symbol
        'type':       'limit',        // 'market', 'limit'
        'side':       'buy',          // 'buy', 'sell'
        'price':       0.06917684,    // float price in quote currency
        'amount':      1.5,           // ordered amount of base currency
        'filled':      1.1,           // filled amount of base currency
        'remaining':   0.4,           // remaining amount to fill
        'cost':        0.076094524,   // 'filled' * 'price'
        'trades':    [ ... ],         // a list of order trades/executions
        'fee': {                      // fee info, if available
            'currency': 'BTC',        // which currency the fee is (usually quote)
            'cost': 0.0009,           // the fee amount in that currency
            'rate': 0.002,            // the fee rate (if available)
        },
        'info': { ... },              // the original unparsed order structure as is
    }
    '''


    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        return await self.ex.create_order(symbol, type, side, amount, price, params)

    async def fetch_order(self, id, symbol=None, params={}) -> Dict:
        return await self.ex.fetch_order(id, symbol, params)

    async def cancel_order(self, id, symbol=None, params={}) -> None:
        return await self.ex.cancel_order(id, symbol, params)


    async def buy(self, symbol: str, rate: float, amount: float) -> Dict:
        return await self.ex.create_limit_buy_order(symbol, amount, rate)

    async def sell(self, symbol: str, rate: float, amount: float) -> Dict:
        return await self.ex.create_limit_sell_order(symbol, amount, rate)



    # 限价买卖
    async def buy_cancel(self, symbol, amount):
        logger.debug(self.to_string() + "buy_cancel({0}, {1}) start".format(symbol, amount))
        if amount <= self.ex.markets[symbol]['limits']['amount']['min']:
            logger.debug(self.to_string() + "buy_cancel({0}, {1}) return min={2}".format(symbol, amount, self.ex.markets[symbol]['limits']['amount']['min']))
            return
        #await self.fetch_order_book(symbol, 5)
        price = self.order_book[symbol]['asks'][0][0]
        amount = util.downRound(amount, self.ex.markets[symbol]['precision']['amount'])
        ret = await self.ex.create_order(symbol, 'limit', 'buy', amount, price)
        ret = await self.ex.fetch_order(ret['id'], symbol)
        logger.debug(self.to_string() + "buy_cancel({0}, {1}) ret={2}".format(symbol, amount, ret))
        if not ret['filled'] or ret['filled'] <= 0.0:
            logger.debug(self.to_string() + "buy_cancel({0}, {1}) ret['filled'] <= 0.0 ret={2}".format(symbol, amount, ret))
        # 订单没有成交全部，剩下的订单取消
        if not ret['remaining'] or ret['remaining'] > 0:
            logger.debug(self.to_string() + "buy_cancel({0}, {1}) ret['remaining']={2}".format(symbol, amount, ret['remaining']))
            c = 0
            while c < 5:
                try:
                    c = c + 1
                    await self.ex.cancel_order(ret['id'])
                    logger.debug(self.to_string() + "buy_cancel({0}, {1}) cancel_order({2}) c={3}".format(symbol, amount, ret['id'], c))
                    break
                except:
                    logger.error(traceback.format_exc())
        logger.debug(self.to_string() + "buy_cancel({0}, {1}) end ret={2}".format(symbol, amount, ret))
        return ret

    async def sell_cancel(self, symbol, amount):
        logger.debug(self.to_string() + "sell_cancel({0}, {1}) start".format(symbol, amount))
        if amount <= self.ex.markets[symbol]['limits']['amount']['min']:
            logger.debug(self.to_string() + "sell_cancel({0}, {1}) return min={2}".format(symbol, amount, self.ex.markets[symbol]['limits']['amount']['min']))
            return
        #await self.fetch_order_book(symbol, 5)
        price = self.order_book[symbol]['bids'][0][0]
        amount = util.downRound(amount, self.ex.markets[symbol]['precision']['amount'])
        ret = await self.ex.create_order(symbol, 'limit', 'sell', amount, price)
        ret = await self.ex.fetch_order(ret['id'], symbol)
        logger.debug(self.to_string() + "sell_cancel({0}, {1}) ret={2}".format(symbol, amount, ret))
        if not ret['filled'] or ret['filled'] <= 0.0:
            logger.debug(self.to_string() + "sell_cancel({0}, {1}) ret['filled'] <= 0.0 ret={2}".format(symbol, amount, ret))
        # 订单没有成交全部，剩下的订单取消
        if not ret['remaining'] or ret['remaining'] > 0:
            logger.debug(self.to_string() + "sell_cancel({0}, {1}) ret['remaining']={2}".format(symbol, amount, ret['remaining']))
            c = 0
            while c < 5:
                try:
                    c = c + 1
                    await self.ex.cancel_order(ret['id'])
                    logger.debug(self.to_string() + "sell_cancel({0}, {1}) cancel_order({2}) c={3}".format(symbol, amount, ret['id'], c))
                    break
                except:
                    logger.error(traceback.format_exc())
        logger.debug(self.to_string() + "sell_cancel({0}, {1}) end ret={2}".format(symbol, amount, ret))
        return ret

    # 有交易所，只支持 limit order 
    async def buy_all(self, symbol, amount):
        logger.debug(self.to_string() + "buy_all({0}, {1}) start".format(symbol, amount))
        if amount < self.ex.markets[symbol]['limits']['amount']['min']:
            logger.debug(self.to_string() + "buy_all({0}, {1}) return amount.min={2}".format(symbol, amount, self.ex.markets[symbol]['limits']['amount']['min']))
            return
        #await self.fetch_order_book(symbol, 5)
        price = self.order_book[symbol]['asks'][0][0]
        amount = util.downRound(amount, self.ex.markets[symbol]['precision']['amount'])
        logger.debug(self.to_string() + "buy_all({0}, {1}) price={2}".format(symbol, amount, price))
        ret = None
        c = 0
        while c < 5:
            try:
                c = c + 1
                ret = await self.ex.create_order(symbol, 'limit', 'buy', amount, price)
                ret = await self.ex.fetch_order(ret['id'], symbol)
                logger.debug(self.to_string() + "buy_all({0}, {1}) create_order() ret={2} c={3}".format(symbol, amount, ret, c))
                break
            except:
                logger.error(traceback.format_exc())
        c = 0
        while ret['remaining'] is not None and ret['remaining'] >= self.ex.markets[symbol]['limits']['amount']['min']:
            try:
                price = self.order_book[symbol]['asks'][4][0]
                logger.debug(self.to_string() + "buy_all({0}, {1}) remaining price={2} c={3}".format(symbol, amount, price, c))
                ret = await self.ex.create_order(symbol, 'limit', 'buy', ret['remaining'], price)
                ret = await self.ex.fetch_order(ret['id'], symbol)
                logger.debug(self.to_string() + "buy_all({0}, {1}) remaining ret={2} c={3}".format(symbol, amount, ret, c))
                if ret['remaining'] is not None and ret['remaining'] >= self.ex.markets[symbol]['limits']['amount']['min']:
                    await self.fetch_order_book(symbol, 5)
            except:
                logger.error(traceback.format_exc())
                c = c + 1
                if c > 5:
                    raise
        logger.debug(self.to_string() + "buy_all({0}, {1}) end".format(symbol, amount))

    async def sell_all(self, symbol, amount):
        logger.debug(self.to_string() + "sell_all({0}, {1}) start".format(symbol, amount))
        if amount < self.ex.markets[symbol]['limits']['amount']['min']:
            logger.debug(self.to_string() + "sell_all({0}, {1}) return amount.min={2}".format(symbol, amount, self.ex.markets[symbol]['limits']['amount']['min']))
            return
        #await self.fetch_order_book(symbol, 5)
        price = self.order_book[symbol]['bids'][0][0]
        amount = util.downRound(amount, self.ex.markets[symbol]['precision']['amount'])
        ret = None
        c = 0
        while c < 5:
            try:
                c = c + 1
                ret = await self.ex.create_order(symbol, 'limit', 'sell', amount, price)
                ret = await self.ex.fetch_order(ret['id'], symbol)
                logger.debug(self.to_string() + "sell_all({0}, {1}) ret={2} c={3}".format(symbol, amount, ret, c))
                break
            except:
                logger.error(traceback.format_exc())
        c = 0
        while ret['remaining'] is not None and ret['remaining'] >= self.ex.markets[symbol]['limits']['amount']['min']:
            try:
                price = self.order_book[symbol]['bids'][4][0]
                logger.debug(self.to_string() + "sell_all({0}, {1}) remaining price={2} c={3}".format(symbol, amount, price, c))
                ret = await self.ex.create_order(symbol, 'limit', 'sell', ret['remaining'], price)
                ret = await self.ex.fetch_order(ret['id'], symbol)
                logger.debug(self.to_string() + "sell_all({0}, {1}) remaining ret={2} c={3}".format(symbol, amount, ret, c))
                if ret['remaining'] is not None and ret['remaining'] >= self.ex.markets[symbol]['limits']['amount']['min']:
                    await self.fetch_order_book(symbol, 5)
            except:
                logger.error(traceback.format_exc())
                c = c + 1
                if c > 5:
                    raise
        logger.debug(self.to_string() + "sell_all({0}, {1}) end".format(symbol, amount))

    # 仓位再平衡
    async def rebalance_position(self, symbol):
        #logger.debug(self.to_string() + "rebalance_position({0}) start".format(symbol))
        if self.rebalance_position_proportion <= 0.0:
            #logger.debug(self.to_string() + "rebalance_position({0}) return rebalance_position_proportion <= 0.0".format(symbol))
            return
        if arrow.utcnow().timestamp < self.rebalance_time + 60:
            #logger.debug(self.to_string() + "rebalance_position({0}) return  time  ".format(symbol))
            return
        self.rebalance_time = arrow.utcnow().timestamp
        logger.debug(self.to_string() + "rebalance_position({0}) start".format(symbol))
        await self.load_markets()
        await self.fetch_balances()
        await self.fetch_ticker(symbol)
        self.set_symbol(symbol)
        pos_value = self.balance[self.base_cur]['free'] * self.ticker['bid']
        total_value = self.balance[self.quote_cur]['free'] + pos_value
        target_pos_value = total_value * self.rebalance_position_proportion
        if pos_value < target_pos_value * 0.8:
            buy_amount = (target_pos_value - pos_value) / self.ticker['bid']
            logger.debug(self.to_string() + "rebalance_position({0}) buy_all({1}) ".format(symbol, buy_amount))
            await self.buy_all(symbol, buy_amount)
        elif pos_value > target_pos_value * 1.2:
            sell_amount = (pos_value - target_pos_value) / self.ticker['bid']
            logger.debug(self.to_string() + "rebalance_position({0}) sell_all({1}) ".format(symbol, sell_amount))
            await self.sell_all(symbol, sell_amount)
        self.rebalance_time = arrow.utcnow().timestamp
        logger.debug(self.to_string() + "rebalance_position({0}) end".format(symbol))

    # 评估账户 最小下注量
    async def balance_amount_min(self, symbol):
        logger.debug(self.to_string() + "balance_amount_min({0}) start".format(symbol))
        await self.load_markets()
        await self.fetch_balances()
        await self.fetch_ticker(symbol)
        self.set_symbol(symbol)
        pos_value = self.balance[self.base_cur]['free'] * self.ticker['bid']
        total_value = self.balance[self.quote_cur]['free'] + pos_value
        total_amount = total_value / self.ticker['ask'] * 0.97
        ret = max(self.ex.markets[symbol]['limits']['amount']['min'] * 2, total_amount * 0.02)
        logger.debug(self.to_string() + "balance_amount_min({0}) end ret={1}".format(symbol, ret))
        return ret


    # 3角套利，查找可以套利的币
    async def triangle_find_best_profit(self, quote1 = "BTC", quote2 = "ETH"):
        btc_coin = dict()
        eth_coin = dict()
        allcoin = await self.ex.request('ticker/price', 'v3')
        for item in allcoin:
            coin = item["symbol"][:-3]
            quote = item["symbol"][-3:]
            if quote == quote1:
                btc_coin[coin] = item
            if quote == quote2:
                eth_coin[coin] = item

        ethprice = float(btc_coin[quote2]["price"])

        #print("币种------ETH记价---ETH/BTC价---转成BTC价---直接BTC价--价差比")
        find_coin = ''
        find_profit = 0.0
        for k, v in btc_coin.items():
            if k in eth_coin:
                coin2btc = float(ethprice) * float(eth_coin[k]["price"])
                btcbuy  = float(btc_coin[k]["price"])
                profit = (btcbuy - coin2btc) / coin2btc
                if abs(profit) > 0.008:
                    #print("%s\t%10.8f  %10.8f  %10.8f  %10.8f  %s"%(k, float(eth_coin[k]["price"]), round(ethprice,8), round(coin2btc, 8), btcbuy, profit))
                    if abs(profit) > abs(find_profit):
                        find_coin = k
                        find_profit = profit

        if find_coin != '':
            coin_btc = find_coin + '/' + quote1
            coin_eth = find_coin + '/' + quote2
            fee = 0.003
            await self.fetch_order_book(coin_btc)
            fee += self.slippage_ratio
            #print(coin_btc, ': bids=', self.buy_1_price, '|asks=', self.sell_1_price, '|spread%=', round(self.slippage_ratio, 4))
            await self.fetch_order_book(coin_eth) 
            fee += self.slippage_ratio
            #print(coin_eth, ': bids=', self.buy_1_price, '|asks=', self.sell_1_price, '|spread%=', round(self.slippage_ratio, 4))
            if abs(find_profit) > fee:
                logger.info("%s \t %10.4f"%(find_coin, abs(find_profit) - fee))



class exchange_dryrun(exchange_trade):
    # Holds all open sell orders for dry_run
    _dry_run_open_orders: Dict[str, Any] = {}

    def __init__(self, ex_id, userid = 1):
        super(exchange_dryrun, self).__init__(ex_id, userid = 1)
        self.ex.apiKey = ""
        self.ex.secret = ""
        self.ex.password = ""
        self.ex.uid = ""

    def buy(self, pair: str, rate: float, amount: float) -> Dict:
        order_id = f'dry_run_buy_{randint(0, 10**6)}'
        self._dry_run_open_orders[order_id] = {
            'pair': pair,
            'price': rate,
            'amount': amount,
            'type': 'limit',
            'side': 'buy',
            'remaining': 0.0,
            'datetime': arrow.utcnow().isoformat(),
            'status': 'closed',
            'fee': None
        }
        return {'id': order_id}


    def sell(self, pair: str, rate: float, amount: float) -> Dict:
        order_id = f'dry_run_sell_{randint(0, 10**6)}'
        self._dry_run_open_orders[order_id] = {
            'pair': pair,
            'price': rate,
            'amount': amount,
            'type': 'limit',
            'side': 'sell',
            'remaining': 0.0,
            'datetime': arrow.utcnow().isoformat(),
            'status': 'closed'
        }
        return {'id': order_id}


    def get_balance(self, currency: str) -> float:
        return 999.9

    def get_balances(self) -> dict:
        return {}

    def cancel_order(self, order_id: str, pair: str) -> None:
        return


    def get_order(self, order_id: str, pair: str) -> Dict:
        order = self._dry_run_open_orders[order_id]
        order.update({
            'id': order_id
        })
        return order


    def get_trades_for_order(self, order_id: str, pair: str, since: datetime) -> List:
        return []




