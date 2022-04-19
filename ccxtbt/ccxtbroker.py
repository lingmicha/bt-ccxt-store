#!/usr/bin/env python
# -*- coding: utf-8; py-indent-offset:4 -*-
###############################################################################
#
# Copyright (C) 2015, 2016, 2017 Daniel Rodriguez
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
###############################################################################
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import collections
import json

from backtrader import BrokerBase, OrderBase, Order
from backtrader.position import Position
from backtrader.utils.py3 import queue, with_metaclass

from .ccxtstore import CCXTStore


class CCXTOrder(OrderBase):
    def __init__(self, owner, data, ccxt_order):
        self.owner = owner
        self.data = data
        self.ccxt_order = ccxt_order
        self.executed_fills = []
        self.ordtype = self.Buy if ccxt_order['side'] == 'buy' else self.Sell
        self.size = float(ccxt_order['amount'])


        super(CCXTOrder, self).__init__()
        self.p.data = data # fix params data not defined

class MetaCCXTBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaCCXTBroker, cls).__init__(name, bases, dct)
        CCXTStore.BrokerCls = cls


class CCXTBroker(with_metaclass(MetaCCXTBroker, BrokerBase)):
    '''Broker implementation for CCXT cryptocurrency trading library.
    This class maps the orders/positions from CCXT to the
    internal API of ``backtrader``.

    Broker mapping added as I noticed that there differences between the expected
    order_types and retuned status's from canceling an order

    Added a new mappings parameter to the script with defaults.

    Added a get_balance function. Manually check the account balance and update brokers
    self.cash and self.value. This helps alleviate rate limit issues.

    Added a new get_wallet_balance method. This will allow manual checking of the any coins
        The method will allow setting parameters. Useful for dealing with multiple assets

    Modified getcash() and getvalue():
        Backtrader will call getcash and getvalue before and after next, slowing things down
        with rest calls. As such, th

    The broker mapping should contain a new dict for order_types and mappings like below:

    broker_mapping = {
        'order_types': {
            bt.Order.Market: 'market',
            bt.Order.Limit: 'limit',
            bt.Order.Stop: 'stop-loss', #stop-loss for kraken, stop for bitmex
            bt.Order.StopLimit: 'stop limit'
        },
        'mappings':{
            'closed_order':{
                'key': 'status',
                'value':'closed'
                },
            'canceled_order':{
                'key': 'result',
                'value':1}
                }
        }

    Added new private_end_point method to allow using any private non-unified end point

    '''

    order_types = {Order.Market: 'market',
                   Order.Limit: 'limit',
                   Order.Stop: 'stop',  # stop-loss for kraken, stop for bitmex
                   Order.StopLimit: 'stop limit'}

    mappings = {
        'closed_order': {
            'key': 'status',
            'value': 'closed'
        },
        'canceled_order': {
            'key': 'status',
            'value': 'canceled'}
    }

    def __init__(self, broker_mapping=None, debug=False, **kwargs):
        super(CCXTBroker, self).__init__()

        if broker_mapping is not None:
            try:
                self.order_types = broker_mapping['order_types']
            except KeyError:  # Might not want to change the order types
                pass
            try:
                self.mappings = broker_mapping['mappings']
            except KeyError:  # might not want to change the mappings
                pass

        self.store = CCXTStore(**kwargs)

        self.currency = self.store.currency

        self.positions = collections.defaultdict(Position)

        self.debug = debug
        self.indent = 4  # For pretty printing dictionaries

        self.notifs = queue.Queue()  # holds orders which are notified

        self.open_orders = list()

        self.startingcash = self.store._cash
        self.startingvalue = self.store._value

        self.use_order_params = False

        self.markets = self.store.load_markets() # no need to frequently update

    def get_balance(self):
        self.store.get_balance()
        self.cash = self.store._cash
        self.value = self.store._value
        return self.cash, self.value

    def get_wallet_balance(self, currency, params={}):
        balance = self.store.get_wallet_balance(currency, params=params)
        try:
            cash = balance['free'][currency] if balance['free'][currency] else 0
        except KeyError:  # never funded or eg. all USD exchanged
            cash = 0
        try:
            value = balance['total'][currency] if balance['total'][currency] else 0
        except KeyError:  # never funded or eg. all USD exchanged
            value = 0
        return cash, value

    def getcash(self):
        # Get cash seems to always be called before get value
        # Therefore it makes sense to add getbalance here.
        # return self.store.getcash(self.currency)
        # self.store.get_balance()
        self.cash = self.store._cash
        return self.cash

    get_cash = getcash

    def getvalue(self, datas=None):
        # return self.store.getvalue(self.currency)
        # self.store.get_balance()
        if datas is None:
            self.value = self.store._value
            return self.value

        pos_value = 0.0
        for data in datas:
            comminfo = self.getcommissioninfo(data)
            position = self.getposition(data)

            dvalue = comminfo.getvaluesize(position.size, data.close[0])
            pos_value += dvalue
        return pos_value

    get_value = getvalue

    def get_notification(self):
        try:
            return self.notifs.get(False)
        except queue.Empty:
            return None

    def notify(self, order):
        self.notifs.put(order)

    def sync_exchange_positions(self, datas=None):

        exchange_positions = self.store.getposition()

        for i, data in enumerate(datas):
            pos = self.positions[data._dataname]
            if data._dataname in exchange_positions:
                size = exchange_positions[data._dataname]['size']
                price = exchange_positions[data._dataname]['price']
                pos.set( size, price )

    def getposition(self, data, clone=True):
        # return self.o.getposition(data._dataname, clone=clone)
        pos = self.positions[data._dataname]
        if clone:
            pos = pos.clone()
        return pos

    def next(self):
        if self.debug:
            print('Broker next() called')

        for o_order in list(self.open_orders):
            oID = o_order.ccxt_order['id']

            # Print debug before fetching so we know which order is giving an
            # issue if it crashes
            if self.debug:
                print('Fetching Order ID: {}'.format(oID))

            # Get the order
            ccxt_order = self.store.fetch_order(oID, o_order.data.p.dataname)

            # Check for new fills
            if 'trades' in ccxt_order and ccxt_order['trades'] is not None:
                for fill in ccxt_order['trades']:
                    if fill not in o_order.executed_fills:
                        o_order.execute(fill['datetime'], fill['amount'], fill['price'],
                                        0, 0.0, 0.0,
                                        0, 0.0, 0.0,
                                        0.0, 0.0,
                                        0, 0.0)
                        o_order.executed_fills.append(fill['id'])

            if self.debug:
                print(json.dumps(ccxt_order, indent=self.indent))

            # Check if the order is closed
            if ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:

                """
                futures trading doesn't return trades inside orders, 
                so update order execution in one run
                """
                if self.store.get_type() == 'future':
                    o_order.execute(ccxt_order['datetime'], ccxt_order['amount'], ccxt_order['price'],
                                    0, 0.0, 0.0,
                                    0, 0.0, 0.0,
                                    0.0, 0.0,
                                    0, 0.0)

                pos = self.getposition(o_order.data, clone=False)
                pos.update(o_order.size, o_order.price)
                o_order.completed()
                self.notify(o_order)
                self.open_orders.remove(o_order)
                self.get_balance()

            # Manage case when an order is being Canceled from the Exchange
            #  from https://github.com/juancols/bt-ccxt-store/
            if ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
                self.open_orders.remove(o_order)
                o_order.cancel()
                self.notify(o_order)

    def _submit(self, owner, data, exectype, side, amount, price, params):
        if amount == 0 or price == 0:
        # do not allow failing orders
            return None

        # format amount & price
        formatted_amount = self.store.amount_to_precision(data.p.dataname, amount)
        formatted_price = None
        if price != None:
            formatted_price = self.store.price_to_precision(data.p.dataname, price)

        # check limits
        if not self.check_exchange_limit(data, formatted_amount, formatted_price):
            # Order would fail sending to exchange
            return None

        order_type = self.order_types.get(exectype) if exectype else 'market'
        created = int(data.datetime.datetime(0).timestamp()*1000)
        # Extract CCXT specific params if passed to the order
        params = params['params'] if 'params' in params else params
        if not self.use_order_params:
            ret_ord = self.store.create_order(symbol=data.p.dataname, order_type=order_type, side=side,
                                              amount=formatted_amount, price=formatted_price, params={})
        else:
            try:
                # all params are exchange specific: https://github.com/ccxt/ccxt/wiki/Manual#custom-order-params
                params['clientOrderId'] = created  # Add timestamp of order creation for backtesting
                ret_ord = self.store.create_order(symbol=data.p.dataname, order_type=order_type, side=side,
                                                  amount=formatted_amount, price=formatted_price, params=params)
            except:
                # save some API calls after failure
                self.use_order_params = False
                return None

        # bug fix: there is a chance create_order returned with a closed order,\
        # or open order with trades, re-fetch order may lose trades \
        # old way:
        # _order = self.store.fetch_order(ret_ord['id'], data.p.dataname)
        # order = CCXTOrder(owner, data, _order)

        # new way:
        order = CCXTOrder(owner, data, ret_ord)
        order.price = ret_ord['price']
        order.dt = ret_ord['datetime']

        # Check for new fills
        if 'trades' in ret_ord and ret_ord['trades'] is not None:
            for fill in ret_ord['trades']:
                if fill not in order.executed_fills:
                    order.execute(fill['datetime'], fill['amount'], fill['price'],
                                    0, 0.0, 0.0,
                                    0, 0.0, 0.0,
                                    0.0, 0.0,
                                    0, 0.0)
                    order.executed_fills.append(fill['id'])

        if self.debug:
            log.debug(json.dumps(ret_ord, indent=self.indent))

        # Check if the order is closed
        if ret_ord[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:

            """
             futures trading doesn't return trades inside orders, 
             so update order execution in one run
             """
            if self.store.get_type() == 'future':
                order.execute(ret_ord['datetime'], ret_ord['amount'], ret_ord['price'],
                                0, 0.0, 0.0,
                                0, 0.0, 0.0,
                                0.0, 0.0,
                                0, 0.0)

            pos = self.getposition(order.data, clone=False)
            pos.update(order.size, order.price)
            order.completed()
            self.notify(order)
            self.get_balance()
            return order # this order is not added into open order queue

        # if not closd, add into open order queue
        self.open_orders.append(order)

        self.notify(order)
        return order

    def buy(self, owner, data, size, price=None, plimit=None,
            exectype=None, valid=None, tradeid=0, oco=None,
            trailamount=None, trailpercent=None,
            **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        return self._submit(owner, data, exectype, 'buy', size, price, kwargs)

    def sell(self, owner, data, size, price=None, plimit=None,
             exectype=None, valid=None, tradeid=0, oco=None,
             trailamount=None, trailpercent=None,
             **kwargs):
        del kwargs['parent']
        del kwargs['transmit']
        return self._submit(owner, data, exectype, 'sell', size, price, kwargs)

    def cancel(self, order):

        oID = order.ccxt_order['id']

        if self.debug:
            print('Broker cancel() called')
            print('Fetching Order ID: {}'.format(oID))

        # check first if the order has already been filled otherwise an error
        # might be raised if we try to cancel an order that is not open.
        ccxt_order = self.store.fetch_order(oID, order.data.p.dataname)

        if self.debug:
            print(json.dumps(ccxt_order, indent=self.indent))

        if ccxt_order[self.mappings['closed_order']['key']] == self.mappings['closed_order']['value']:
            return order

        ccxt_order = self.store.cancel_order(oID, order.data.p.dataname)

        if self.debug:
            print(json.dumps(ccxt_order, indent=self.indent))
            print('Value Received: {}'.format(ccxt_order[self.mappings['canceled_order']['key']]))
            print('Value Expected: {}'.format(self.mappings['canceled_order']['value']))

        if ccxt_order[self.mappings['canceled_order']['key']] == self.mappings['canceled_order']['value']:
            self.open_orders.remove(order)
            order.cancel()
            self.notify(order)
        return order

    def get_orders_open(self, safe=False):
        return self.store.fetch_open_orders()

    def check_exchange_limit(self, data, amount, price):
        '''
        https://github.com/ccxt/ccxt/wiki/Manual#precision-and-limits
        check exchange limit met before issue an order

        Order amount >= limits['amount']['min']
        Order amount <= limits['amount']['max']
        Order price >= limits['price']['min']
        Order price <= limits['price']['max']
        Order cost (amount * price) >= limits['cost']['min']
        Order cost (amount * price) <= limits['cost']['max']
        Precision of amount must be <= precision['amount']
        Precision of price must be <= precision['price']
        '''
        symbol = data.p.dataname
        limits = self.markets[symbol]['limits']
        if limits['amount']['min'] != None and amount < limits['amount']['min']:
            print(f"{symbol} ORDER NOT SENT: AMOUNT({amount}) BELOW EXCHANGE LIMIT:{limits['amount']['min']}")
            return False

        if limits['amount']['max'] != None and amount > limits['amount']['max']:
            print(f"{symbol} ORDER NOT SENT: AMOUNT({amount}) EXCEED EXCHANGE LIMIT:{limits['amount']['max']}")
            return False

        if price != None:
            if limits['price']['min'] != None and price < limits['price']['min']:
                print(f"{symbol} ORDER NOT SENT: PRICE({price}) BELOW EXCHANGE LIMIT:{limits['price']['min']}")
                return False
            if limits['price']['max'] != None and price > limits['price']['max']:
                print(f"{symbol} ORDER NOT SENT: PRICE({price}) EXCEED EXCHANGE LIMIT:{limits['price']['max']}")
                return False

        cost = abs(amount) * data.close[0]
        if limits['cost']['min'] != None and cost < limits['cost']['min']:
            print(f"{symbol} ORDER NOT SENT: VALUE({cost}) BELOW EXCHANGE LIMIT:{limits['cost']['min']}")
            return False
        if limits['cost']['max'] != None and cost > limits['cost']['max']:
            print(f"{symbol} ORDER NOT SENT: VALUE({cost}) EXCEED EXCHANGE LIMIT:{limits['cost']['max']}")
            return False

        value = self.get_value()
        if cost >= value:
            print(f"{symbol} ORDER NOT SENT: VALUE({cost} EXCEED THE TOTAL PORTFOLIO VALUE({value}))")
            return False

        cash = self.get_cash()
        if cost >= cash:
            print(f"{symbol} ORDER NOT SENT: VALUE({cost} EXCEED THE REMAINING CASH({cash}))")
            return False

        return True

    def private_end_point(self, type, endpoint, params, prefix = ""):
        '''
        Open method to allow calls to be made to any private end point.
        See here: https://github.com/ccxt/ccxt/wiki/Manual#implicit-api-methods

        - type: String, 'Get', 'Post','Put' or 'Delete'.
        - endpoint = String containing the endpoint address eg. 'order/{id}/cancel'
        - Params: Dict: An implicit method takes a dictionary of parameters, sends
          the request to the exchange and returns an exchange-specific JSON
          result from the API as is, unparsed.
        - Optional prefix to be appended to the front of method_str should your
          exchange needs it. E.g. v2_private_xxx

        To get a list of all available methods with an exchange instance,
        including implicit methods and unified methods you can simply do the
        following:

        print(dir(ccxt.hitbtc()))
        '''
        endpoint_str = endpoint.replace('/', '_')
        endpoint_str = endpoint_str.replace('{', '')
        endpoint_str = endpoint_str.replace('}', '')

        if prefix != "":
            method_str = prefix.lower() + '_private_' + type.lower() + endpoint_str.lower()
        else:
            method_str = 'private_' + type.lower() + endpoint_str.lower()

        return self.store.private_end_point(type=type, endpoint=method_str, params=params)
