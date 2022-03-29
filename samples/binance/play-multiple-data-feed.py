from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime
import pytz

import json
import os
import time
from datetime import datetime, timedelta

import backtrader as bt

from ccxtbt import CCXTStore
import signal


class TestStrategy(bt.Strategy):

    def _print_current_bar_price(self, data):
        txt = list()
        txt.append(data._name)
        txt.append('%04d' % len(data))
        txt.append('%s' % datetime.now().time())
        txt.append('%s' % data.datetime.datetime(0))
        txt.append('{}'.format(data.open[0]))
        txt.append('{}'.format(data.high[0]))
        txt.append('{}'.format(data.low[0]))
        txt.append('{}'.format(data.close[0]))
        txt.append('{}'.format(data.volume[0]))
        print(', '.join(txt))

    def __init__(self):
        self._count = [0] * len(self.datas)
        signal.signal(signal.SIGINT, self.sigstop)


    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, data._name, ' data is ', data._getstatusname(status), *args)

    def notify_store(self, msg, *args, **kwargs):
        print('*' * 5, 'STORE NOTIF:', msg)

    def prenext(self):
        # call next() even when data is not available for all tickers
        self.next()

    def next(self):
        # run on the symbols
        for i, d in enumerate(self.datas):
            if len(d) > self._count[i]:
                self._count[i] = len(d)
                self._print_current_bar_price(d)

    def sigstop(self, a ,b):
        print('Stopping Backtrader')
        self.env.runstop()

if __name__ == '__main__':

    # absolute dir the script is in
    script_dir = os.path.dirname(__file__)
    abs_file_path = os.path.join(script_dir, '../params-production.json')
    with open(abs_file_path, 'r') as f:
        params = json.load(f)

    cerebro = bt.Cerebro(quicknotify=True)

    # Create our store
    config = {'apiKey': params["binance"]["apikey"],
              'secret': params["binance"]["secret"],
              'enableRateLimit': True,
              'options': {  # Futures Trading
                  'defaultType': 'future',
              },
              'nonce': lambda: str(int(time.time() * 1000)),
              }

    store = CCXTStore(exchange='binance', currency='USDT', config=config, retries=5, debug=True, sandbox=False)

    # Get the broker and pass any kwargs if needed.
    # ----------------------------------------------
    # Broker mappings have been added since some exchanges expect different values
    # to the defaults. Case in point, Kraken vs Bitmex. NOTE: Broker mappings are not
    # required if the broker uses the same values as the defaults in CCXTBroker.
    broker_mapping = {
        'order_types': {
            bt.Order.Market: 'market',
            bt.Order.Limit: 'limit',
            bt.Order.Stop: 'stop-loss',  # stop-loss for kraken, stop for bitmex
            bt.Order.StopLimit: 'stop limit'
        },
        'mappings': {
            'closed_order': {
                'key': 'status',
                'value': 'closed'
            },
            'canceled_order': {
                'key': 'status',
                'value': 'canceled'
            }
        }
    }

    broker = store.getbroker(broker_mapping=broker_mapping)
    cerebro.setbroker(broker)

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Get our data
    # Drop newest will prevent us from loading partial data from incomplete candles
    hist_start_date = datetime.utcnow() - timedelta(minutes=50)
    data = store.getdata(dataname='LUNA/USDT', name="LUNAUSDT",
                         timeframe=bt.TimeFrame.Minutes, fromdate=hist_start_date,
                         compression=1, ohlcv_limit=50, drop_newest=True)  # , historical=True)

    hist_start_date = datetime.utcnow() - timedelta(minutes=50)
    symbols = ["PEOPLE/USDT", "LUNA/USDT", "BNB/USDT", "BTC/USDT"]
    #symbols = ["PEOPLE/USDT"]
    for s in symbols:
        data = store.getdata(dataname=s,
                             tz=pytz.timezone('UTC'),
                             timeframe=bt.TimeFrame.Minutes,
                             fromdate=hist_start_date,
                             compression=1,
                             ohlcv_limit=50,
                             drop_newest=True,
                             qcheck=0.001)
        cerebro.adddata(data, name=s)

    # Run over everything
    cerebro.run()

    #Finally plot the end results
    #cerebro.plot(style='candlestick')
    cerebro.plot()