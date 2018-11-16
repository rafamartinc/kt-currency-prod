#!/usr/bin/env python

"""
Loads currency data from APIs and sends it to Kafka.

This class is meant to be a Kafka producer, responsible for retrieving
data about a currency's value in real time, and loading it into the system.
Will hold connection methods to several APIs, and will be responsible for
one specific currency.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

import datetime
import threading
import time

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"

from apis.cryptocompareapi import CryptoCompareApi
from apis.kafka_api import CurrencyProducer


class KingstonProducer:

    def __init__(self, args):

        self.__api = CryptoCompareApi()

        self.__symbol = args['symbol']
        self.__reference = args['reference']
        self.__sleep = args['sleep']

        try:
            self.__kafka_producer = CurrencyProducer(args['kafka_host'], args['kafka_port'], args['kafka_topic'])

        except Exception as ex:
            print('Exception while connecting Kafka.')
            print(str(ex))

        else:
            if args['rollback'] != 0:
                print('[INFO] Initializing rollback...')
                self.__historical_prices()

            print('[INFO] Initializing retrieval of current prices...\n')
            while True:
                self.__store_current_price()
                time.sleep(self.__sleep)

    def __historical_prices(self):

        retrieve_from = int(datetime.datetime.utcnow().timestamp())
        continue_query = True
        results = []

        while continue_query:
            batch = self.__api.histominute(self.__reference, self.__symbol, ts=retrieve_from)
            for i in reversed(range(len(batch))):
                row = batch[i]
                document = {
                    'timestamp': datetime.datetime.fromtimestamp(float(row['time'])).isoformat() + '.000000Z',
                    'currency': self.__symbol,
                    'value': 1 / row['close'],
                    'reference_currency': self.__reference
                }
                results.insert(0, document)
                retrieve_from = min(retrieve_from, row['time'])

            print('[INFO] ' + str(len(batch)) + ' historical prices loaded from the API.')

            if len(batch) < self.__api.query_limit:
                continue_query = False

        for document in results:
            self.__kafka_producer.send(document)
        print('[INFO] Historical prices sent to Kafka.')

    def __store_current_price(self):
        results = self.__api.price(self.__reference, [self.__symbol])
        for k in results:
            results[k] = 1 / results[k]

        document = {
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'currency': self.__symbol,
            'value': results[self.__symbol],
            'reference_currency': self.__reference
        }
        print(document)
        self.__kafka_producer.send(document)
