#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import timeit
import time

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"

from .apis.cryptocompare import CryptoCompareApi
from .apis.kafka import CurrencyProducer
from .model.value import MinuteValue


class KingstonProducer:

    def __init__(self, symbol, reference='EUR', sleep=5, rollback=7*24*60,
                 kafka_servers='localhost:9092', kafka_topic='kt_currencies'):

        self._api = CryptoCompareApi()

        self._symbol = symbol
        self._reference = reference
        self._sleep = sleep
        self._kafka_producer = CurrencyProducer(kafka_topic, kafka_servers)

        if rollback != 0:
            print('[INFO] Initializing rollback...')
            self._historical_prices()

        print('[INFO] Initializing retrieval of current prices...\n')
        self._current_prices()

    def _historical_prices(self):

        retrieve_from = int(datetime.datetime.utcnow().timestamp())
        continue_query = True
        results = []

        while continue_query:
            try:
                batch = self._api.histominute(self._reference, self._symbol, ts=retrieve_from)
            except Exception as ex:
                print(str(ex))
                continue_query = False
            else:
                for i in reversed(range(len(batch))):
                    row = batch[i]
                    results.insert(0, MinuteValue(
                        timestamp=datetime.datetime.fromtimestamp(float(row['time'])).isoformat() + '.000000Z',
                        value=1 / row['close'],
                        currency=self._symbol,
                        reference_currency=self._reference,
                        api='CCCAGG'
                    ).to_json())
                    retrieve_from = min(retrieve_from, row['time'])

                print('[INFO] ' + str(len(batch)) + ' historical prices loaded from the API.')

                if len(batch) < self._api.query_limit:
                    continue_query = False

        print('>' + results[0])
        for document in results:
            self._kafka_producer.send(document)
        print('[INFO] Historical prices sent to Kafka.')

    def _current_prices(self):

        time_marker = timeit.default_timer()
        previous_marker = time_marker - self._sleep

        while True:
            self._store_current_price()

            diff = time_marker - (previous_marker + self._sleep)
            time.sleep(max(0, int(self._sleep - diff)))
            
            previous_marker = time_marker
            time_marker = timeit.default_timer()

    def _store_current_price(self):

        results = self._api.price(self._reference, [self._symbol])
        for k in results:
            results[k] = 1 / results[k]

        document = MinuteValue(
            timestamp=datetime.datetime.utcnow().isoformat() + 'Z',
            value=results[self._symbol],
            currency=self._symbol,
            reference_currency=self._reference,
            api='CCCAGG'
        ).to_json()

        if self._kafka_producer.send(document):
            print(document)
