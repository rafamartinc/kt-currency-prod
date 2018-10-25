#!/usr/bin/env python

"""
Loads currency data from APIs and sends it to Kafka.

This script is meant to be a Kafka producer, responsible for retrieving
data about a currency's value in real time, and loading it into the system.
Will hold connection methods to several APIs, and will be responsible for
one specific currency.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

import datetime
import threading
import json
import time
import sys
from kafka import KafkaProducer

from apis.cryptocompareapi import CryptoCompareApi

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class KingstonProducer:

    def __init__(self, args):

        self._api = CryptoCompareApi()
        self._kafka_producer = self._connect_kafka_producer(args['kafka_host'], args['kafka_port'])

        self._symbol = args['symbol']
        self._reference = args['reference']
        self._sleep = args['sleep']

        if args['rollback'] != 0:
            print('[INFO] Creating separate thread to load historical data...')
            thread = threading.Thread(target=self._store_historical_prices(), args=())
            thread.start()

        while True:
            self._store_current_price()
            time.sleep(self._sleep)

    def _store_current_price(self):
        results = self._api.price(self._reference, [self._symbol])
        for k in results:
            results[k] = 1 / results[k]

        document = {
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            'currency': self._symbol,
            'value': results[self._symbol],
            'reference_currency': self._reference
        }
        print(document)
        self._kafka_producer.send('kt_currencies', value=json.dumps(document).encode('utf-8'))

    def _store_historical_prices(self):

        retrieve_from = int(datetime.datetime.utcnow().timestamp())
        continue_query = True

        while continue_query:
            results = self._api.histominute(self._reference, self._symbol, ts=retrieve_from)
            for result in results:
                document = {
                    'timestamp': datetime.datetime.fromtimestamp(float(result['time'])).isoformat() + '.000000Z',
                    'currency': self._symbol,
                    'value': result['close'],
                    'reference_currency': self._reference
                }
                retrieve_from = min(retrieve_from, result['time'])
    
            if len(results) < self._api.query_limit:
                continue_query = False

            print(datetime.datetime.fromtimestamp(retrieve_from).isoformat() + '.000000Z')

    @staticmethod
    def _connect_kafka_producer(kafka_host, kafka_port):
        producer = None
        print('[INFO] Connecting to Kafka...')
        try:
            producer = KafkaProducer(bootstrap_servers=[kafka_host + ':' + kafka_port])
        except Exception as ex:
            print('Exception while connecting Kafka.')
            print(str(ex))
        finally:
            print('[INFO] Connection with Kafka established.')
            return producer


def parse_args(args):

    args_dict = {}

    if len(args) % 2 != 0:
        raise Exception('Unexpected number of arguments.')
    else:
        new_key = ''
        for i in range(len(args)):
            if i % 2 == 0:
                new_key = args[i].lstrip('--')
            else:
                args_dict[new_key] = args[i]

    return args_dict


def check_args(args):

    mandatory_keys = ['symbol']
    optional_keys = ['reference', 'sleep', 'kafka_host', 'kafka_port', 'rollback']

    for k in mandatory_keys:
        if k not in args:
            raise Exception('Missing argument: ' + str(k) + '.')

    for k in args:
        if k not in mandatory_keys and k not in optional_keys:
            raise Exception('Unexpected argument: ' + str(k) + '.')

    if 'reference' not in args:
        args['reference'] = 'EUR'
    if 'kafka_host' not in args:
        args['kafka_host'] = 'localhost'
    if 'kafka_port' not in args:
        args['kafka_port'] = '9092'

    args['sleep'] = 5 if 'sleep' not in args else max(5, int(args['sleep']))

    max_minutes = 7 * 24 * 60
    if 'rollback' not in args:
        args['rollback'] = 0
    elif args['rollback'] == 'max':
        args['rollback'] = max_minutes
    else:
        args['rollback'] = int(args['rollback'])
        args['rollback'] = max(0, min(max_minutes, args['rollback']))


def main(args):

    try:

        args = parse_args(args)
        check_args(args)

    except Exception as ex:

        print(str(ex))
        print('Usage: python kt_currency_producer.py' +
              '\n       --symbol <to_symbol> (e.g.: USD)' +
              '\n       --reference <from_symbol> (optional, default: EUR)' +
              '\n       --sleep <seconds> (optional, minimum/default: 5)' +
              '\n       --kafka_host <ip> (optional, default: localhost)' +
              '\n       --kafka_port <port> (optional, default: 9092)'
              '\n       --rollback <minutes> (optional, default/max: 7 days')

    else:

        KingstonProducer(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
