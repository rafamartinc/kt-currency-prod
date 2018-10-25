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
import json
import time
import sys
from kafka import KafkaProducer

from src.cryptoapi import CryptoApi

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class KingstonProducer:

    def __init__(self, symbol, reference='EUR', sleep=5):

        api = CryptoApi()
        kafka_producer = connect_kafka_producer()

        while True:
            results = api.price(reference, symbol)
            for k in results:
                results[k] = 1 / results[k]

            document = {datetime.datetime.utcnow().isoformat() + 'Z': results}
            print(document)
            kafka_producer.send('kt_currencies', value=json.dumps(document).encode('utf-8'))

            time.sleep(sleep)


def connect_kafka_producer():
    producer = None
    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda x: x,
                                 api_version=(0, 10))
    except Exception as ex:
        print('[ERROR] Exception while connecting Kafka')
        print(str(ex))
    finally:
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
    optional_keys = ['reference', 'sleep', 'kafka_host', 'kafka_port']

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
        args['kafka_port'] = '9200'
    args['sleep'] = 5 if 'sleep' not in args else max(5, int(args['sleep']))


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
              '\n       --kafka_port <port> (optional, default: 9200)'
              '\n       --rollback <minutes> (optional, default/max: 7 days')

    else:

        kp = KingstonProducer(args['symbol'], args['reference'], args['sleep'])


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
