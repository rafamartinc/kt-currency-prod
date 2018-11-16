#!/usr/bin/env python

"""
Loads currency data from APIs and sends it to Kafka.

This script is meant to retrieve data about a currency's value in real
time, and loading it into the system. Will hold connection methods to
several APIs, and will be responsible for one specific currency.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

import sys

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"

from kingston_producer import KingstonProducer


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
    optional_keys = ['reference', 'sleep', 'kafka_host', 'kafka_port', 'kafka_topic', 'rollback']

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
    if 'kafka_topic' not in args:
        args['kafka_topic'] = 'kt_currencies'

    args['sleep'] = 5 if 'sleep' not in args else max(5, int(args['sleep']))

    max_minutes = 7 * 24 * 60
    if 'rollback' not in args or args['rollback'] == 'max':
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
        print('Usage: python main.py' +
              '\n       --symbol <to_symbol> (e.g.: USD)' +
              '\n       --reference <from_symbol> (optional, default: EUR)' +
              '\n       --sleep <seconds> (optional, default/min: 5)' +
              '\n       --kafka_host <ip> (optional, default: localhost)' +
              '\n       --kafka_port <port> (optional, default: 9092)' +
              '\n       --kafka_topic <topic> (optional, default: kt_currencies)' +
              '\n       --rollback <minutes> (optional, default/max: 7 days')

    else:

        KingstonProducer(args)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
