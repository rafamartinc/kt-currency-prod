#!/usr/bin/env python

"""
Loads currency data from APIs and sends it to Kafka.

This script is meant to retrieve data about a currency's value in real
time, and loading it into the system. Will hold connection methods to
several APIs, and will be responsible for one specific currency.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

import argparse
import sys

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"

from .kingston_producer import KingstonProducer


def main():

    parser = argparse.ArgumentParser(description='Produces currency values obtained from several APIs.')
    parser.add_argument('symbol',
                        type=str,
                        help='Currency for which the data is to be retrieved')
    parser.add_argument('-r', '--reference',
                        default='EUR',
                        type=str,
                        help='Currency on which the values are to be expressed (default: EUR)')
    parser.add_argument('-s', '--sleep',
                        default=5,
                        type=int,
                        help='Delay between measures (default/min.: 5s)')
    parser.add_argument('-b', '--rollback',
                        default=7 * 24 * 60,
                        type=int,
                        help='Minutes to roll back data from APIs (default/max.: 7d)')
    parser.add_argument('-k', '--kafka_host',
                        default='localhost',
                        type=str,
                        help='Kafka host (default: localhost)')
    parser.add_argument('-p', '--kafka_port',
                        default=9092,
                        type=int,
                        help='Kafka port (default: 9092)')
    parser.add_argument('-t', '--kafka_topic',
                        default='kt_currencies',
                        type=str,
                        help='Kafka topic to store data in (default: kt_currencies)')

    args = parser.parse_args()

    KingstonProducer(args.symbol, reference=args.reference, sleep=args.sleep, rollback=args.rollback,
                     kafka_host=args.kafka_host, kafka_port=args.kafka_port, kafka_topic=args.kafka_topic)


if __name__ == '__main__':
    sys.exit(main())
