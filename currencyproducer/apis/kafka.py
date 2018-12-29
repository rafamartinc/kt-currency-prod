#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Loads currency data into Kafka.

This script is meant to be an interface to Kafka, responsible for loading
messages into the system.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

from kafka import KafkaProducer
import json
import time

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class CurrencyProducer:

    def __init__(self, kafka_topic, kafka_servers='localhost:9092'):

        self._kafka_servers = kafka_servers
        self._kafka_topic = kafka_topic

        self._producer = None
        self._connect()

    def _connect(self):
        while self._producer is None:
            try:
                print('[INFO] Trying to connect to Kafka...')
                self._producer = KafkaProducer(bootstrap_servers=self._kafka_servers.split(','))
            except Exception as ex:
                print('Exception while connecting Kafka, retrying in 1 second')
                print(str(ex))

                self._producer = None
                time.sleep(1)
            else:
                print('[INFO] Connected to Kafka.')

    def send(self, document):

        sent = False

        while not sent:
            try:
                sent = self._producer.send(self._kafka_topic, value=json.dumps(document).encode('utf-8'))
            except Exception as ex:
                print('Exception while sending to Kafka.')
                print(str(ex))

                self._producer = None
                self._connect()

        return sent
