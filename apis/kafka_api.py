#!/usr/bin/env python

"""
Loads currency data into Kafka.

This script is meant to be an interface to Kafka, responsible for loading
messages into the system.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

from kafka import KafkaProducer
import json

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class CurrencyProducer:

    def __init__(self, kafka_host, kafka_port, kafka_topic):

        self.__kafka_host = kafka_host
        self.__kafka_port = kafka_port
        self.__kafka_topic = kafka_topic

        print('[INFO] Connecting to Kafka...')
        self.__producer = KafkaProducer(bootstrap_servers=[self.__kafka_host + ':' + self.__kafka_port])

    def send(self, document):
        self.__producer.send(self.__kafka_topic, value=json.dumps(document).encode('utf-8'))
