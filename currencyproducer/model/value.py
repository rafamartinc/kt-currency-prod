#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File description.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class MinuteValue:

    def __init__(self, timestamp, value, currency, reference_currency, api):

        self._timestamp = timestamp
        self._value = value
        self._currency = currency
        self._reference_currency = reference_currency
        self._api = api

    def to_json(self):

        return {
            'timestamp': self._timestamp,
            'value': self._value,
            'currency': self._currency,
            'reference_currency': self._reference_currency,
            'api': self._api
        }
