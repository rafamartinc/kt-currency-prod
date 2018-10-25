#!/usr/bin/env python

"""
Loads currency data from APIs.

This script is responsible for retrieving data about a currency's value
in real time, as well as historical data if needed. Will hold connection
methods to several APIs, and will be responsible for one specific currency.

This file is subject to the terms and conditions defined in the file
'LICENSE.txt', which is part of this source code package.
"""

from urllib import request
import json

__author__ = "Rafael Martín-Cuevas, Rubén Sainz"
__credits__ = ["Rafael Martín-Cuevas", "Rubén Sainz"]
__version__ = "0.1.0"
__status__ = "Development"


class CryptoCompareApi:

    def __init__(self):
        self.__url = "https://min-api.cryptocompare.com/data/"

    def histominute(self, fsym, tsym, sign=False, try_conversion=False,
                    exchange="CCCAGG", aggregate=1, limit=1440, ts=None):
        """
        Get open, high, low, close, volumefrom and volumeto from the each minute historical
        data. This data is only stored for 7 days, if you need more, use the hourly or daily
        path. It uses BTC conversion if data is not available because the coin is not
        trading in the specified currency

        :param fsym: String (required). From symbol (currency).
        :param tsym: String (required). To symbols (currencies).
        :param sign: Boolean. If set to true, the server will sign the requests.
        :param try_conversion: Boolean. If set to true, it will try to get the missing
                values by converting currencies to BTC.
        :param exchange: Exchange data will be loaded from.
        :param aggregate: Integer. Number of minutes represented by each entry of the
                resulting data.
        :param limit: Integer. Indicates de maximum amount of entries to return, up to
                1440 per request (24h).
        :param ts: Timestamp. Timestamp from which to load the data (backwards).
        :return: Resulting data, as a dictionary.
        """

        result = {}  # Default return value.

        # Build URL needed to request data from the server.
        url = self.__url + "histominute?" + \
              "fsym=" + str(fsym) + "&tsym=" + str(tsym) + "&sign=" + str(sign) + \
              "&tryConversion=" + str(try_conversion) + "&aggregate=" + str(aggregate) + \
              "&limit=" + str(limit) + "&e=" + str(exchange)

        # Add the value 'ts', only if specified.
        if ts:
            url += "&toTs=" + str(ts)

        # Connect and request data.
        with request.urlopen(url) as response:

            html = response.read()

            try:
                # Convert to Python dictionary.
                result = json.loads(html)

                if result.has_key("Response") and result.has_key("Message"):
                    if result["Response"] == "Error":
                        print("< Error > " + str(result["Message"]))

            except ValueError as e:
                print("< ValueError > " + str(e))

        return result

    def price(self, fsym, tsyms, sign=False, try_conversion=False, exchange="CCCAGG"):
        """
        Get open, high, low, close, volumefrom and volumeto from the each minute historical
        data. This data is only stored for 7 days, if you need more, use the hourly or daily
        path. It uses BTC conversion if data is not available because the coin is not
        trading in the specified currency

        :param fsym: String (required). From symbol (currency).
        :param tsyms: List (required). To symbols (currencies).
        :param sign: Boolean. If set to true, the server will sign the requests.
        :param try_conversion: Boolean. If set to true, it will try to get the missing
                values by converting currencies to BTC.
        :param exchange: Exchange data will be loaded from.
        :return: Resulting data, as a dictionary.
        """

        result = {}  # Default return value.

        # Build URL needed to request data from the server.
        url = self.__url + "price?" + \
              "fsym=" + str(fsym) + "&tsyms=" + ",".join(tsyms) + "&sign=" + str(sign) + \
              "&tryConversion=" + str(try_conversion) + "&e=" + str(exchange)

        # Connect and request data.
        with request.urlopen(url) as response:

            html = response.read()

            try:
                # Convert to Python dictionary.
                result = json.loads(html)

            except ValueError as e:
                print("< ValueError > " + str(e))

        return result
