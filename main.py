from urllib import request, error
import datetime
import json
import time


class CryptoApi:

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

        result = {} # Default return value.

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


def histominute_dict_to_dataset(dataset, dict):
    result = None
    for data in dict['Data']:

        if result is None:
            result = int(data['time']) - 60

        dataset[float(data['time'])] = {

            'open': float(data['open']),
            'close': float(data['close']),
            'high': float(data['high']),
            'low': float(data['low']),
            'volumefrom': float(data['volumefrom']),
            'volumeto': float(data['volumeto'])
        }

    return result


def insert_to_dataset(dataset, input_file):
    data = input_file.readline()

    while data:
        data = data.split(";")

        dataset[float(data[0])] = {

            'open': float(data[1]),
            'close': float(data[2]),
            'high': float(data[3]),
            'low': float(data[4]),
            'volumefrom': float(data[5]),
            'volumeto': float(data[6])
        }

        data = input_file.readline()


api = CryptoApi()
realConnection = True
from_symbol = "EUR"
to_symbol = ["ETH", "XRP", "LTC", "NEO", "XMR", "BCH", "BTC"]
ds = {}

while True:
    results = api.price(from_symbol, to_symbol)
    for k in results:
        results[k] = 1 / results[k]

    print({datetime.datetime.utcnow().isoformat() + 'Z': results})

    time.sleep(10)


filename = from_symbol.lower() + to_symbol.lower() + ".csv"
try:
    f = open(filename)
except IOError:
    print("[WARNING] File not found when trying to read the data.")
else:
    f.readline()
    insert_to_dataset(ds, f)

if realConnection:
    # If real Internet connection is allowed, download data using the API.
    dict = api.histominute(from_symbol, to_symbol)
    previousTimestamp = histominute_dict_to_dataset(ds, dict)

    keyArray = ds.keys()
    while previousTimestamp is not None:

        if previousTimestamp in keyArray:
            previousTimestamp -= 60

        else:
            dict = api.histominute(from_symbol, to_symbol, ts=previousTimestamp)
            previousTimestamp = histominute_dict_to_dataset(ds, dict)
            keyArray = ds.keys()

    f = open(filename, "w")
    f.write("Timestamp;Open;Close;High;Low;Volume From;Volume To\n")

    for t in sorted(ds.keys()):
        str_output = str(t) + ";" + \
                str(ds[t]['open']) + ";" + \
                str(ds[t]['close']) + ";" + \
                str(ds[t]['high']) + ";" + \
                str(ds[t]['low']) + ";" + \
                str(ds[t]['volumefrom']) + ";" + \
                str(ds[t]['volumeto']) + "\n"
        f.write(str_output)
        print(str_output)

print(str(len(ds)) + " records loaded.")
"""for timestamp in sorted(dataset.keys()):
    #print str(datetime.datetime.fromtimestamp(timestamp)) + " ___ " + str(dataset[timestamp])
    print str(timestamp) + " ___ " + str(dataset[timestamp])"""
