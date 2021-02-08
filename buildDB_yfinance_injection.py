# Daily data injection

import yfinance as yf
import settings
from pymongo import MongoClient
import datetime

# get tickers from the database
# input should be the collection name i.e:
# companylist_amex, companylist_nasdaq, companylist_nyse
# if you need multiple, pass in a list
def get_tickers(collection_name = None):
    db_spec = settings.db_settings['price_time_series']
    client = MongoClient(db_spec['host'], db_spec['port'])
    db = client[db_spec['db_name']]

    if collection_name is None:
        collection_names = settings.db_settings['ticker_list']['collection_names']
    elif isinstance(collection_name, list):
        collection_names = collection_name
    else:
        collection_names = [collection_name]

    tickers = settings.index_tickers
    tickers = tickers + settings.additional_tickers

    for name in collection_names:
        collection = db[name]
        tickers = collection.distinct('Symbol') + tickers


    return tickers


class Stock_Time_Series_YF:

    def __init__(self, tickers = None, interval='1m', period='1d', start_date = None, end_date = None, collection_name = None):
        if tickers is None:
            self.tickers = get_tickers(collection_name)
        else:
            if isinstance(tickers, list) == False:
                tickers = [tickers]
            self.tickers = tickers

        self.interval = interval
        self.period = period
        self.start_date = start_date
        self.end_date = end_date

    def inject_time_series_db(self):
        db_spec = settings.db_settings['price_time_series']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]

        successes = []
        for ticker in self.tickers:

            try:
                inst = yf.Ticker(ticker)

                if self.start_date and self.end_date is not None:
                    time_series = inst.history(start=self.start_date, end=self.end_date)
                else:
                    time_series = inst.history(interval=self.interval, period= self.period)

                time_series = time_series.reset_index()
                dtime_series = time_series.to_dict('records')

                collection = db[ticker]
                pid = collection.insert_many(dtime_series).inserted_ids
                successes.append(ticker)
            except:
                continue

        return successes


class Option_Time_Series_YF:

    def __init__(self, tickers = None, collection_name = None):

        if tickers is None:
            self.tickers = get_tickers(collection_name)
        else:
            if isinstance(tickers, list) == False:
                tickers = [tickers]
            self.tickers = tickers

    def inject_time_series_db(self):
        db_spec = settings.db_settings['price_time_series']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]

        successes = []
        for ticker in self.tickers:

            try:
                inst = yf.Ticker(ticker)
                expiration_dates = inst.options

                collection = db[ticker + '_options']
                for expiry in expiration_dates:
                    try:
                        options = inst.option_chain(expiry)

                        call_options = options.calls
                        lcall_options = call_options.to_dict('records')
                        list_options = []
                        for record in lcall_options:
                            record['expiry'] = expiry
                            record['put_call'] = 'call'
                            record['insert_timestamp'] = datetime.datetime.now()
                            list_options.append(record)

                        put_options = options.puts
                        lput_options = put_options.to_dict('records')

                        for record in lput_options:
                            record['expiry'] = expiry
                            record['put_call'] = 'put'
                            record['insert_timestamp'] = datetime.datetime.now()
                            list_options.append(record)
                        pid = collection.insert_many(list_options).inserted_ids
                    except:
                        continue

                successes.append(ticker)
            except:
                continue

        return successes


def main():

    #inject daily stock data
    #inst_stocks = Stock_Time_Series_YF(tickers = 'AAPL', interval='1m', period='8d')
    #record_stocks = inst_stocks.inject_time_series_db()
    #print(record_stocks)

    #inject daily option data
    inst_options = Option_Time_Series_YF(tickers = ['AAPL', 'TSLA'])
    record_options = inst_options.inject_time_series_db()
    print(record_options)

if __name__== "__main__":
    main()