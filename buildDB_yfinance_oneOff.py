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
    for name in collection_names:
        collection = db[name]
        tickers = collection.distinct('Symbol') + tickers

    return tickers

class Build_InformationDB_YF:

    def __init__(self, tickers = None, collection_name = None):

        if tickers is None:
            self.tickers = get_tickers(collection_name)
        else:
            if isinstance(tickers, list) == False:
                tickers = [tickers]
            self.tickers = tickers

    def inject_info_db(self):
        db_spec = settings.db_settings['company_info']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]
        collection = db[db_spec['collection_name']]

        successes = []
        for ticker in self.tickers:

            try:
                inst = yf.Ticker(ticker)
                information = inst.info
                information['symbol'] = ticker
                pid = collection.insert_one(information).inserted_ids
                successes.append(ticker)
            except:
                continue
        time_now = datetime.datetime.now()
        record_success = {'type':'record',
                          'timestamp': time_now,
                          'successful_tickers': successes
                          }
        pid = collection.insert_one(record_success).inserted_id
        return successes


class Build_Time_Series_YF:

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
                time_series = inst.history(period = 'max')
                #time_series = inst.history(interval='2m', start='2020-03-01', end='2020-04-03')
                time_series = time_series.reset_index()
                dtime_series = time_series.to_dict('records')

                collection = db[ticker]
                pid = collection.insert_many(dtime_series).inserted_ids
                successes.append(ticker)
            except:
                continue

        time_now = datetime.datetime.now()
        record_success = {'type': 'record',
                          'timestamp': time_now,
                          'successful_tickers': successes
                          }
        pid = collection.insert_one(record_success).inserted_id
        return successes


def main():
    indices = settings.index_tickers

    inst = Build_Time_Series_YF(tickers = indices)
    #inst = Build_Time_Series_YF()
    record = inst.inject_time_series_db()
    print(record)

    #inst = Build_InformationDB_YF(tickers = indices)
    #record = inst.inject_info_db()
    #print(record)

if __name__== "__main__":
    main()



