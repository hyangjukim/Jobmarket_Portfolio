from DataProcessor.buildDB_yfinance_injection import Stock_Time_Series_YF, Option_Time_Series_YF
from pymongo import MongoClient
from Utilities.data_manipulation import data_manipulation
import settings
import datetime

class yfinance_daily_data_injection:

    def __init__(self):

        self.injected_stocks = None
        self.injected_options = None

    def get_names_available_data(self):
        inst_data = data_manipulation()
        collection_names = inst_data.collection_names

        list_of_pairs = []
        for name in collection_names:
            if name.endswith('_options'):
                try:
                    pair = [name]
                    stock_name = name.split('_')[0]
                    pair.append(stock_name)

                    # To-do: grab actual data from DB
                    list_of_pairs.append(pair)
                except:
                    print('No stock corresponding to {} found'.format(name))
                    continue
        return list_of_pairs

    def check_imported_stock_data(self, end_date):
        tickers = self.get_names_available_data()
        inst = data_manipulation()

        missing_list = []

        # check if the first three days of stock data is missing
        # if yes, obtain the past week stock tick data
        start_date = end_date + datetime.timedelta(days=-1)

        for ticker in tickers:
            pd_tick_data = inst.import_stock_tick_data(ticker[1], start_date, end_date)
            if pd_tick_data.empty:
                missing_list.append(ticker[1])
        return missing_list


    def inject_missing_stock_tick_data(self, end_date = datetime.datetime.now()):
        missing_list = self.check_imported_stock_data(end_date)

        inst_stocks = Stock_Time_Series_YF(tickers=missing_list, interval='1m', period='1d')
        record_injected_stocks = inst_stocks.inject_time_series_db()
        print(record_injected_stocks)
        return record_injected_stocks



    def add_injected_lists_to_DB(self):
        db_spec = settings.db_settings['price_time_series']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]
        collection = db['injected_tickers']

        record = dict()
        today = datetime.datetime.now().date()
        injection_date = today + datetime.timedelta(days=-1)
        timestamp_str = injection_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")

        record['injection_date'] = timestamp_str
        record['stocks'] = self.injected_stocks
        record['options'] = self.injected_options

        pid = collection.insert_one(record).inserted_id
        return pid


    def execute_injection(self):

        # inject daily stock data
        inst_stocks = Stock_Time_Series_YF()
        record_stocks = inst_stocks.inject_time_series_db()
        self.injected_stocks = record_stocks

        # inject daily option data
        inst_options = Option_Time_Series_YF()
        record_options = inst_options.inject_wrapper()
        self.injected_options = record_options

        # check if any stock tick data is missing and retry if needed
        record_reinjected_stocks = self.inject_missing_stock_tick_data()
        self.injected_stocks = self.injected_stocks + record_reinjected_stocks
        return 0

    def tasks(self):
        start_time = datetime.datetime.now()
        self.execute_injection()
        self.add_injected_lists_to_DB()
        end_time = datetime.datetime.now()
        time_diff = end_time - start_time
        return time_diff


def main():
    inst = yfinance_daily_data_injection()
    time_elapsed = inst.tasks()
    print(inst.injected_stocks)
    print(inst.injected_options)
    print('Process took {} time'.format(time_elapsed))

if __name__== "__main__":
    main()