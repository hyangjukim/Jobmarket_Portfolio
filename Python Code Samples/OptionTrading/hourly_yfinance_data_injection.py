from DataProcessor.buildDB_yfinance_injection import Option_Time_Series_YF
from pymongo import MongoClient
import settings
import datetime

class yfinance_hourly_data_injection:

    def __init__(self):

        self.injected_options = None


    def add_injected_lists_to_DB(self):
        db_spec = settings.db_settings['price_time_series']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]
        collection = db['injected_tickers']

        record = dict()
        today = datetime.datetime.now().date()
        injection_date = today
        timestamp_str = injection_date.strftime("%d-%b-%Y (%H:%M:%S.%f)")

        record['injection_date'] = timestamp_str
        record['options'] = self.injected_options

        pid = collection.insert_one(record).inserted_id
        return pid


    def execute_injection(self):

        # inject daily option data
        inst_options = Option_Time_Series_YF()
        record_options = inst_options.inject_wrapper()
        self.injected_options = record_options

        return 0

    def tasks(self):
        start_time = datetime.datetime.now()
        self.execute_injection()
        self.add_injected_lists_to_DB()
        end_time = datetime.datetime.now()
        time_diff = end_time - start_time
        return time_diff


def main():
    inst = yfinance_hourly_data_injection()
    time_elapsed = inst.tasks()
    print(inst.injected_options)
    print('Process took {} time'.format(time_elapsed))

if __name__== "__main__":
    main()