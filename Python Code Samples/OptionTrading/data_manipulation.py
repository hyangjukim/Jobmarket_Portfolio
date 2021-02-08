# this class should be inherited or imported to all strategies
# basically giving functionalities to manipulate data from the MongoDB
import settings
from pymongo import MongoClient
import datetime
import pandas as pd
from Utilities.visualization import Visualize

# insert the list of tickers in the following format:
class data_manipulation:

    def __init__(self, start_date=None, end_date=None, tickers=None):
        if start_date is not None:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.now() + datetime.timedelta(days=-365)

        if end_date is not None:
            self.end_date = end_date
        else:
            self.end_date = datetime.datetime.now()

        self.collection_names = None
        self.db = self.connect_db()

        if tickers is not None:
            self.tickers = tickers
        else:
            self.tickers = self.get_names_available_data()


    def connect_db(self):
        db_spec = settings.db_settings['price_time_series']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]

        self.collection_names = db.list_collection_names()
        return db

    def get_names_available_data(self):
        list_of_pairs = []
        for name in self.collection_names:
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

    def import_stock_all_data(self, ticker, start=None, end=None):

        if start is None: start = self.start_date
        if end is None: end = self.end_date

        collection = self.db[ticker]
        data = []
        for doc_date in collection.find({'Date': {"$gt": start, "$lt": end}}):
            data.append(doc_date)

        for doc_datetime in collection.find({'Datetime': {'$gt': start, '$lt': end}}):
            data.append(doc_datetime)

        pd_data = pd.DataFrame(data)

        # merge date into datetime
        pd_data['Datetime'] = pd_data['Datetime'].fillna(pd_data['Date'])
        pd_data = pd_data.drop(columns=['Date'])
        return pd_data

    def import_stock_daily_data(self, ticker, start=None, end=None):

        if start is None: start = self.start_date
        if end is None: end = self.end_date

        collection = self.db[ticker]
        data = []
        for doc_date in collection.find({'Date': {"$gt": start, "$lt": end}}):
            data.append(doc_date)

        pd_data = pd.DataFrame(data)
        return pd_data

    def import_stock_tick_data(self, ticker, start=None, end=None):

        if start is None: start = self.start_date
        if end is None: end = self.end_date

        collection = self.db[ticker]
        data = []
        for doc_date in collection.find({'Datetime': {"$gt": start, "$lt": end}}):
            data.append(doc_date)

        pd_data = pd.DataFrame(data)
        return pd_data


    def import_option_daily_data(self, ticker, start=None, end=None):

        if start is None: start = self.start_date
        if end is None: end = self.end_date

        collection = self.db[ticker + '_options']
        option_data = []
        for doc_date in collection.find({'lastTradeDate': {"$gt": start, "$lt": end}}):
            option_data.append(doc_date)

        pd_option_data = pd.DataFrame(option_data)
        return pd_option_data


    def get_closest_stock_price(self, ticker, date_time):
        collection = self.db[ticker]
        stock_data = []
        stock_data_fallback = []
        start = date_time + datetime.timedelta(days=-4)
        end = date_time + datetime.timedelta(days=1)

        for doc_date in collection.find({'Datetime': {"$gt": start, "$lt": end}}):
            stock_data.append(doc_date)

        if not stock_data:
            for doc_date in collection.find({'Date': {"$gt": start, "$lt": end}}):
                stock_data_fallback.append(doc_date)

            if not stock_data_fallback:
                print ('There is no historical stock data for {}'.format(ticker))
                return None, None

            pd_stock_data_fallback = pd.DataFrame(stock_data_fallback)
            index_fallback = abs(pd_stock_data_fallback['Date'] - date_time).idxmin()
            stock_price_fallback = pd_stock_data_fallback.iloc[index_fallback]['Close']
            selected_datetime_fallback = pd_stock_data_fallback.iloc[index_fallback]['Date']
            return stock_price_fallback, selected_datetime_fallback

        else:
            pd_stock_data = pd.DataFrame(stock_data)
            index = abs(pd_stock_data['Datetime'] - date_time).idxmin()
            stock_price = pd_stock_data.iloc[index]['Close']
            selected_datetime = pd_stock_data.iloc[index]['Datetime']

            return stock_price, selected_datetime


    # this function needs to have a close to accurate trade_date
    def get_option(self, ticker, trade_start_date = None, trade_end_date=None, expiry=None, K=None, contractSymbol = None, call_put = 'call', get_time_series=False):
        collection = self.db[ticker + '_options']
        option_data = []
        if trade_end_date is None:
            trade_end_date = datetime.datetime.now()

        if contractSymbol is None:
            for doc_date in collection.find({'strike': K, 'expiry': expiry, 'put_call': call_put}):
                #doc_date['time_to_expiry']= (datetime.datetime.strptime(doc_date['expiry'], '%Y-%m-%d') - trade_date).days/365.0
                option_data.append(doc_date)
        else:

            for doc_date in collection.find({'contractSymbol': contractSymbol}):
                #doc_date['time_to_expiry']= (datetime.datetime.strptime(doc_date['expiry'], '%Y-%m-%d') - trade_date).days/365.0
                option_data.append(doc_date)

        df_option_data = pd.DataFrame(option_data)

        # get rid of options that has larger last trade date than the input trade date
        if trade_end_date is not None:
            df_option_data = df_option_data[df_option_data['lastTradeDate'] <= trade_end_date]
        if trade_start_date is not None:
            df_option_data = df_option_data[df_option_data['lastTradeDate'] >= trade_start_date]

        if not get_time_series:
            # get the closest time to trade_date up to the trade date
            if len(df_option_data) >= 1.1:
                index = abs(df_option_data['lastTradeDate'] - trade_end_date).idxmin()
                df_option_data = df_option_data.loc[index]

            if isinstance(df_option_data, pd.DataFrame):
                recent_date = df_option_data['lastTradeDate'].max()
                df_option_data = df_option_data.loc[df_option_data['lastTradeDate'] == recent_date]

            assert isinstance(df_option_data, pd.Series)

        return df_option_data

    # generic function that returns subset of the universe
    # give range for each inputs and make it query
    def get_subset_options(self, S, K, vol, trade_date, expiry_date, call_put, tau = 1):

        pass

    def get_all_options_on_given_tradingDate(self, ticker, trade_date):
        collection = self.db[ticker + '_options']
        option_data = []

        start = trade_date + datetime.timedelta(days=-4)
        #end = trade_date + datetime.timedelta(days=1)
        end = trade_date
        for doc_date in collection.find({'lastTradeDate': {"$gt": start, "$lt": end}}):
            doc_date['time_to_expiry'] = (datetime.datetime.strptime(doc_date['expiry'], '%Y-%m-%d') - trade_date).days / 365.0
            option_data.append(doc_date)

        pd_option_data = pd.DataFrame(option_data)
        return pd_option_data


    def get_shortlisted_portfolio(self, selection_datetime, datetime_runtime = False):
        db_spec = settings.db_settings['shortlisted_portfolio']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]
        collection = db[db_spec['collection_name']]

        start = selection_datetime
        end = selection_datetime + datetime.timedelta(days=1)

        selected = []

        if datetime_runtime:
            for doc_date in collection.find({'datetime_added_to_db': {"$gt": start, "$lt": end}}):
                selected.append(doc_date)
        else:
            for doc_date in collection.find({'datetime_analysis_run': {"$gt": start, "$lt": end}}):
                selected.append(doc_date)

        pd_option_data = pd.DataFrame(selected)
        return pd_option_data

    def get_selected_portfolio(self, selection_datetime, datetime_runtime = False):
        db_spec = settings.db_settings['selected_portfolio']
        client = MongoClient(db_spec['host'], db_spec['port'])
        db = client[db_spec['db_name']]
        collection = db[db_spec['collection_name']]

        start = selection_datetime
        end = selection_datetime + datetime.timedelta(days=1)

        selected = []

        if datetime_runtime:
            for doc_date in collection.find({'datetime_added_to_db': {"$gt": start, "$lt": end}}):
                selected.append(doc_date)
        else:
            for doc_date in collection.find({'datetime_analysis_run': {"$gt": start, "$lt": end}}):
                selected.append(doc_date)

        pd_option_data = pd.DataFrame(selected)
        return pd_option_data



def main():

    inst = data_manipulation()

    '''get stock tick data
    pd_tick_data = inst.import_stock_tick_data('PLTR', datetime.datetime(2020, 11, 24, 00, 00), datetime.datetime(2020, 11, 25, 23, 00))
    from config import LOCAL_OUTPUT
    pd_tick_data.to_csv(LOCAL_OUTPUT + r'\PLTR_tick_data.csv')
    '''

    # Get options
    option = inst.get_all_options_on_given_tradingDate('AI', datetime.datetime(2020, 12, 23, 23, 0))
    print(option)
    from config import LOCAL_OUTPUT
    option.to_csv(LOCAL_OUTPUT + r'\SPX_options.csv')


    # Get option time series
    #option_time_series = inst.get_option('TSLA', contractSymbol='TSLA210226C00777500', get_time_series=True)
    option_time_series = inst.get_option('AAPL', contractSymbol='AAPL220121C00135000', get_time_series=True)
    print(option_time_series['lastTradeDate'])
    from config import LOCAL_OUTPUT
    option_time_series.to_csv(LOCAL_OUTPUT + r'\option_time_series_c.csv')


    pd_AAPL = inst.import_stock_daily_data('AAPL')
    print(pd_AAPL)
    plot = Visualize(pd_AAPL)
    plot.plot_stock()
    from config import LOCAL_OUTPUT
    pd_AAPL.to_csv(LOCAL_OUTPUT + r'\AAPL_daily_data.csv')


    '''
    price, time = inst.get_closest_stock_price('AAPL', datetime.datetime(2019,3,5,16,0))
    print('The price at time {} is {}'.format(time, price))
    
    option = inst.get_options('AAPL', datetime.datetime(2020,3,5,16,0), 200)
    print(option)
    
    short_listed = inst.get_shortlisted_portfolio(datetime.datetime(2020,3,27,0,0))
    print(short_listed)'''

if __name__== "__main__":
    main()