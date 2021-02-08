import datetime
from Utilities.data_manipulation import data_manipulation
from DataProcessor.buildDB_yfinance_injection import Stock_Time_Series_YF

def get_names_available_data():
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

tickers = get_names_available_data()
inst = data_manipulation()

missing_list = []
for ticker in tickers:
    pd_tick_data = inst.import_stock_tick_data(ticker[1], datetime.datetime(2020, 8, 21, 00, 00), datetime.datetime(2020, 8, 21, 23, 59))
    if pd_tick_data.empty:
        missing_list.append(ticker[1])

print(missing_list)

inst_stocks = Stock_Time_Series_YF(tickers = missing_list, interval='2m', period='14d')
record_stocks = inst_stocks.inject_time_series_db()
print(record_stocks)