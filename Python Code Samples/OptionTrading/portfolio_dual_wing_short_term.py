# This is the main strategy to be deployed
import multiprocessing
import datetime
import pandas as pd
from scipy import stats
from Utilities.data_manipulation import data_manipulation
import Utilities.black_scholes_merton as bs
from config import LOCAL_OUTPUT


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


def run_dual_wing_strategy(input_arg):

    inst = strat_dual_wings_short_term()

    #unpack input arguments
    ticker = input_arg['ticker']
    trade_datetime = input_arg['batch_datetime']
    flag_output_file = input_arg['flag_output_file']

    if trade_datetime is None:
        trade_datetime = datetime.datetime.now()

    if ticker == 'SPX':
        underlying_stock_price, time_call = inst.inst_data.get_closest_stock_price('^GSPC', trade_datetime)
    else:
        underlying_stock_price, time_call = inst.inst_data.get_closest_stock_price(ticker, trade_datetime)

    if underlying_stock_price is None:
        print('No stock price is found for {}'.format(ticker))
        return 0

    short_list_each_ticker = inst.all_in_one_option_selection(ticker, trade_datetime=trade_datetime, output_file=flag_output_file)

    return 0


def for_multiprocessing(tickers = None, trade_datetime=None, flag_output_file = True):
    batchrun_start = datetime.datetime.now()

    num_threads = multiprocessing.cpu_count()
    print('Number of threads: {}'.format(num_threads))

    if tickers is None:
        tickers = get_names_available_data()

    if trade_datetime is None:
        trade_datetime = datetime.datetime.now()

    input_arg = []
    for ticker in tickers:
        dinput = dict()
        dinput['ticker'] = ticker[1]
        dinput['batch_datetime'] = trade_datetime
        dinput['flag_output_file'] = flag_output_file
        input_arg.append(dinput)

    p = multiprocessing.Pool(num_threads)

    result = p.map(run_dual_wing_strategy, input_arg)

    p.close()
    p.join()


    batchrun_end = datetime.datetime.now()
    print('Batch run process took total {} time'.format(batchrun_end - batchrun_start))

    return tickers



class strat_dual_wings_short_term:

    def __init__(self):
        self.inst_data = data_manipulation()

    def all_in_one_option_selection(self, ticker, trade_datetime='recent', output_file=True):
        start_time = datetime.datetime.now()

        if trade_datetime == 'recent':
            trade_datetime = datetime.datetime.now()

        pd_option_data = self.inst_data.get_all_options_on_given_tradingDate(ticker, trade_datetime)

        # get all strike and expiry
        try:
            list_strike = pd_option_data['strike']
            list_strike.drop_duplicates(inplace=True)
            list_expiries = pd_option_data['expiry']
            list_expiries.drop_duplicates(inplace=True)
        except:
            return None

        most_recent_transaction = pd_option_data['lastTradeDate'].max()

        if ticker == 'SPX':
            current_stock_price, time_current_stock = self.inst_data.get_closest_stock_price('^GSPC', most_recent_transaction)
        else:
            current_stock_price, time_current_stock = self.inst_data.get_closest_stock_price(ticker,
                                                                                             most_recent_transaction)

        # loop through all options and get the highest return
        list_record = []
        for expiry in list_expiries:
            annual_expiry = (datetime.datetime.strptime(expiry, '%Y-%m-%d') - time_current_stock).days/365.0
            for strike in list_strike:
                for type in ['call', 'put']:
                    try:
                        option = self.inst_data.get_option(ticker, trade_start_date=None, trade_end_date=trade_datetime, expiry=expiry, K=strike, call_put=type)

                        try:
                            option['time_to_expiry'] = (datetime.datetime.strptime(option['expiry'], '%Y-%m-%d') - time_current_stock).days/365.0
                            option['time_to_expiry_last_trade'] = (datetime.datetime.strptime(option['expiry'], '%Y-%m-%d') - option['lastTradeDate']).days/365.0
                        except:
                            continue
                        if ticker == 'SPX':
                            underlying_stock_price, time_call = self.inst_data.get_closest_stock_price('^GSPC', option['lastTradeDate'])
                        else:
                            underlying_stock_price, time_call = self.inst_data.get_closest_stock_price(ticker, option[
                                'lastTradeDate'])

                        # skip this option if data is not up to par
                        if option['ask'] <= 0.01:
                            continue
                        if option['bid'] <= 0.00001:
                            option['bid'] = option['ask'] * 0.9
                        if option['impliedVolatility'] <= 0.0001:
                            continue

                        check_option_bs_price, risk_sensitivities = bs.bs_price(type, underlying_stock_price,
                                                                               option['strike'],
                                                                               option['time_to_expiry_last_trade'],
                                                                               option['impliedVolatility'], r=0.003)
                        option['check_option_bs'] = check_option_bs_price

                        # backout the interest rate by matching the ask price
                        interest_rate = bs.get_interest_rate(0.01, option, underlying_stock_price, option_price = 0.5)
                        r = interest_rate.x[0]

                        option['interest_rate'] = r

                        # start recording...
                        record = option.to_dict()

                        current_bs_price, hy_risk_sensitivity = bs.bs_price(type, current_stock_price,
                                                                              option['strike'],
                                                                              option['time_to_expiry'],
                                                                              option['impliedVolatility'], r=option['interest_rate'])


                        record['stock_datetime_last'] = time_call
                        record['stock_price_last'] = underlying_stock_price
                        record['stock_datetime_current'] = time_current_stock
                        record['stock_price_current'] = current_stock_price
                        record['bsm_price_current'] = current_bs_price

                        record['delta'] = hy_risk_sensitivity['delta']
                        record['gamma'] = hy_risk_sensitivity['gamma']
                        record['vega'] = hy_risk_sensitivity['vega']
                        record['theta'] = hy_risk_sensitivity['theta']
                        record['rho'] = hy_risk_sensitivity['rho']

                        record['check_current_bid_ask'] = False
                        if (most_recent_transaction - record['lastTradeDate']).days >= 7:
                            record['check_current_bid_ask'] = True

                        # calculate the empirical implied vol percentile
                        historical_implied_vol = self.get_historical_implied_vol(ticker, option['contractSymbol'], trade_datetime)
                        threshold_empirical_percentile = stats.percentileofscore(historical_implied_vol, option['impliedVolatility'])/100.0
                        record['vol_percentile'] = threshold_empirical_percentile
                        record['original_moneyness'] = current_stock_price/record['strike']

                        # filter out options that are unnecessary - later
                        list_record.append(record)

                    except:
                        continue

        if list_record:
            record_list = pd.DataFrame(list_record)
            if output_file is True:
                #today = datetime.datetime.now()
                sToday = trade_datetime.strftime('%m_%d_%Y')
                try:
                    record_list.to_csv(LOCAL_OUTPUT + r'\records_short_term_{}_{}.csv'.format(ticker, sToday))
                except:
                    print(r'\records_{}_{}.csv not dumped out properly'.format(ticker, sToday))

        end_time = datetime.datetime.now()
        print('Process took {} time for ticker {}'.format(end_time - start_time, ticker))

        return list_record



    def get_historical_implied_vol(self, ticker, symbol, trade_date):
        option_time_series = self.inst_data.get_option(ticker, trade_end_date=trade_date, contractSymbol=symbol, get_time_series=True)
        option_time_series.drop_duplicates(subset='lastTradeDate', inplace=True)

        loption_time_series = option_time_series['impliedVolatility'].to_list()

        return loption_time_series


def main():
    shortlist_date = datetime.datetime(2020, 11, 25, 23, 0)
    for_multiprocessing([['PLTR_options','PLTR']], trade_datetime=shortlist_date)

    #for_multiprocessing([['AAPL_options','AAPL'],['NFLX_options','NFLX'], ['AMZN_options','AMZN'],['FB_options','FB']
    #                     ,['TSLA_options','TSLA'], ['GOOG_options','GOOG']], trade_datetime=shortlist_date)

if __name__== "__main__":
    main()