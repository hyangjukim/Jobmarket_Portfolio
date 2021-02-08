# build strategies here
# Note that each class will represent a strategy. In the strategy, there should be data handler, metrics calculation,
# backtesting, and also risk management. Sometimes, in conjuction with the metrics calculation, we can introduce a statistical
# modeling component, but the simpler the better. I don't think it is necessary to fit any quantitative model to the
# training dataset. Hopefully there is a universal indicator to signal the trading.

# This is the main strategy to be deployed
import multiprocessing
import datetime
import pandas as pd
import numpy as np
import settings
from scipy import stats
from pymongo import MongoClient
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

    db_spec = settings.db_settings['shortlisted_portfolio']
    client = MongoClient(db_spec['host'], db_spec['port'])
    db = client[db_spec['db_name']]
    collection = db[db_spec['collection_name']]

    inst = strat_dual_wings()

    #unpack input arguments
    ticker = input_arg['ticker']
    trade_datetime = input_arg['batch_datetime']
    flag_output_file = input_arg['flag_output_file']
    slow_mode = input_arg['slow_mode']

    if trade_datetime is None:
        trade_datetime = datetime.datetime.now()

    underlying_stock_price, time_call = inst.inst_data.get_closest_stock_price(ticker, trade_datetime)

    if underlying_stock_price is None:
        print('No stock price is found for {}'.format(ticker))
        return 0

    short_list_each_ticker = inst.all_in_one_option_selection(ticker, trade_datetime=trade_datetime, output_file=flag_output_file, slow_mode = slow_mode)
    upload_time = datetime.datetime.now()
    add_ticker = []
    if short_list_each_ticker is not None:
        for option in short_list_each_ticker:
            option['ticker'] = ticker
            option['datetime_added_to_db'] = upload_time
            option['datetime_analysis_run'] = trade_datetime
            if '_id' in option:
                del option['_id']
            add_ticker.append(option)

    #pid = collection.insert_many(add_ticker).inserted_ids
    for document in add_ticker:
        try:
            pid = collection.insert_one(document).inserted_id
        except:
            for key, val in document.items():
                if isinstance(val, np.bool_):
                    val = bool(val)
                if isinstance(val, np.int64):
                    val = int(val)
                if isinstance(val, np.float64):
                    val = float(val)
                document[key] = val

            pid = collection.insert_one(document).inserted_id

    return 0


def for_multiprocessing(tickers = None, trade_datetime=None, flag_output_file = True, slow_mode = False):
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
        dinput['slow_mode'] = slow_mode
        input_arg.append(dinput)

    p = multiprocessing.Pool(num_threads)

    result = p.map(run_dual_wing_strategy, input_arg)

    p.close()
    p.join()


    batchrun_end = datetime.datetime.now()
    print('Batch run process took total {} time'.format(batchrun_end - batchrun_start))

    return tickers



class strat_dual_wings:

    def __init__(self):
        self.inst_data = data_manipulation()
        self.balance = 10000
        #self.collection_names = self.inst_data.collection_names



    def all_in_one_option_selection(self, ticker, trade_datetime='recent', output_file=True, time_passes=0.25, slow_mode = False):
        start_time = datetime.datetime.now()

        if trade_datetime == 'recent':
            trade_datetime = datetime.datetime.now()

        # get all options for the specified ticker
        # get the list of strikes and expiries for all options
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
        current_stock_price, time_current_stock = self.inst_data.get_closest_stock_price(ticker, most_recent_transaction)

        # get empirical return
        empirical_return = self.empirical_dist_comparison(ticker, stock_datetime=trade_datetime)

        surface = None
        if slow_mode is True:
            surface = bs.construct_surfaces(ticker)

        # loop through all options and get the highest return
        list_record = []
        for expiry in list_expiries:
            annual_expiry = (datetime.datetime.strptime(expiry, '%Y-%m-%d') - time_current_stock).days/365.0
            if annual_expiry <= time_passes + 0.5:
                continue
            for strike in list_strike:
                for type in ['call', 'put']:
                    try:
                        option = self.inst_data.get_option(ticker, trade_start_date=None, trade_end_date=trade_datetime, expiry=expiry, K=strike, call_put=type)

                        try:
                            option['time_to_expiry'] = (datetime.datetime.strptime(option['expiry'], '%Y-%m-%d') - time_current_stock).days/365.0
                            option['time_to_expiry_last_trade'] = (datetime.datetime.strptime(option['expiry'], '%Y-%m-%d') - option['lastTradeDate']).days/365.0
                        except:
                            continue

                        underlying_stock_price, time_call = self.inst_data.get_closest_stock_price(ticker, option['lastTradeDate'])

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
                        option['is_vol_overwritten'] = False

                        # if bsm is too far from bid/ask then recalibrate the implied vol
                        '''
                        if abs(check_option_bs_price/option['ask']-1)>=0.35:
                            print('{} has bad implied vol data'.format(option['contractSymbol']))
                            option['is_vol_overwritten'] = True
                            new_implied_vol = bs.get_implied_vol(option['impliedVolatility'], option, underlying_stock_price, option_price = 0.5)
                            option['impliedVolatility'] = new_implied_vol.x[0]'''


                        # backout the interest rate by matching the ask price
                        interest_rate = bs.get_interest_rate(0.01, option, underlying_stock_price, option_price = 0.5)
                        r = interest_rate.x[0]

                        option['interest_rate'] = r

                        # start recording...
                        record = option.to_dict()

                        last_option_bs_price, risk_sensitivities = bs.bs_price(type, underlying_stock_price,
                                                                          option['strike'],
                                                                          option['time_to_expiry_last_trade'],
                                                                          option['impliedVolatility'], r=option['interest_rate'])

                        current_bs_price, hy_risk_sensitivity = bs.bs_price(type, current_stock_price,
                                                                              option['strike'],
                                                                              option['time_to_expiry'],
                                                                              option['impliedVolatility'], r=option['interest_rate'])

                        # the key... calculate the threshold
                        # Note that the forecasted implied vol is original implied vol * 0.9 just to be quick and dirty unless it is close to ATM
                        if option['put_call'] == 'call':
                            if (option['strike']-current_stock_price)/option['strike'] <= 0.1:
                                record['future_implied_vol'] = option['impliedVolatility']
                                optimized = bs.backout_underlying_stock_price(current_stock_price, option, vol_scale = 1.0, option_paid=current_bs_price, time_passes=time_passes)
                            else:
                                record['future_implied_vol'] = option['impliedVolatility'] * 0.9
                                optimized = bs.backout_underlying_stock_price(current_stock_price, option, vol_scale = 0.9, option_paid=current_bs_price, time_passes=time_passes)

                        if option['put_call'] == 'put':
                            if (current_stock_price-option['strike'])/current_stock_price <= 0.1:
                                record['future_implied_vol'] = option['impliedVolatility']
                                optimized = bs.backout_underlying_stock_price(current_stock_price, option, vol_scale = 1.0, option_paid=current_bs_price, time_passes=time_passes)
                            else:
                                record['future_implied_vol'] = option['impliedVolatility'] * 0.9
                                optimized = bs.backout_underlying_stock_price(current_stock_price, option, vol_scale = 0.9, option_paid=current_bs_price, time_passes=time_passes)


                        if optimized.success:
                            underlying_threshold_stock = optimized.x[0]
                        else:
                            print('Optimization failed for symbol {}'.format(option['contractSymbol']))
                            continue
                            #To-do: make a function that finds the threshold with something like bisection etc.
                            #underlying_threshold_stock = bs.backout_underlying_stock_price_slow(underlying_stock_price, option,
                            #                                          time_passes=time_passes, option_paid= current_bs_price)


                        record['threshold_stock'] = underlying_threshold_stock

                        if slow_mode is True:
                            moneyness = underlying_threshold_stock/option['strike']
                            implied_vol = bs.get_implied_vol_from_surface(surface, moneyness, option['time_to_expiry'] - time_passes)
                            record['surface_implied_vol'] = implied_vol
                            future_option_price, future_risk_sensitivity = bs.bs_price(type, underlying_threshold_stock,
                                                                                       option['strike'],
                                                                                       option['time_to_expiry'] - time_passes,
                                                                                       implied_vol,  r=0.0001)
                        else:
                            future_option_price, future_risk_sensitivity = bs.bs_price(type, underlying_threshold_stock,
                                                                                  option['strike'],
                                                                                  option['time_to_expiry'] - time_passes,
                                                                                  record['future_implied_vol'], r=0.0001)

                        record['option_return_current'] = (self.balance/current_bs_price) * (future_option_price - current_bs_price)/self.balance
                        record['stock_return'] = underlying_threshold_stock / current_stock_price - 1
                        record['stock_datetime_last'] = time_call
                        record['stock_price_last'] = underlying_stock_price
                        record['stock_datetime_current'] = time_current_stock
                        record['stock_price_current'] = current_stock_price

                        record['bsm_price_last'] = last_option_bs_price
                        record['bsm_price_current'] = current_bs_price
                        record['bsm_future_price'] = future_option_price

                        record['delta'] = hy_risk_sensitivity['delta']
                        record['gamma'] = hy_risk_sensitivity['gamma']
                        record['vega'] = hy_risk_sensitivity['vega']
                        record['theta'] = hy_risk_sensitivity['theta']
                        record['rho'] = hy_risk_sensitivity['rho']
                        record['return_impliedVol_ratio'] = abs(record['stock_return'])/option['impliedVolatility']

                        record['check_current_bid_ask'] = False
                        if (most_recent_transaction - record['lastTradeDate']).days >= 7:
                            record['check_current_bid_ask'] = True

                        # calculate the empirical implied vol percentile
                        historical_implied_vol = self.get_historical_implied_vol(ticker, option['contractSymbol'], trade_datetime)
                        threshold_empirical_percentile = stats.percentileofscore(historical_implied_vol, option['impliedVolatility'])/100.0
                        record['vol_percentile'] = threshold_empirical_percentile


                        record['window_max'] = empirical_return['window_max']
                        record['window_min'] = empirical_return['window_min']
                        record['1year_max'] = empirical_return['1year_max']
                        record['1year_min'] = empirical_return['1year_min']
                        record['window_max_return'] = empirical_return['window_max']/current_stock_price-1
                        record['window_min_return'] = empirical_return['window_min']/current_stock_price-1
                        record['original_moneyness'] = current_stock_price/record['strike']
                        record['threshold_moneyness'] = underlying_threshold_stock/record['strike']

                        if option['put_call'] == 'call':
                            if abs(record['stock_return']) <= record['window_max_return']:
                                record['is_in_return_range'] = True
                            else:
                                record['is_in_return_range'] = False
                        else:
                            if abs(record['stock_return']) <= abs(record['window_min_return']):
                                record['is_in_return_range'] = True
                            else:
                                record['is_in_return_range'] = False

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
                    record_list.to_csv(LOCAL_OUTPUT + r'\records_{}_{}.csv'.format(ticker, sToday))
                except:
                    print(r'\records_{}_{}.csv not dumped out properly'.format(ticker, sToday))

        end_time = datetime.datetime.now()
        print('Process took {} time for ticker {}'.format(end_time - start_time, ticker))

        return list_record

    def empirical_dist_comparison(self, ticker, stock_datetime=None, window = 66):

        if stock_datetime is None:
            start_date = datetime.datetime.now() + datetime.timedelta(days=-365)
            end_date = datetime.datetime.now()
        else:
            start_date = stock_datetime + datetime.timedelta(days=-365)
            end_date = stock_datetime

        # To-do: fix the database for the daily stock prices
        '''
        inst = yf.Ticker(ticker)
        stock_data = inst.history(start=start_date, end=end_date)
        lprice = stock_data['Close'].to_list()'''

        inst = data_manipulation(start_date)
        stock_data = inst.import_stock_daily_data(ticker, start = start_date, end= end_date)
        stock_data.drop_duplicates(subset="Date", inplace=True)
        stock_data.sort_values("Date", inplace=True)
        lprice = stock_data['Close'].to_list()

        # data preparation
        moving_max = []
        moving_min = []
        obs_return = []
        for p in range(len(lprice) - window):

            max_return = np.max(lprice[p+1:p + window]) / lprice[p] - 1
            min_return = np.min(lprice[p+1:p + window]) / lprice[p] - 1

            moving_max.append(max_return)
            moving_min.append(min_return)

        recent_max_return = np.max(lprice[-window:])/lprice[-1] - 1
        recent_min_return = np.min(lprice[-window:]) / lprice[-1] - 1

        result_dictionary = dict()
        result_dictionary['moving_max'] = moving_max
        result_dictionary['moving_min'] = moving_min
        #result_dictionary['recent_max_return'] = stats.percentileofscore(moving_max, recent_max_return)/100.0
        #result_dictionary['recent_min_return'] = 1-stats.percentileofscore(moving_min, recent_min_return)/100.0
        result_dictionary['window_max'] = np.max(lprice[-window:])
        result_dictionary['window_min'] = np.min(lprice[-window:])
        result_dictionary['1year_max'] = np.max(lprice)
        result_dictionary['1year_min'] = np.min(lprice)
        return result_dictionary

    # this gives the target price - Depricated for now
    def single_name_dailydata_analysis(self, ticker, stock_price, stock_datetime=None):
        if stock_datetime is None:
            start_date = datetime.datetime.now() + datetime.timedelta(days=-365)
            option_date = datetime.datetime.now()
        else:
            start_date = stock_datetime + datetime.timedelta(days=-365)
            option_date = stock_datetime

        # get ATM implied vol
        pd_option_data = self.inst_data.get_all_options_on_given_tradingDate(ticker, option_date)
        if pd_option_data.empty == False:
            catch_implied_vol = pd_option_data.loc[
                (pd_option_data['strike'] >= stock_price * 0.9) & (pd_option_data['strike'] <= stock_price * 1.1)]
        else:
            return 1.3 * stock_price, 0.7 * stock_price
        ATM_implied_vol = np.average(catch_implied_vol['impliedVolatility'])

        '''
        Documentation: Just simply set the return to be ATM implied vol * 0.5. 
        '''

        up_stock_price = (1 + ATM_implied_vol * 0.5) * stock_price
        down_stock_price = (1 - ATM_implied_vol * 0.5) * stock_price

        return up_stock_price, down_stock_price


    def get_historical_implied_vol(self, ticker, symbol, trade_date):
        option_time_series = self.inst_data.get_option(ticker, trade_end_date=trade_date, contractSymbol=symbol, get_time_series=True)
        option_time_series.drop_duplicates(subset='lastTradeDate', inplace=True)

        loption_time_series = option_time_series['impliedVolatility'].to_list()

        return loption_time_series



    # To-do: If possible, make a good regression to get the projected implied volatility
    def implied_vol_regression(self):

        pass


# Post processing: After the initial filtering, group the possible call-put wings and run the combinations to
# understand which options are the best for each ticker, and also understand what is a good price to offer
# to the market makers

class post_processing:

    def __init__(self, analysis_date, option_candidates = None):
        self.analysis_date = analysis_date
        self.data_inst = data_manipulation()
        self.pd_shortlisted = self.data_inst.get_shortlisted_portfolio(analysis_date)

        self.full_ticker_list = self.pd_shortlisted['ticker'].drop_duplicates(inplace=False)


        if option_candidates is None:
            self.option_candidates = self.full_ticker_list
        else:
            self.option_candidates = option_candidates


    def run_process(self, output_file = True):

        range_info_to_rank_order = []

        # group the option candidates for one ticker

        final_list = []
        for ticker in self.option_candidates:
            pd_ticker = self.pd_shortlisted.loc[self.pd_shortlisted['ticker'] == ticker]

            calls = pd_ticker.loc[pd_ticker['put_call'] == 'call']
            puts = pd_ticker.loc[pd_ticker['put_call'] == 'put']

            #filter out bad calculation
            calls = calls[calls['option_return_current'] >= 0]
            calls = calls[calls['stock_return'] >= 0]
            calls = calls[abs(calls['interest_rate']) <= 0.10]

            puts = puts[puts['option_return_current'] >= 0]
            puts = puts[puts['stock_return'] <= 0]
            puts = puts[abs(puts['interest_rate']) <= 0.10]

            if calls.empty is True or puts.empty is True:
                continue
            '''
            # if not in peak or trough just skip
            check = calls.iloc[0]
            max_return = check['window_max_return']
            min_return = check['window_min_return']

            if abs(max_return) >= 0.10 and abs(min_return) >= 0.10:
                continue
            '''

            # pick top 20 best performing calls and puts
            calls = calls.nsmallest(20, 'threshold_stock')
            puts = puts.nlargest(20, 'threshold_stock')

            dcalls = calls.to_dict('record')
            dputs = puts.to_dict('record')

            # Then check if the selected option passed BSM backtesting
            call_vol_percentile = []
            put_vol_percentile = []
            for call in dcalls:
                if call['vol_percentile'] <= 0.50:
                    call_vol_percentile.append(call)

            for put in dputs:
                if put['vol_percentile'] <= 0.50:
                    put_vol_percentile.append(put)

            if not call_vol_percentile or not put_vol_percentile:
                continue

            # make a final list...by ticker...
            final_list = final_list + call_vol_percentile + put_vol_percentile

        #export csv
        if output_file is True:
            pd_result = pd.DataFrame(final_list)
            today = self.analysis_date
            sToday = today.strftime('%m_%d_%Y')
            pd_result.to_csv(LOCAL_OUTPUT + r'\sorted_option_pairs_{}.csv'.format(sToday))

        return final_list

    # To-Do : leverage this if needed in the future
    def check_data_quality(self, option):
        symbol = option['contractSymbol']
        ticker = option['ticker']
        current_implied_vol = option['impliedVolatility']
        option_trade_date = option['lastTradeDate']

        option_time_series = self.data_inst.get_option(ticker, contractSymbol=symbol, get_time_series=True)

        index = abs(option_time_series['lastTradeDate'] - option_trade_date).idxmin()
        df_option_data = option_time_series.loc[index]
        assert isinstance(df_option_data, pd.Series)

        pass



def main():
    shortlist_date = datetime.datetime(2020, 10, 23, 23, 0)
    #for_multiprocessing([['JPM_options', 'JPM']])

    for_multiprocessing(trade_datetime=shortlist_date)

    shortlist_date = datetime.datetime(2020, 10, 23, 19, 0)
    inst = post_processing(shortlist_date)
    inst.run_process()

if __name__== "__main__":
    main()