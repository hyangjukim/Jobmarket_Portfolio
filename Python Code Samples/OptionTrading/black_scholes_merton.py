from Utilities.data_manipulation import data_manipulation
import numpy as np
from scipy.stats import norm
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import datetime
from scipy import interpolate
import pandas as pd

# To-do: Honestly, most of the cases, BSM cannot match the option bid-ask prices. The market factors in higher demand...
# For example, the pendemic crisis made the put option much desirable, which makes the price higher than the BSM pricer.
# So honestly I think it works better to use, Delta, Gamma, Theta, Vega to predict the future value of the option price.
# I don't think Yahoo finance provide that information, that is why I have to stick with repricing using BSM.
# However, once I am able to purchase more data with all sensitivities, it should be more accurate to predict the future price..
# Then... please add that sensitivity pricer to this module.

def bs_price(call_put, S, K, tau, sigma, r):
    d_1 = 1/(sigma*(np.sqrt(tau)))*(np.log(S/K) + (r + sigma*sigma/2.0)*tau)
    d_2 = d_1 - sigma*np.sqrt(tau)

    if call_put == 'call':
        price = norm.cdf(d_1)*S - norm.cdf(d_2)*np.exp(-r*tau)*K
    else:
        price = norm.cdf(-d_2)*K*np.exp(-r*tau) - norm.cdf(-d_1)*S

    risk_sensitivities = dict()

    if call_put == 'call':
        risk_sensitivities['delta'] = norm.cdf(d_1)
        risk_sensitivities['gamma'] = norm.pdf(d_1)/(S*sigma*(np.sqrt(tau)))
        risk_sensitivities['vega'] = S*norm.pdf(d_1)*np.sqrt(tau)
        risk_sensitivities['theta'] = -(S*norm.pdf(d_1)*sigma)/(2*np.sqrt(tau))-r*K*np.exp(-r*tau)*norm.cdf(d_2)
        risk_sensitivities['rho'] = K*tau*np.exp(-r*tau)*norm.cdf(d_2)
    else:
        risk_sensitivities['delta'] = -norm.cdf(-d_1)
        risk_sensitivities['gamma'] = norm.pdf(d_1)/(S*sigma*(np.sqrt(tau)))
        risk_sensitivities['vega'] = S*norm.pdf(d_1)*np.sqrt(tau)
        risk_sensitivities['theta'] = -(S*norm.pdf(d_1)*sigma)/(2*np.sqrt(tau))+r*K*np.exp(-r*tau)*norm.cdf(-d_2)
        risk_sensitivities['rho'] = -K*tau*np.exp(-r*tau)*norm.cdf(-d_2)

    return price, risk_sensitivities


def backout_underlying_stock_price(S, option, option_paid, vol_scale = 0.9, time_passes = 0.25, surface = None):

    try:
        result = minimize(_func_backout_stock, S, args=(option, option_paid, vol_scale, time_passes, surface), method = 'SLSQP')
    except ValueError:
        print('failed using SLSQP')

    if result.success:
        return result
    else:
        try:
            print('failed using SLSQP')
            result = minimize(_func_backout_stock, S, args=(option, option_paid, vol_scale, time_passes, surface), method='CG')
        except ValueError:
            print('failed using CG')

    if result.success:
        return result
    else:
        try:
            print('failed using CG')
            result = minimize(_func_backout_stock, S, args=(option, option_paid, vol_scale, time_passes, surface), method='BFGS')
        except ValueError:
            print('failed using BFGS, now running CG')

    if result.success:
        return result
    else:
        try:
            print('failed using BFGS')
            result = minimize(_func_backout_stock, S, args=(option, option_paid, vol_scale, time_passes, surface), method='Newton-CG')
        except ValueError:
            print('failed using Newton-CG')

    return result



# note that this is assuming I will invest equal amount to put and call
def _func_backout_stock(x, option, option_paid, vol_scale, time_passes, surface = None):
    balance = 10000

    # option information
    type = option['put_call']
    K = option['strike']
    tau = option['time_to_expiry']
    if surface is None:
        vol = option['impliedVolatility'] * vol_scale
    else:
        moneyness = x/K
        vol = get_implied_vol_from_surface(surface, moneyness, tau - time_passes)

    r = 0.0001
    bsm_price, risk_sensitivity = bs_price(type, x, K, tau - time_passes, vol, r)
    num_option = balance / option_paid

    option_intrinsic_value = bsm_price * num_option
    value = (option_intrinsic_value - balance * 2)**2

    return value

# this will simply backout the interest rate by least squares of black scholes with the bid or ask price.
# one problem is that this free variable may not make too much sense if I brute forcely calculate the interest rate
# For example, the implied vol can have bad data, and then the interest rate will have to compensate for that even though
# the value of interest rate doesn't make sense which means the interest rate can be negative or insanely large number
# So bad data has to be filtered out before calculating this to not get trapped in this...
# another thing is, I think the put-call parity has to be taken into account to get the most accurate interest rate.
# however, if this simple back out does work without much problem, I don't want to complicate things by calculting
# multiple equation with one unknown. This might take another additional hour or so which I don't want to spend doing so.

# make the option_price somewhere between 0 and 1
def get_interest_rate(r, option, S, option_price = 0.5):

    try:
        result = minimize(helper_get_interest_rate, r, args=(option, S, option_price), method='SLSQP')
    except ValueError:
        print('failed using SLSQP, now running CG')
        result = minimize(helper_get_interest_rate, r, args=(option, S, option_price), method='CG')
    except ValueError:
        print('failed using CG, now running BFGS')
        result = minimize(helper_get_interest_rate, r, args=(option, S, option_price), method='BFGS')
    except ValueError:
        print('failed using CG, now running Newton-CG')
        result = minimize(helper_get_interest_rate, r, args=(option, S, option_price), method='Newton-CG')
    return result

def helper_get_interest_rate(x, option, S, option_price = 0.5):
    # call information
    type = option['put_call']
    K = option['strike']
    tau = option['time_to_expiry']
    vol = option['impliedVolatility']

    price = option['ask'] * option_price + option['bid'] * (1-option_price)

    bsm_price, risk_sensitivity = bs_price(type, S, K, tau, vol, x)
    value = (bsm_price - price) ** 2

    return value

# make the option_price somewhere between 0 and 1
def get_implied_vol(sigma, option, S, option_price = 0.5):

    try:
        result = minimize(helper_get_implied_vol, sigma, args=(option, S, option_price), method='SLSQP')
    except ValueError:
        print('failed using SLSQP, now running CG')
        result = minimize(helper_get_implied_vol, sigma, args=(option, S, option_price), method='CG')
    except ValueError:
        print('failed using CG, now running BFGS')
        result = minimize(helper_get_implied_vol, sigma, args=(option, S, option_price), method='BFGS')
    except ValueError:
        print('failed using CG, now running Newton-CG')
        result = minimize(helper_get_implied_vol, sigma, args=(option, S, option_price), method='Newton-CG')
    return result

def helper_get_implied_vol(x, option, S, option_price = 0.5):
    # call information
    type = option['put_call']
    K = option['strike']
    tau = option['time_to_expiry']
    r = 0.01

    price = option['ask'] * option_price + option['bid'] * (1-option_price)

    bsm_price, risk_sensitivity = bs_price(type, S, K, tau, x, r)
    value = (bsm_price - price) ** 2

    return value

# get the most recent liquid options and reconstruct the vol surface.
# for the ones without the vol information, just interpolate the surface
def construct_surfaces(ticker, trade_date = None, plot = False):
    # gather all data from the database: match the stock date and time
    if trade_date is None:
        trade_datetime = datetime.datetime.now()

    inst_data = data_manipulation()
    pd_option_data = inst_data.get_all_options_on_given_tradingDate(ticker, trade_datetime)
    pd_option_data.sort_values('lastTradeDate', ascending=False, inplace = True)
    pd_option_data.drop_duplicates('contractSymbol', inplace = True)

    # get rid of implied vol lower than 0.0001
    pd_option_data = pd_option_data[pd_option_data.impliedVolatility >= 0.0001]

    lOption_data = pd_option_data.to_dict('record')

    lSurface = []
    for opt in lOption_data:
        stock_price, stock_timestamp = inst_data.get_closest_stock_price(ticker, opt['lastTradeDate'])
        opt['stock_price'] = stock_price
        opt['stock_datetime'] = stock_timestamp
        opt['moneyness'] = stock_price/opt['strike']

        # okay... don't make it too complicated...just use all the options without duplicates and construct the surface
        # I think that will be sufficient
        lSurface.append(opt)

    # Columns will be the moneyness and index will be the time to maturity
    pd_surface = pd.DataFrame(lSurface)
    pd_surface.sort_values('moneyness', inplace=True)
    pd_surface.sort_values('time_to_expiry', inplace=True)

    if plot is True:
        fig = plt.figure(figsize=(6, 6))
        ax = fig.add_subplot(111, projection='3d')
        x = pd_surface['moneyness'].to_numpy()
        y = pd_surface['time_to_expiry'].to_numpy()
        xv, yv = np.meshgrid(x, y)
        z = pd_surface['impliedVolatility'].to_numpy()
        z = np.expand_dims(z, axis=1)
        ax.plot_surface(xv, yv, z)
        ax.set_title('Implied Vol Surface plot')
        ax.set_xlabel('Moneyness')
        ax.set_ylabel('Time to Expiry')
        ax.set_zlabel('Implied Volatility')
        plt.show()
    return pd_surface

def get_implied_vol_from_surface(surface, moneyness, time_to_expiry):
    x = surface['moneyness'].to_numpy()
    y = surface['time_to_expiry'].to_numpy()
    #xv, yv = np.meshgrid(x, y)
    z = surface['impliedVolatility'].to_numpy()
    #z = np.expand_dims(z, axis=1)
    f = interpolate.interp2d(x, y, z, kind='cubic')

    implied_vol = f(moneyness, time_to_expiry)

    return implied_vol[0]