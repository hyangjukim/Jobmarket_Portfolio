# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 16:24:39 2019

@author: Hyangju Kim
"""

# this is to run the optimization algorithm on a particular input dataset

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt 
import portfolio_optimization as pfopt


# import the data and calc inputs 
returns = pd.read_csv("returns.csv")
num_assets = len(returns.columns) - 1
name_assets = returns.columns.tolist()[1:]
avg_rets = returns.mean()
cov_mat = returns.cov()

#matrix_cov = cov_mat.as_matrix()
#test = np.array([1,0,0,0,0])
#test2 = np.array([0,1,0,0,0])

# calculate Markowitz Portfolio
target_ret = avg_rets.quantile(0.7) # need to change this manually
weights_markowitz = pfopt.markowitz_portfolio(cov_mat, avg_rets, target_ret).values
lweights_markowitz = weights_markowitz.tolist()
lweights_markowitz.insert(0, 'markowitz')

# calculate min-variance portfolio
weights_minvar = pfopt.min_var_portfolio(cov_mat).values
lweights_minvar = weights_minvar.tolist()
lweights_minvar.insert(0, 'minvar')

# calculate tangency portoflio i.e. best sharp ratio
weights_tangency = pfopt.tangency_portfolio(cov_mat, avg_rets).values
lweights_tangency = weights_tangency.tolist()
lweights_tangency.insert(0, 'tangency')

# dump out the results in csv
weights = [lweights_markowitz, lweights_minvar, lweights_tangency]

name_assets.insert(0, 'method')
df_weights = pd.DataFrame(weights, columns = name_assets)
df_weights.to_csv('results_weights.csv')

## generate plots

# make random scenarios and plot the optimized portfolios in them

num_scen = 20000
uniform_rand = np.random.uniform(0, 1, num_scen * num_assets)
uniform_rand = np.reshape(uniform_rand, (num_scen , num_assets))
# the weights should sum up to 1
count = 0
for i in uniform_rand:
    uniform_rand[count] = i/sum(i) 
    count += 1
    
#print (uniform_rand)

returns_scenarios = []
vol_scenarios = []

#generate scenario returns and vols
for i in range(num_scen):
    ret_scen = pfopt.portfolio_return(uniform_rand[i], avg_rets)
    vol_scen = pfopt.portfolio_vol(uniform_rand[i], cov_mat)
    returns_scenarios.append(ret_scen)
    vol_scenarios.append(vol_scen)

# add the three points with the optimized weights
ret_markowitz = pfopt.portfolio_return(weights_markowitz, avg_rets)
vol_markowitz = pfopt.portfolio_vol(weights_markowitz, cov_mat)
returns_scenarios.append(ret_markowitz)
vol_scenarios.append(vol_markowitz)

ret_minvar = pfopt.portfolio_return(weights_minvar, avg_rets)
vol_minvar = pfopt.portfolio_vol(weights_minvar, cov_mat)
returns_scenarios.append(ret_minvar)
vol_scenarios.append(vol_minvar)

ret_tangency = pfopt.portfolio_return(weights_tangency, avg_rets)
vol_tangency = pfopt.portfolio_vol(weights_tangency, cov_mat)
returns_scenarios.append(ret_tangency)
vol_scenarios.append(vol_tangency)

xmin = min(vol_scenarios)
xmax = max(vol_scenarios)
ymin = min(returns_scenarios) - abs(min(returns_scenarios)) * 0.2
ymax = max(returns_scenarios) * 1.1

# plot the scenarios along with the optimized portfolios
plt.scatter(vol_scenarios, returns_scenarios)
plt.title('Efficient Frontier')
plt.xlabel('portfolio vol')
plt.ylabel('portfolio return')
plt.xlim(0, xmax)
plt.ylim(ymin, ymax)

plt.plot(vol_scenarios[num_scen], returns_scenarios[num_scen], 'ro')
plt.plot(vol_scenarios[num_scen+1], returns_scenarios[num_scen+1], 'ro')
plt.plot(vol_scenarios[num_scen+2], returns_scenarios[num_scen+2], 'ro')

plt.show()

