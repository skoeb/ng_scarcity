#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 10:14:22 2019

@author: skoebric
"""
# --- Python Library Imports ---
import datetime
import requests
import json
import os
import concurrent.futures as cf

# --- Package Imports ---
import pandas as pd
import numpy as np
import wbdata as wb
    
# --- Module Imports ---
import config
import helper_functions
import api_functions
import forecast_functions
import stats_functions

def data_gather_and_forecast(series_id, source, name, threshold=0.5, how='spline', scale=None):
    """
    High-level wrapper that performs three steps:
        1) Fetches datapoints for all countries from a *source* API using a *series_id*.
        2) Cleans that data to drop countries with excessive missing data.
        3) Forecasts that data using *how* between config.START_YEAR and config.END_YEAR
    
    Inputs
    ------
    series_id: (string) with a {} for country formatting if the source is 'EIA'
    source: (string) either EIA or WB currently.
    name: (string) the name that will be given to the column when completed
    threshold: (float, or None) the percent of years a country must have to be included.
    how: (string) the algoritihm for forecasting, will be passed to pandas.dataframe.interpolate
        currently only 'spline' is tested. 
        
    Examples
    --------
    
    '4.1_SHARE.RE.IN.ELECTRICITY' is the world bank series_id for percent of renewables in a countries
    energy mix. Passing in that, along with 'WB' as source would return a long dataframe with columns
    ['country','iso','forecast'], if an argument is provided to *name*, that will be used to replace 'forecast'
    
    """
 
    # --- Make API Request ---
    if source == "EIA":
        df = api_functions.eia_query_for_countries(series_id)
    elif source == "WB":
        df = api_functions.wb_query(series_id)
    
    # --- Clean Results ---
    df = helper_functions.filter_missing_data(df, how=threshold) #drop countries with >20% missing data
    
    # --- Make Forecast ---
    df = forecast_functions.make_ts_forecast(df, how=how)
    
    # --- Scale ---
    if scale != None:
        df['forecast'] = df['forecast'] * scale
    
    # --- Clean up ---
    df = df.rename({'forecast':name}, axis='columns') #rename forecast column
    df = df.drop(['value','indicator'], axis='columns') #drop value column

    return df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~ GATHER DATA THAT WILL BE USED FOR MODEL ~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
# --- EIA Sources ---
ng_production = data_gather_and_forecast(series_id="INTL.26-1-{}-BCF.A", source="EIA", name="ng_production") #units: billion cubic feet
ng_consumption = data_gather_and_forecast(series_id="INTL.26-2-{}-BCF.A", source="EIA", name="ng_consumption") #units: billion cubic feet
#ng_imports = data_gather_and_forecast("INTL.26-3-{}-BCF.A", source="EIA", name="ng_imports") #units: billion cubic feet
#ng_exports = data_gather_and_forecast("INTL.26-4-{}-BCF.A", source="EIA", name="ng_exports") #units: billion cubic feet

# --- WB Sources ---
#gdp_per_capita = data_gather_and_forecast('NY.GDP.PCAP.CD', source="WB", name="gdp_per_capita")
#ng_rents = data_gather_and_forecast('NY.GDP.NGAS.RT.ZS', source="WB", name="ng_rents")

# --- NG Reserves Without Forecast---
ng_reserves = api_functions.eia_query_for_countries("INTL.3-6-{}-TCF.A") #units: trillion cubic feet
ng_reserves = helper_functions.filter_missing_data(ng_reserves, how=0.5) #drop countries with >20% missing data
ng_reserves['value'] = ng_reserves['value'] * 1000 # trillion cubic feet >>> billion cubic feet
ng_reserves = ng_reserves.rename({'value':'reserves'}, axis='columns')
ng_reserves = ng_reserves.drop('indicator', axis='columns')

ng_reserves = helper_functions.groupby_rolling_mean(ng_reserves, value_name='reserves') #create rolling average column
ng_reserves['recoverable_reserves'] = ng_reserves['reserves_ra'] * config.NG_RECOVERY_FACTOR
ng_reserves = ng_reserves.loc[(ng_reserves['year'] >= config.START_YEAR) & (ng_reserves['year'] <= config.END_YEAR)]

#%%

# --- Merge NG Reserves with Consumption, Production ---
dfs = [ng_production, ng_consumption, ng_reserves]
merge_on=['country','iso','year']
merged = dfs[0]
for df in dfs[1:]:
    merged = merged.merge(df, on=merge_on, how='outer')
    
# --- Clean up ---
merged = merged[['country','iso','year','ng_production','ng_consumption','recoverable_reserves']]

# --- Engineer new columns ---
merged['surplus'] = merged['ng_production'] - merged['ng_consumption'] #negative = imports needed, positive = exports possible
merged['surplus_pct_production'] = merged['surplus'] / merged['ng_production']

# --- Forecast Future Reserves ---
forecast = merged.groupby(['country','iso'], as_index=False).apply(forecast_functions.forecast_reserves)

# --- Forecast Reserve Years Remaining ---
forecast['prod_yrs_remaining'] = forecast['reserves_forecast'] / forecast['ng_production']
forecast['cons_yrs_reserves'] = forecast['reserves_forecast'] / forecast['ng_consumption']

# --- Drop Rows with Missing Values ---
forecast = forecast.dropna(subset=['ng_production','ng_consumption','reserves_forecast'])

# --- Join on historic ng prices ---
forecast = helper_functions.join_historic_ng_price(forecast)

#%%
import importlib
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score
from sklearn import metrics
 
importlib.reload(stats_functions)

# --- Split in and out of sample X and y ---
in_X, in_y, out_X = stats_functions.in_out_sample_X_y(forecast, y_column='ng_price')

# --- Baseline OLS Regression ---
model = LinearRegression()
scores = cross_val_score(model, in_X, in_y, scoring='neg_mean_absolute_error', cv=5)
ols_mean = scores.mean()
print('Mean OLS RMSE: {}'.format(ols_mean))

#%%
# Henry Hub Cost per 1MT 152.83
# PHL Dec 2019 9.24 $/MBTU
