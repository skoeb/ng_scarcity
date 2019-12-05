#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 17:08:54 2019

@author: skoebric
"""
# --- Package Imports ---
import pandas as pd
import numpy as np
import pycountry
 
# --- Module Imports ---
import config

def apply_ts_forecast_to_group(df, start_year, end_year, how):
    """Function to apply make_ts_forecast on groupby object."""
    
    # --- Create datetime index ---
    df.index = pd.to_datetime(df['year'], format='%Y')
    df = df.reindex(pd.date_range("{}-01-01".format(start_year), "{}-01-01".format(end_year), \
                              freq='AS'), fill_value=np.nan)
    
    # --- Fill in static columns ---
    df['year'] = df.index.year
    
    # --- Impute values column ---
    df['value'] = df['value'].astype(float)
    df['forecast'] = df['value'].interpolate(method='spline', order=1, limit_direction='both')
    
    return df
    
    
def make_ts_forecast(df, group_by_columns=['country','iso','indicator'],
                     start_year=config.START_YEAR, end_year=config.END_YEAR,
                     how='spline'):
    """
    Make a time series forecast for a non-stationary series. 
    
    This will work well for things that a decent analyst would say will constantly move in one direcetion,
    without exogenous intervention e.g.:
        - Electricity Consumption
        - Natural Gas Consumption
        - Population Growth
    
    This should be used with caution on variables with exogenous interaction, eg.:
        - Natural Gas Imports
        - Natural Gas Reserves
        - GDP Per Capita
    
    Inputs
    ------
    df: Contains a 'value' and 'year' column. df is *long* meaning it can contain multiple countries and indicators. 
    group_by_columns: Columns that will be used to group the dataframe, with each unique combination recieving its own forecast.
    start_year: First year of data (within the sample)
    end_year: Last year of forecast (out of sample)
    how: Passed to pandas.df.interpolate, only 'spline' is currently tested.
    """
    
    grouped = df.groupby(group_by_columns)[['year','value']].apply(lambda x: apply_ts_forecast_to_group(x, start_year=start_year, end_year=end_year, how=how))
    grouped = grouped.reset_index()
    
    # --- Clip Lower ---
    grouped['value'] = grouped['value'].clip(lower=0)
    
    # --- Clean up ---
    grouped = grouped.drop('level_3', axis='columns', errors='ignore')
    
    return grouped

def forecast_reserves(df):
    """
    Forecast future NG reserves:
        1) Hold the most recently observed reserves quantity static (potentially unrealistic depending on country)
        2) Calculate cumulative sum of future ng_production forecast
        3) Subtract future produciton from future reserves
    
    Meant to be applied through a groupby.
    """
    
    # --- Forecast reserves with forward fill ---
    df['reserves_forecast'] = df['recoverable_reserves'].ffill()
    #TODO: Is there a better way to do this? currently, we're just taking the last reserve value (2017) and assuming that is constant
    
    # --- Subtract cumulative future produciton from forecasted reserves ---
    df.loc[df['recoverable_reserves'].isnull(), 'cum_future_production'] = df.loc[df['recoverable_reserves'].isnull(), 'ng_production'].cumsum()
    df['reserves_forecast'] = df['reserves_forecast'] - df['cum_future_production']
    
    # --- Clean up ---
    df['reserves_forecast'] = df['reserves_forecast'].fillna(df['recoverable_reserves'])
    df = df.drop(['cum_future_production'], axis='columns')
    
    return df