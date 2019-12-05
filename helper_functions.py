#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 13:26:04 2019

@author: skoebric
"""

# --- Package Imports ---
import pandas as pd
import numpy as np
import pycountry
 
# --- Module Imports ---
import config


def country_name_to_iso3(country_name):
    try:
        return pycountry.countries.get(name=country_name).alpha_3
    except Exception:
        return None

def iso3_to_country_name(iso3):
    try:
        return pycountry.countries.get(alpha_3=iso3).name
    except Exception:
        return None


def filter_missing_data(df, how=None, value_column='value', drop_columns=['pct_null']):
    """
    Drop or fill missing values. 
    
    Inputs
    ------
    df (pd.DataFrame) - Usually the output of a wb_query (or queries)
    
    how (None, str, float) -
         None : do nothing (convert zeros to nan)
        'drop' : drop all missing values (including zeros)
         float : threshold of years per country/indicator to drop
            i.e. if 'Bhutan' has 2/3rds nans for a given indicator, and thresh is set as 0.5
            bhutan will be dropped for the given indicator.
            
    """
   
    df.loc[df['value'] == 0, 'value'] = None
   
    if how == 'drop':
        df = df.dropna(subset=['value'])
    
    elif isinstance(how, float):
        # --- Calc percent that is null for each country/indicator combo ---
        grouped = df[['country','indicator','value']]
        grouped = grouped.groupby(['country','indicator']).apply(lambda x: x['value'].notnull().mean())
        grouped.name = 'pct_null'
        grouped = grouped.reset_index(drop=False)
        
        # --- Merge grouped back onto df ---
        df = df.merge(grouped, on=['country','indicator'], how='left')
        
        # --- Filter based on how value ---
        df = df.loc[df['pct_null'] >= how]
    
    # --- Clean up ---
    if len(drop_columns) > 0:
        df = df.drop(drop_columns, axis='columns', errors='ignore')
    
    return df

def groupby_rolling_mean(df, value_name, merge_on=['country','iso'], periods=5, min_periods=1):
    """
    Apply a rolling mean to a long df with mutliple groups. 
    
    Inputs
    ------
    df with columns from merge_on, value_name, and 'year'
    
    Outputs
    -------
    df with a _ra column appended
    """
    
    # --- Sort ---
    df.sort_values(['country','year'], ascending=True, inplace=True)
    new_value_name = value_name + '_ra'
    
    # --- Apply ---
    def rolling_mean(series):
        series = series.rolling(periods, min_periods=min_periods).mean()
        return series
    
    df[new_value_name] = df.groupby(merge_on)[value_name].transform(rolling_mean) #rolling average of reserves
    
    # --- Clean up ---
    df = df[merge_on + ['year', value_name, new_value_name]]
    
    return df
