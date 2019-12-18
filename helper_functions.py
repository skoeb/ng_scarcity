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
import pycountry_convert as pcc
 
# --- Module Imports ---
import config
import api_functions

pd.options.mode.chained_assignment = None


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

def country_to_continent(country_name):
    "Convert country name string to continent"
    country_alpha2 = pcc.country_name_to_country_alpha2(country_name)
    country_continent_code = pcc.country_alpha2_to_continent_code(country_alpha2)
    country_continent_name = pcc.convert_continent_code_to_continent_name(country_continent_code)
    return country_continent_name


def join_historic_ng_price(df, source='BP', map_dict=config.BP_GAS_HUB_DICT, map_column='continent',
                           scaler='gas_pump_price'):
    """
    Create a new 'ng_price' column for historical data points based on a mapping of a column
    (i.e. continent) with a second datafame with annual prices.
    
    Inputs
    ------
    df - Contains a 'country' column and a 'year' column.
    source - Only BP available now; see load_bp_ng_historical_prices()
    map_dict - map of continents to most economically influential hub
    map_column - column containing keys of map_dict
    scaler - domestic column to scale regional hub ng price by
    
    Outputs
    -------
    Dataframe with historical gas prices added on as 'ng_price'
    """
    
    if source == 'BP':
        gas_price_df = api_functions.load_bp_ng_historical_prices()
    else:
        raise NotImplementedError
        
    if map_column == 'continent':
        # --- Find Continent for each country ---
        df['continent'] = df['country'].apply(country_to_continent)
        df['hub'] = df[map_column].map(map_dict)
    else:
        raise NotImplementedError
    
    # --- merge on gas_price_df ---
    df = df.merge(gas_price_df, on=['year','hub'], how='left')
    
    if scaler == 'gas_pump_price':
        
        # --- merge on scaler column ---
        hub_country_dict = {'Netherlands':'ttf',
                            'Japan':'jkm',
                            'United States':'henryhub'}
        
        gasoline_pump_price = api_functions.get_gasoline_pump_price()
        
        df = df.merge(gasoline_pump_price, on=['year','country','iso'], how='left')
        
        # --- merge on country/hub column ---
        hub_gasoline_pump_price = gasoline_pump_price.loc[gasoline_pump_price['country'].isin(list(hub_country_dict.keys()))]
        hub_gasoline_pump_price['hub'] = hub_gasoline_pump_price['country'].map(hub_country_dict)
        hub_gasoline_pump_price.rename({'gasoline_cost':'hub_gasoline_cost'}, axis='columns', inplace=True)
        hub_gasoline_pump_price = hub_gasoline_pump_price[['year','hub','hub_gasoline_cost']]
        
        df = df.merge(hub_gasoline_pump_price, on=['year','hub'], how='left')
        
        # --- Scale based on gasoline cost ---
        df['scaler'] = df['gasoline_cost'] / df['hub_gasoline_cost']
        df['ng_price'] = df['scaler'] * df['hub_ng_price']
        
        # --- Clean up ---
        df.loc[df['hub_ng_price'].isna(), ['hub_gasoline_cost','gasoline_cost','scaler']] = np.nan
        
        return df
    
    else:
        df['ng_price'] = df['hub_ng_price']
        return df
        
    
