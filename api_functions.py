#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 13:25:24 2019

@author: skoebric
"""
# --- Python Library Imports ---
import datetime
import requests
import json
import concurrent.futures as cf
import os

# --- Package Imports ---
import pandas as pd
import numpy as np
import wbdata as wb
import pycountry
    
# --- Module Imports ---
import config
import helper_functions

def wb_query(indicator, countries='all', start_year=config.START_YEAR, end_year=config.END_YEAR, iso3=True):
    """
    Retrieve data for a world bank indicator,
    For all world bank indicators, see:
    https://data.worldbank.org/indicator
    
    Credit to wbdata for a wrapper around API requests: https://wbdata.readthedocs.io/en/latest/
    
    Input
    -----
    indicator (str) - the world bank indicator as a string, i.e. '4.1_SHARE.RE.IN.ELECTRICITY' is share of RE in a countries generation mix.
    countries (list) - Default 'all' will pull all countries, if specifying a single country or a list of countries, double check spelling w/ world bank
    start_year (int) - First year to pull data from, most world bank indicators are reported on an annual basis.
    end_year (int) - Last year to pull data from, most world bank indicators are reported on an annual basis.
        
    Outputs
    -------
    Pandas DataFrame - Long, with columns for 'country','date', and 'value'
    """
    
    # --- Define daterange as tuple ---
    daterange = (datetime.datetime(start_year, 1, 1), datetime.datetime(end_year, 12, 31))
    
    # --- Make api requests ---
    wb_query = wb.get_data(indicator, data_date=daterange, country=countries)
    
    # --- Package results as a df ---
    country_list = [d['country']['value'] for d in wb_query]
    year_list = [d['date'] for d in wb_query]
    value_list = [d['value'] for d in wb_query]
    
    df = pd.DataFrame({'country':country_list,
                       'year':year_list,
                       'value':value_list})
    df['indicator'] = indicator
    df['value'] = df['value'].astype(float)
    df['year'] = df['year'].astype(int)
    
    if iso3:
        df['iso'] = df['country'].apply(helper_functions.country_name_to_iso3)
        
    return df

def eia_query(series_id, country, pandas_out=True, api_key=config.EIA_API_KEY, country_name=True):
    """
    Helper function to make EIA requests, provided a series_id.

    Inputs
    ------
    - series_id (str): Looks something like 'TOTAL.CLTCBUS.A'
    - pandas_out (bool): Return a pd.DataFrame(), or a json object.
    - API_KEY (str): Store this in config.py, free from EIA.gov
    
    Outputs
    -------
    pd.DataFrame()
    """
    
    # --- Format series_id and make request ---
    series_id_formatted = series_id.format(country)
    url = f'http://api.eia.gov/series/?api_key={api_key}&series_id={series_id_formatted}'
    r = requests.get(url, verify = False)
    
    # --- Process API request ---
    if r.ok:
        j = json.loads(r.content)
    else: r.raise_for_status()

    if pandas_out:
        if 'series' in j.keys():
            data = j['series'][0]['data']
            df = pd.DataFrame(data, columns = ['year','value'])
            df.loc[df['value'].isin(['--','NA']), 'value'] = None
            df['value'] = df['value'].astype(float)
            df['year'] = df['year'].astype(int)
            df['iso'] = country
            df['indicator'] = series_id
            
            if country_name:
                df['country'] = df['iso'].apply(helper_functions.iso3_to_country_name)
        
        else:
            return pd.DataFrame(columns=[])
        return df
    else:
        return j

def eia_query_for_countries(series_id, countries='all'):
    """
    Wrapper around eia_query to run for multiple countries.
    
    Inputs
    ------
    - series_id (str): should include a {} which can be formatted with a country sub-string.
    - countries ('all' or list): list of iso3 codes, note that the eia doesn't have data for every country
    
    Outputs
    -------
    pd.DataFrame() - Long
    """
    
    if countries=='all':
        countries = [c.alpha_3 for c in pycountry.countries]
        
    # --- Submit all country api requests with concurrent.futures ---
    completed_dfs = []
    with cf.ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(eia_query, series_id, country) for country in countries]
    
        for f in cf.as_completed(futures):
            completed_dfs.append(f.result())
    
    df = pd.concat(completed_dfs, axis='rows')
    
    return df

def load_bp_ng_historical_prices():
    """
    Full Report on page 37: https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2019-full-report.pdf
    """
    
    df = pd.read_csv(os.path.join('prices','bp_ng_cost_million_btu.csv'))
    df = df.fillna(np.nan)
    
    # -- Convert to price per meter cube ---
    df = df.set_index('year')
    df = df / config.METERS_CUBED_MBTU
    
    # --- Clean up ---
    df['jkm'] = df['jkm'].fillna(df['japan'])
    df = df[['jkm','ttf','henryhub']]
    df = df.reset_index()
    
    # --- Make long ---
    df = df.melt(id_vars='year', var_name='hub', value_name='hub_ng_price')
    
    return df

def get_gasoline_pump_price():
    """ Not currently used. """
    df = wb_query('EP.PMP.SGAS.CD')

    # --- Clean Results ---
    df = helper_functions.filter_missing_data(df, how=0.2) #drop countries with >20% missing data
    
    # --- Clean up ---
    df = df.rename({'value':'gasoline_cost'}, axis='columns')
    df['iso'] = df['iso'].astype(str)
    df = df.loc[~df['iso'].isin(['None',None,np.nan])]
    
    # --- Fill na ---
    df = df.sort_values(['country','year'], ascending=True)
    df['gasoline_cost'] = df.groupby(['country','iso'])['gasoline_cost'].apply(lambda x: x.ffill().bfill())
    
    # --- More clean up ---
    df = df[['year','country','iso','gasoline_cost']]

    return df

