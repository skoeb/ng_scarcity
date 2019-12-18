#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  6 12:22:46 2019

@author: skoebric
"""

# --- Package Imports ---
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score
from sklearn import metrics
 
# --- Module Imports ---
import config

def in_out_sample_X_y(df, y_column,
                      drop_columns=['recoverable_reserves','continent','hub_ng_price','gasoline_cost',
                                    'hub_gasoline_cost','scaler','country','iso'],
                      dummy_columns=['hub']):
    """
    Split in and out of sample along X and y columns.
    """
    
    df = df.reset_index(drop=True)
    
    df = df.drop(drop_columns, axis='columns')
    
    if len(dummy_columns) > 0:
        dummies = []
        for c in dummy_columns:
            dummy = pd.get_dummies(df[c])
            dummies.append(dummy)
        dummy_df = pd.concat(dummies, axis='columns')
        df = pd.concat([df, dummy_df], axis='columns')
        df = df.drop(dummy_columns, axis='columns')
    
    # --- Pull out in-sample rows ---
    in_sample = df.loc[~df['ng_price'].isna()]
    in_sample = in_sample.loc[in_sample['year'] < 2019]
    
    # --- Pull out out-sample rows ---
    out_sample = df.loc[df['ng_price'].isna()]

    
    # --- Split into X and y ---
    X_columns = list(df.columns)
    X_columns.remove(y_column)
    
    in_sample_X = in_sample[X_columns]
    out_sample_X = out_sample[X_columns]
    in_sample_y = in_sample[y_column]
    
    return in_sample_X, in_sample_y, out_sample_X
    
