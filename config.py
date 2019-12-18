#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 11:52:59 2019

@author: skoebric
"""

EIA_API_KEY = 'e29ec60601eaa48657d0571969d0830a'

START_YEAR = 2005
END_YEAR = 2050

NG_RECOVERY_FACTOR = 0.85 #https://petrowiki.org/Dry_gas_reservoirs

METERS_CUBED_MBTU = 28.263
#1380 cubic meters per ton when vaporized, 2.12 cubic meters for LNG
#1 million BTU = 28.263 m3

BP_GAS_HUB_DICT = {'Africa':'ttf',
                  'Asia':'jkm',
                  'Europe':'ttf',
                  'North America':'henryhub',
                  'Oceania':'jkm',
                  'South America':'henryhub'}