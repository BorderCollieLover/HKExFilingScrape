# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 14:44:09 2022

@author: mtang
"""

from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import re
import csv
import os
import pandas as pd
from urllib.error import URLError, HTTPError
from operator import itemgetter
import locale
import shutil
from glob import glob
import numpy as np
import yfinance as yf

HKFilingsDir = "X:\\HKExFilings\\"
HKStockInfoDir = HKFilingsDir + "StockInfo\\"

data = pd.read_excel(HKStockInfoDir+ "ListofSecurities.xlsx")
data = data[data['Category']=='Equity']
data1 = data[data['Sub-Category']=="Equity Securities (Main Board)"]
data2 = data[data['Sub-Category']=="Equity Securities (GEM)"]
data  = pd.concat([data1, data2],ignore_index=True)
stock_codes = data['Stock Code']
tickers = [ str(x).zfill(4)+".HK" for x in stock_codes]
data.index = tickers 
stock_info = yf.Tickers(tickers)
stock_info.tickers['0005.HK']
#stock_industry = [stock_info.tickers[ticker].info['industry'] for ticker in tickers]
#for i in range(len(tickers)):
    
    
# for i in range(10):
#     ticker = tickers[i]
#     try:
#         for data_key in stock_info.tickers[ticker].info.keys():
#             try:
#                 data.at[i,data_key] = stock_info.tickers[ticker].info[data_key]
#             except Exception as e: 
#                 print(e)
#     except Exception as e:
#         print(e)
                
#small_tickers = tickers[:10]
#list_of_dics = [ stock_info.tickers[ticker].info for ticker in small_tickers]
list_of_dics = [ stock_info.tickers[ticker].info for ticker in tickers]
equity_data = pd.DataFrame(list_of_dics)
equity_data.index = tickers 
equity_data.to_excel(HKStockInfoDir+ "HKSecuritiesData.xlsx")
equity_data.to_excel(HKStockInfoDir+ "HKSecuritiesData " + datetime.datetime.today().strftime("%Y%m%d")+".xlsx")




    # try:
    #     print(ticker, stock_info.tickers[ticker].info['industry'])
    #     data.iloc[i]['Industry'] = stock_info.tickers[ticker].info['industry']
    # except Exception as e:
    #     print(ticker, e)
    #     data.iloc[i]['Industry'] = ''
        
    # try:
    #     print(ticker, stock_info.tickers[ticker].info['industry'])
    #     data.iloc[i]['Industry'] = stock_info.tickers[ticker].info['industry']
    # except Exception as e:
    #     print(ticker, e)
    #     data.iloc[i]['Industry'] = ''
        
    
    
    
        
    
        

#data['Industry'] = stock_industry

#output_data = data[['Stock Code', 'Name of Securities', 'Industry']]