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
#to_excel run into 'DataFrame' object has no attribute 'data' problem
equity_data.to_csv(HKStockInfoDir+ "HKSecuritiesData.csv")
equity_data.to_csv(HKStockInfoDir+ "Archive\\HKSecuritiesData " + datetime.datetime.today().strftime("%Y%m%d")+".csv")
equity_data.to_excel(HKStockInfoDir+ "HKSecuritiesData.xlsx",engine="openpyxl")
equity_data.to_excel(HKStockInfoDir+ "Archive\\HKSecuritiesData " + datetime.datetime.today().strftime("%Y%m%d")+".xlsx",engine="openpyxl")


# This is tested for breaking data down into blocks but it seems the above method works -- just taking a long time
# block_num = 100
# block_size = len(data.index)// block_num

# for i in range(block_num):
#     if i== block_num -1 : 
#         ticker_set = tickers[i*block_size:(len(data.index)-1)]
#     else:
#         ticker_set = tickers[i*block_size:((i+1)*block_size-1)]
    
#     list_of_dics = [ stock_info.tickers[ticker].info for ticker in ticker_set]
    
#     equity_data = pd.DataFrame(list_of_dics)
#     equity_data.index = ticker_set
    
#     equity_data.to_excel(HKStockInfoDir+ str(i) + " HKSecuritiesData.xlsx",engine="openpyxl")
#     equity_data.to_excel(HKStockInfoDir+ str(i) + " HKSecuritiesData " + datetime.datetime.today().strftime("%Y%m%d")+".xlsx",engine="openpyxl")

