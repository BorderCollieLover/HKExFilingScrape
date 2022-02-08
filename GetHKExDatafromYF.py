# -*- coding: utf-8 -*-
"""
Created on Wed Jan 12 14:44:09 2022

@author: mtang
"""

import datetime
import pandas as pd
import yfinance as yf


DriveName = "V:"
HKFilingsDir = DriveName+"\\HKExFilings\\"
HKStockInfoDir = HKFilingsDir + "StockInfo\\"

data = pd.read_excel(HKStockInfoDir+ "ListofSecurities.xlsx")
data = data[data['Category']=='Equity']
data1 = data[data['Sub-Category']=="Equity Securities (Main Board)"]
data2 = data[data['Sub-Category']=="Equity Securities (GEM)"]
data  = pd.concat([data1, data2],ignore_index=True)
stock_codes = data['Stock Code']
stocks = [ str(x).zfill(4)+".HK" for x in stock_codes]

data = pd.read_excel(HKStockInfoDir+ "ListofSecurities.xlsx")
data = data[data['Category']=='Equity']
data = data[data['Sub-Category']=="Investment Companies"]
stock_codes = data['Stock Code']
close_end_funds = [ str(x).zfill(4)+".HK" for x in stock_codes]

data = pd.read_excel(HKStockInfoDir+ "ListofSecurities.xlsx")
data = data[data['Category']=='Exchange Traded Products']
data = data[data['Sub-Category']=="Exchange Traded Funds"]
stock_codes = data['Stock Code']
etfs = [ str(x).zfill(4)+".HK" for x in stock_codes]


data = pd.read_excel(HKStockInfoDir+ "SZAshares.xlsx", header=0)
stock_codes = data.iloc[:,4]
szashares = [ str(x).zfill(6)+".SZ" for x in stock_codes]

data = pd.read_excel(HKStockInfoDir+ "SHAshares.xlsx", header=0)
stock_codes = data.iloc[:,2]
shashares = [ str(x).zfill(6)+".SS" for x in stock_codes]

ashares = sorted(list(szashares)+list(shashares))


def GetHKExDatafromYF(tickers, filename):
    #tickers = tickers[:10]
    stock_info = yf.Tickers(tickers)
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
   
    list_of_dics = [ stock_info.tickers[ticker].info for ticker in tickers]
    equity_data = pd.DataFrame(list_of_dics)
    equity_data.index = tickers 
    
    #to_excel run into 'DataFrame' object has no attribute 'data' problem
    equity_data.to_csv(HKStockInfoDir+ filename + ".csv")
    equity_data.to_csv(HKStockInfoDir+ "Archive\\" + filename +" " + datetime.datetime.today().strftime("%Y%m%d")+".csv")
    equity_data.to_excel(HKStockInfoDir+ filename + ".xlsx",engine="openpyxl")
    equity_data.to_excel(HKStockInfoDir+ "Archive\\" + filename + " " + datetime.datetime.today().strftime("%Y%m%d")+".xlsx",engine="openpyxl")
    return equity_data


try:
    GetHKExDatafromYF(stocks, "HKSecuritiesData")
except Exception as e: 
    print(e)
    
try:    
    GetHKExDatafromYF(etfs, "HKETFData")
except Exception as e:
    print(e)
    
try:
    GetHKExDatafromYF(close_end_funds, "HKCEFData")
except Exception as e:
    print(e)

try:
    GetHKExDatafromYF(szashares, "SZSecuritiesData")
except Exception as e: 
    print(e)

try:
    GetHKExDatafromYF(shashares, "SHSecuritiesData")
except Exception as e: 
    print(e)



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

