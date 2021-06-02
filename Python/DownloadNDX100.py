# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 17:48:57 2018

@author: mintang
"""

#Note that the NDX website clears the csv file about 3 hours before market open 
#So always run this script after market closes but at least 3 hours before market opens

import os
import urllib.request
import csv
from datetime import datetime
import sys
sys.path.insert(0, '/Users/Shared/Models/Python/')
import FileToolsModule as FTM


def DownloadNDXFile():
    srcFile = 'https://www.nasdaq.com/quotes/nasdaq-100-stocks.aspx?render=download'
    tgtFile = '/Users/Shared/USMarkets/NDX/NDX100/'+datetime.strftime(datetime.now().date(), "%Y%m%d")+'.csv'
    if  os.path.isfile(tgtFile):
        os.remove(tgtFile)
    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
    except Exception as e:
        print(tgtFile)
        print(e)
        return
    
    
    latest_file = '/Users/Shared/Models/NDX100.csv'
    if  os.path.isfile(latest_file):
        os.remove(latest_file)
    try:
        urllib.request.urlretrieve(srcFile, latest_file)
    except Exception as e:
        print(latest_file)
        print(e)
        return
    return

def DownloadNDXStockLists():
    srcFile = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download'
    tgtFile = '/Users/Shared/USMarkets/NDX/NASDAQ/'+datetime.strftime(datetime.now().date(), "%Y%m%d")+'.csv'
    if  os.path.isfile(tgtFile):
        os.remove(tgtFile)
    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
        latest_file = '/Users/Shared/USMarkets/NDX/NASDAQ.csv'
        if  os.path.isfile(latest_file):
            os.remove(latest_file)
        urllib.request.urlretrieve(srcFile, latest_file)    
    except Exception as e:
        print(tgtFile)
        print(e)
        
        
        
    srcFile = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download'
    tgtFile = '/Users/Shared/USMarkets/NDX/NYSE/'+datetime.strftime(datetime.now().date(), "%Y%m%d")+'.csv'
    if  os.path.isfile(tgtFile):
        os.remove(tgtFile)
    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
        latest_file = '/Users/Shared/USMarkets/NDX/NYSE.csv'
        if  os.path.isfile(latest_file):
            os.remove(latest_file)
        urllib.request.urlretrieve(srcFile, latest_file)    
    except Exception as e:
        print(tgtFile)
        print(e)
        

    srcFile = 'https://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download'
    tgtFile = '/Users/Shared/USMarkets/NDX/AMEX/'+datetime.strftime(datetime.now().date(), "%Y%m%d")+'.csv'
    if  os.path.isfile(tgtFile):
        os.remove(tgtFile)
    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
        latest_file = '/Users/Shared/USMarkets/NDX/AMEX.csv'
        if  os.path.isfile(latest_file):
            os.remove(latest_file)
        urllib.request.urlretrieve(srcFile, latest_file)    
    except Exception as e:
        print(tgtFile)
        print(e)
        
    
    return
        


    

def UpdateBarDataTicker():
    ndx_file = '/Users/Shared/Models/NDX100.csv'
    ndx_tickers = []
    if os.path.isfile(ndx_file):
        try:
            with open(ndx_file, 'r') as csvfile:
                f_csv = csv.reader(csvfile)
                next(f_csv)
                tickerlines = [tuple(line) for line in f_csv ]
                tickerlines = [tuple(line) for line in tickerlines if ("_" not in line[0])]
                ndx_tickers = list(set([line[0] for line in tickerlines]))
        except Exception as e:
            print(e)
            return
    print(len(ndx_tickers))
    print(ndx_tickers)

    ticker_file="/Users/Shared/IBBarData/BarDataTickers.csv"
    curTickers = []
    if os.path.isfile(ticker_file):
        try:
            with open(ticker_file, 'r') as csvfile:
                tickerlines = [tuple(line) for line in csv.reader(csvfile)]
                tickerlines = [tuple(line) for line in tickerlines if ("_" not in line[0])]
                curTickers = list(set([line[0] for line in tickerlines]))
        except Exception as e:
            print(e)
            return
    
    cur_ticker_len = len(curTickers)
    curTickers = sorted(list(set(curTickers + ndx_tickers)))
    new_ticker_len = len(curTickers)
    
    if (new_ticker_len > cur_ticker_len):
        outputlist = []
        for ticker in curTickers:
            outputlist += [(ticker,)]
            FTM.SafeSaveData(ticker_file, outputlist)

    return
    

DownloadNDXFile()
DownloadNDXStockLists()
UpdateBarDataTicker()


