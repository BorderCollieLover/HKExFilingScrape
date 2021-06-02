#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 09:23:20 2018

@author: mintang
"""
import pandas as pd
import csv
import sys
sys.path.insert(0, '/Users/Shared/Models/Python/')
import UpdateQuandlData
import FileToolsModule as FTM
import datetime
import urllib.request
import os
import zipfile
import subprocess


LIQUIDITYDAYS = 30
QUANDLPATH =  "/Users/Shared/Quandl/EOD/Daily/"
LIQUIDITY_CUTOFF = 20000

def DownloadEODCodes():
    #url = 'https://www.quandl.com/api/v3/databases/EOD/metadata?api_key=3ojxoNzaKndioRPW_hXc'
    srcFile='https://s3.amazonaws.com/quandl-production-static/end_of_day_us_stocks/ticker_list.csv'
    tgtFile = '/Users/Shared/Quandl/EOD/Tickers/'+datetime.datetime.strftime(datetime.datetime.now().date(), "%Y%m%d")+'.csv'
    if not os.path.isfile(tgtFile):
        try:
            urllib.request.urlretrieve(srcFile, tgtFile)
        except Exception as e:
            print(tgtFile)
            print(e)
        

    srcFile='https://www.quandl.com/api/v3/databases/EOD/metadata?api_key=3ojxoNzaKndioRPW_hXc'
    tgtFile = '/Users/Shared/Quandl/EOD/MetaData/'+datetime.datetime.strftime(datetime.datetime.now().date(), "%Y%m%d")+'.csv.gz'
    if not os.path.isfile(tgtFile):
        try:
            urllib.request.urlretrieve(srcFile, tgtFile)
            with zipfile.ZipFile(tgtFile, "r") as zip_ref:
                zip_ref.extractall('/Users/Shared/Quandl/EOD/MetaData/')
            src_file = '/Users/Shared/Quandl/EOD/MetaData/EOD_metadata.csv'
            output_file = '/Users/Shared/Quandl/EOD/MetaData/' +datetime.datetime.strftime(datetime.datetime.now().date(), "%Y%m%d")+'.csv'
            subprocess.call('mv %s %s' %(src_file, output_file), shell=True)
            os.remove(tgtFile)
            data = pd.read_csv(output_file, header=0, index_col=0)
            keep_columns = ['name', 'from_date', 'to_date']
            data = data[keep_columns]
            data.to_csv(output_file)
            default_file = '/Users/Shared/Quandl/EOD/MetaData.csv'
            subprocess.call('cp %s %s' %(output_file, default_file), shell=True)
        except Exception as e:
            print(tgtFile)
            print(e)

    return


def CheckQuandlLiquidity(ticker):
    
    try:
        UpdateQuandlData.UpdateEODData(ticker)
    except Exception as e: 
        print(e)
        
    quandl_file = QUANDLPATH+ticker+".csv"
    quandl_data = None
    try:
        quandl_data = pd.read_csv(quandl_file, header=0, index_col=0,parse_dates=[0])
    except Exception as e: 
        print(e)
        if (os.path.isfile(quandl_file)):
            os.remove(quandl_file)
        return(0)
        
    (nrow, ncol) = quandl_data.shape
    if (nrow < 2*LIQUIDITYDAYS):
        return(0)
        
    UpdateQuandlData.DeduplicateEODData(ticker)
    lastDt = quandl_data.index[nrow-1]
    #print(lastDt)
    
    if ((datetime.datetime.now() - lastDt).days > LIQUIDITYDAYS):
        return(0)
        
    
    return((quandl_data.Volume[-LIQUIDITYDAYS:]).mean())
    
def UpdateLiquidityfromList(tickers, outputfile, sort_by_liquidity=True):
    
    output_data = []
    for ticker in tickers: 
        #print(ticker)
        ticker_liquidity = 0 
        try:
            ticker_liquidity = CheckQuandlLiquidity(ticker)
        except Exception as e:
            print(e)
            
        output_data += [(ticker, ticker_liquidity)]
    
    if sort_by_liquidity:
        output_data= sorted(output_data, key=lambda s:s[1], reverse=True)
        
    FTM.SafeSaveData(outputfile, output_data)
    return(output_data)
    
def UpdateLiquidityfromFile (inputfile,outputfile,  ticker_col_idx=0, sort_by_liquidity=True):
    tickers = []
    try:
        with open(inputfile, 'r') as csvfile:
            tickerlines = [tuple(line) for line in csv.reader(csvfile)]
            tickers = list(set([line[ticker_col_idx] for line in tickerlines]))
    except Exception as e:
        print(e)
        
    if (len(tickers)>1):
        return(UpdateLiquidityfromList(tickers, outputfile, sort_by_liquidity))
        
        
def UpdateQuandlLiquidity():
    DownloadEODCodes()
    quand_ticker_file ='/Users/Shared/Quandl/EOD/MetaData.csv'
    UpdateLiquidityfromFile(quand_ticker_file, "/Users/Shared/Quandl/EOD/QuandlTickersLiquidity.csv", 0)
    
    
#etf_ticker_file = "/Users/Shared/Models/All US ETFs and ETNs.csv"
#UpdateLiquidityfromFile(etf_ticker_file, "/Users/Shared/Models/ETFLiquidity.csv", 1)
#bar_ticker_file="/Users/Shared/IBBarData/BarDataTickers.csv"
#UpdateLiquidityfromFile(bar_ticker_file, "/Users/Shared/Models/BarTickersLiquidity.csv", 0)
#quand_ticker_file = "/Users/Shared/Quandl/EOD/tickers.txt"
#UpdateLiquidityfromFile(quand_ticker_file, "/Users/Shared/Models/QuandlTickersLiquidity.csv", 0)

        
#print(CheckQuandlLiquidity("CLY"))
    
    
#main_etfs = ['SPY','QQQ','GLD','SLV','USO','VXX','VXZ',"EWJ","IBB","FXI","XRT","XLE","XOP","OIH","AMLP","XLF","XLV","XLK","XLY","XLI","XLU","XLP","GDX","XLRE","XLB","EWZ","ASHR"]
#UpdateLiquidityfromList(main_etfs, "/Users/Shared/Models/ETFLiquidity.csv")
    
    
    
    
UpdateQuandlLiquidity()
