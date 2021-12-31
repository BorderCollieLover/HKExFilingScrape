# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 13:13:59 2021

@author: mtang
"""

#Holding History
#To extract the entire holdings history of one underlying by one investor

import pandas as pd

HKFilingsDir = "X:\\HKExFilings\\"
FilingsByTickerDir = HKFilingsDir + "FilingsByTicker\\"


def holding_history_by_investor (ticker, investor):
    #from the filing by ticker file, extract the holding history of a particular investor
    ticker_filing_file = FilingsByTickerDir + ticker + ".csv"
    ticker_filing_header = ['Date', 'Code', 'Investor', 'Shares', 'Currency', 'HighPrice', 'AvgPrice', 'OffExHighPrice', 'OffExAvgPrice', 'LongPos', 
                            'LongPct', 'ShortPos', 'ShortPct', 'LongPosPre', 'LongPctPre', 'ShortPosPre', 'ShortPctPre', 'FilingURL', 'FileDate' ]
    data = pd.read_csv(ticker_filing_file, header=None)
    data.columns = ticker_filing_header
    data = data[data.Investor==investor]
    output_data = data[['Date', 'Code', 'Shares', 'HighPrice', 'AvgPrice', 'OffExHighPrice', 'OffExAvgPrice','LongPos', 
                            'LongPct', 'ShortPos', 'ShortPct', 'LongPosPre', 'LongPctPre', 'ShortPosPre', 'ShortPctPre', 'FileDate', 'FilingURL', 'FileDate' ]]
    
    return(output_data)
    


def period_holding_changes(holding_history, from_dt, to_dt):
    #examine the change in holdings over the period from from_dt to to_dt 
    #use as input the output from holding history by investor
    #try to ascertain a) the actual change in shares for the given period of time; b) the number of reported on/off exchange purchases and average price, 
    # c) the number of non-cash changes; and c) differences or residuals, i.e. changes in shares but not explicitly reported
    
    
    
    return()