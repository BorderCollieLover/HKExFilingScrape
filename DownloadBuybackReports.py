# -*- coding: utf-8 -*-
"""
Created on Thu Jun  3 09:06:20 2021

@author: mtang
"""
import os
import requests
import pandas as pd 
from pandas.tseries.offsets import BDay

HKFilingsDir = "c:\\users\\mtang\\HKEx\\"


def DownloadBuyBackReports():
    
    #Prior to 2003.03.31 Buyback reports are in PDF files 
    cutoffdt = pd.datetime(2003,3,31)
    ## Share repurchase reports from HKEx
    curDt = pd.datetime.today()
    failCtr = 0
    while (curDt > cutoffdt):
        filename = HKFilingsDir + "Repurchase\\" + curDt.strftime('%Y%m%d') + '.xls'
        if os.path.isfile(filename):
            break;
    
        #if os.path.isfile(filename):
        #    curDt = curDt - BDay(1)
        #    continue;
        
        fileurl = 'https://www.hkexnews.hk/reports/sharerepur/documents/SRRPT' + curDt.strftime('%Y%m%d') + '.xls'
    ##    fileurl = 'http://www.hkexnews.hk/reports/sharerepur/documents/' + curDt.strftime('%Y%m%d') + '.pdf'
        print(fileurl)
        try:
            resp = requests.get(fileurl, verify=False)
            if resp.raise_for_status() is None:
                with open(filename, 'wb') as output:
                    output.write(resp.content)
        except Exception as e:
            print(e)
            failCtr += 1
        
        curDt = curDt - BDay(1)
    
    #download repurchase reports of GEM stocks in HKEx
    curDt = pd.datetime.today()
    failCtr = 0
    while (curDt > cutoffdt):
        filename = HKFilingsDir + "GEMRepurchase\\"+ curDt.strftime('%Y%m%d') + '.xls'
        if os.path.isfile(filename):
            break;
    
        #if os.path.isfile(filename):
        #    curDt = curDt - BDay(1)
        #    continue;
        
    
        fileurl = 'https://www.hkexnews.hk/reports/sharerepur/GEM/documents/SRGemRPT' + curDt.strftime('%Y%m%d') + '.xls'
        print(fileurl)
        try:
            resp = requests.get(fileurl, verify=False)
            if resp.raise_for_status() is None:
                with open(filename, 'wb') as output:
                    output.write(resp.content)
        except Exception as e:
            print(e)
            
        curDt = curDt - BDay(1)
            

