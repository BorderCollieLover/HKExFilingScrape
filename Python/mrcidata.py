#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 17:34:39 2019

@author: mintang
"""

from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
#import pandas as pd
import datetime
import os



def ScrapeOneTable(url,outputfile):
        hdr = {'User-Agent': 'Mozilla/5.0'}
        try:
            req = Request(url,headers=hdr)
            filingpage = urlopen(req)
            soup = BeautifulSoup(filingpage)
    
            datatable = soup.find('table')
    
            with open(outputfile, "w") as file:
                file.write(str(datatable))
        except Exception as e:
            print(e)
            

def ScrapeMRCIdata():
    curDt = datetime.datetime.now()
    new_files = []
    
    while True:
        filename = '/Users/shared/MRCI/Raw/'+ curDt.strftime('%Y%m%d') + '.html'
        if os.path.isfile(filename):
            break;
        else:
            new_files += [filename]
        fileurl = 'https://www.mrci.com/ohlc/' + curDt.strftime('%Y') + '/' + curDt.strftime('%-y%m%d')+'.php'
        print(fileurl)
        ScrapeOneTable(fileurl, filename)
    
        curDt = curDt -datetime.timedelta(1)
        if curDt.year < 1998:
            break
    print(new_files)
    return(new_files)

New_MRCI_FILES = ScrapeMRCIdata()

#print(range(2000,2019))