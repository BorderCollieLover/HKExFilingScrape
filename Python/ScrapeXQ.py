#!/usr/local/bin/python3.4

##Sample: 
##https://adesquared.wordpress.com/2013/06/16/using-python-beautifulsoup-to-scrape-a-wikipedia-table/
##about adding header/agents :
##https://docs.python.org/3.4/howto/urllib2.html#id10
##https://docs.python.org/3.4/library/urllib.request.html#urllib.request.Request

##my machine browser agent:
##https://techblog.willshouse.com/2012/01/03/most-common-user-agents/
##Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
import datetime
import re
import html
from dateutil import parser
import csv
import os
import pandas as pd
from pandas.tseries.offsets import BDay
from pandas import DataFrame
import gzip
import multiprocessing
from urllib.error import URLError, HTTPError
from pytz import timezone
import pytz
import unicodedata
import sys
import locale
import getpass
import logging
import time
import random
from operator import itemgetter
import subprocess
import requests
import threading
from bdateutil import relativedelta
import holidays
from urllib.parse import urlencode
import ast
from socket import timeout
import socket


# timeout in seconds
timeout = 5
socket.setdefaulttimeout(timeout)


stockpage = 'http://www.xueqiu.com/S/SZ002039'
stockpage = 'http://www.xueqiu.com/S/CYD'
stockpage = 'http://www.xueqiu.com/S/ABN-F'

values = {'name' : 'Michael Foord',
          'location' : 'Northampton',
          'language' : 'Python' }
data  = urlencode(values)
data = data.encode('utf-8')
##see above comments on webpage to get current user_agent
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'
headers = { 'User-Agent' : user_agent }


def ScrapeOneTicker(dataurl):
    try:
        Req = Request(dataurl, data, headers)
        dataPage = urlopen(Req).read()
        soup = BeautifulSoup(dataPage)
##        pbdata = soup.findAll('td', text=re.compile(u'市净率'))
        tmpdata = soup.find(text=re.compile('SNB\.data\.quote'))
##    except (HTTPError, URLError) as e:
##        print(e)
##        tmpdata = None
##    except timeout as e:
##        print(e)
##        print('time out')
##        tmpdata = None
    except Exception as e:
        print(e)
        tmpdata = None
        
    if tmpdata:
        tmpdata = tmpdata.split('\n')
        for line in tmpdata:
            if line.startswith("SNB.data.quote"):
                #print(line)
                line = line.split('=')
                #print(line)
                #print(line[1])
                dataline = line[1]
                dataline = dataline.strip()
                endPos = dataline.index('}')
                dataline = dataline[:(endPos+1)]
                #print(endPos)
                #print(dataline)
                try:
                    stockdata = ast.literal_eval(dataline)
                except Exception as e:
                    print(e)
                    stockdata = {}
                #print(stockdata)
                return(stockdata)
    else:
        return({})
            

#x = ScrapeOneTicker(stockpage)
hktickersfile = '/Users/shared/models/all.hk.tickers.csv'
shtickersfile = '/Users/shared/marketbreadth/constituents/shcomp.csv'
sztickersfile = '/Users/shared/marketbreadth/constituents/szcomp.csv'
ustickersfile = '/Users/shared/models/all.US.tickers.csv'



def ScrapeXQ():
    outputfile = '/Users/shared/XQ/'+datetime.date.today().strftime('%Y%m%d')+'.csv'
    if os.path.isfile(outputfile):
        return
    
    XQdata = DataFrame()
    ustickers = []
    with open(ustickersfile, 'r') as csvfile:
        for row in csv.reader(csvfile):
            ustickers += row
##    print(len(ustickers))
    ustickers = sorted(list(set(ustickers)))
    failedtickers = []
    for ticker in ustickers:
        #print(ticker)
        xqTicker = ticker
        tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
        if (len(tmpdata)>0):
            tmptb = DataFrame([tmpdata], index=[ticker])
            if XQdata.empty:
                XQdata = tmptb
            else:
                XQdata = pd.concat([XQdata, tmptb])
        else:
            failedtickers += [ticker]
            print(ticker)
    if (len(failedtickers) > 0):
        for ticker in failedtickers:
            xqTicker = ticker
            tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
            if (len(tmpdata)>0):
                tmptb = DataFrame([tmpdata], index=[ticker])
                if XQdata.empty:
                    XQdata = tmptb
                else:
                    XQdata = pd.concat([XQdata, tmptb])
            else:
##                failedtickers += [ticker]
                print(ticker)
    if not XQdata.empty:
        XQdata.to_csv(outputfile, encoding='utf-8')

##    return


    hktickers = []
    with open(hktickersfile, 'r') as csvfile:
        for row in csv.reader(csvfile):
            hktickers += row
    failedtickers = []
    for ticker in hktickers:
        xqTicker = '0' + ticker[:4]
        tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
        if (len(tmpdata)>0):
            tmptb = DataFrame([tmpdata], index=[ticker])
            if XQdata.empty:
                XQdata = tmptb
            else:
                XQdata = pd.concat([XQdata, tmptb])
        else:
            failedtickers += [ticker]
            print(ticker)
    if (len(failedtickers) > 0):
        for ticker in failedtickers:
            xqTicker = '0' + ticker[:4]
            tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
            if (len(tmpdata)>0):
                tmptb = DataFrame([tmpdata], index=[ticker])
                if XQdata.empty:
                    XQdata = tmptb
                else:
                    XQdata = pd.concat([XQdata, tmptb])
            else:
                print(ticker)

        
    if not XQdata.empty:
        XQdata.to_csv(outputfile, encoding='utf-8')

    shtickers = []
    with open(shtickersfile, 'r') as csvfile:
        for row in csv.reader(csvfile):
            shtickers += row
    failedtickers = []
    for ticker in shtickers:
        xqTicker = 'SH' + ticker
        tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
        if (len(tmpdata) >0):
            tmptb = DataFrame([tmpdata], index=[ticker+'.SS'])
            if XQdata.empty:
                XQdata = tmptb
            else:
                XQdata = pd.concat([XQdata, tmptb])
        else:
            failedtickers += [ticker]
            print(ticker)
    if (len(failedtickers) > 0):
        for ticker in failedtickers:
            xqTicker = 'SH' + ticker
            tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
            if (len(tmpdata) >0):
                tmptb = DataFrame([tmpdata], index=[ticker+'.SS'])
                if XQdata.empty:
                    XQdata = tmptb
                else:
                    XQdata = pd.concat([XQdata, tmptb])
            else:
                print(ticker)
        
    if not XQdata.empty:
        XQdata.to_csv(outputfile, encoding='utf-8')
    

    sztickers = []
    with open(sztickersfile, 'r') as csvfile:
        for row in csv.reader(csvfile):
            sztickers += row
    for ticker in sztickers:
        xqTicker = 'SZ' + ticker
        tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
        if (len(tmpdata) >0):
            tmptb = DataFrame([tmpdata], index=[ticker+'.SZ'])
            if XQdata.empty:
                XQdata = tmptb
            else:
                XQdata = pd.concat([XQdata, tmptb])
        else:
            failedtickers += [ticker]
            print(ticker)
    if (len(failedtickers) > 0):
        for ticker in failedtickers:
            xqTicker = 'SZ' + ticker
            tmpdata=ScrapeOneTicker('http://www.xueqiu.com/S/' + xqTicker)
            if (len(tmpdata) >0):
                tmptb = DataFrame([tmpdata], index=[ticker+'.SZ'])
                if XQdata.empty:
                    XQdata = tmptb
                else:
                    XQdata = pd.concat([XQdata, tmptb])
            else:
               print(ticker)
        
    if not XQdata.empty:
        XQdata.to_csv(outputfile, encoding='utf-8')
        XQdata.to_csv('/Users/shared/XQ/CurrentXQ.csv', encoding='utf-8')
                

    #print(XQdata)
        

ScrapeXQ()
