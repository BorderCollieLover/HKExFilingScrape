#!/usr/local/bin/python3.4

##Sample: 
##https://adesquared.wordpress.com/2013/06/16/using-python-beautifulsoup-to-scrape-a-wikipedia-table/

from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import re
import html
from dateutil import parser
import csv
import os
import pandas as pd
from pandas.tseries.offsets import BDay
import gzip
from joblib import Parallel, delayed  
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


CIKfile = '/Users/Shared/Models/fullCIKs.csv'
CorpCIKfile = '/Users/Shared/Models/CorpCIKs.csv'
skipCIKfile = '/Users/Shared/Models/skipCIKs.csv'
coreCIKfile = '/Users/Shared/Models/CIKs.csv'


with open(CorpCIKfile, 'r') as csvfile:
    CorpFunds = [tuple(line) for line in csv.reader(csvfile)]
with open(CIKfile, 'r') as csvfile:
    TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
with open(skipCIKfile, 'r') as csvfile:
    SkipCIKs = [tuple(line) for line in csv.reader(csvfile)]
with open(coreCIKfile, 'r') as csvfile:
    coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]

TargetFunds = list(set(TargetFunds) - set(coreTargetFunds))
TargetFunds = list(set(TargetFunds) - set(SkipCIKs))
TargetFunds = list(set(TargetFunds) - set(CorpFunds))

##Find corporates to add to skip list
## if the sc13 forms contain only one CUSIP or only one holding's name
## Initially thought about removing all funds whose's last filing has been at least
## 5 years old
## But these funds may still exist, e.g. file 13F forms. So ignore those. 
corporatelist = list()
corporatelist2 = list()
corporatelist3 = list()
for fund in TargetFunds:

    if (re.search('capital', fund[0], re.I)):
        continue;
    
    if (re.search('management', fund[0], re.I)):
        continue;

    if (re.match('investment', fund[0], re.I)):
        continue;

    fundfile = '/Users/shared/SC13Forms/' +fund[0] +'.csv'

    try:
        with open(fundfile, mode='r', encoding='utf-8') as infile:
            fundData = [tuple(line) for line in csv.reader(infile)]
        fundCUSIPs = [line[3] for line in fundData]
        fundHoldings = [line[1] for line in fundData]
    except Exception as e:
        continue

##    latestfilingdt = parser.parse(fundData[0][2])
##    if ((datetime.datetime.now()-latestfilingdt) > datetime.timedelta(days=5*365)):
##        corporatelist3 +=[fund]
##        continue


    fundCUSIPs = set(fundCUSIPs)
    fundCUSIPs.discard('')
    fundCUSIPs.discard(' ')
    if (len(fundCUSIPs) == 1):
        #print(fund)
        corporatelist += [fund]
        continue

    fundHoldings = set(fundHoldings)
    fundHoldings.discard('')
    fundHoldings.discard(' ')
    newholdingset = set()
    for coname in fundHoldings:
        newholdingset.add(coname.lower())
        
    if (len(newholdingset) == 1):
        corporatelist2 += [fund]
        continue

    
    


##for fund in coreTargetFunds:
##    fundfile = '/Users/shared/SC13Forms/' +fund[0] +'.csv'
##    try:
##        with open(fundfile, mode='r', encoding='utf-8') as infile:
##            fundData = [tuple(line) for line in csv.reader(infile)]
##        fundCUSIPs = [line[3] for line in fundData]
##        fundHoldings = [line[1] for line in fundData]
##    except Exception as e:
##        continue
##    
##    latestfilingdt = parser.parse(fundData[0][2])
##    if ((datetime.datetime.now()-latestfilingdt) > datetime.timedelta(days=5*365)):
##        #print(fund)
##        corporatelist3 +=[fund]
##        continue

    

    


outputfile = '/Users/shared/models/tmpskiplist.csv'
with open(outputfile, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(corporatelist)


outputfile = '/Users/shared/models/tmpskiplist2.csv'
with open(outputfile, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(corporatelist2)

##outputfile = '/Users/shared/models/tmpskiplist3.csv'
##with open(outputfile, 'w', encoding='utf-8') as csvfile:
##    writer=csv.writer(csvfile)
##    writer.writerows(corporatelist3)
##
    
    
    

