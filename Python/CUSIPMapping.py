#!/usr/local/bin/python3.4

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

##This script is really a collection of functions that are used to massage
## scraped SC13 filings. Many functions are subsequently added to the main 
## scraping script
##The main useful function here is the GetallFunkyCUSIPs function
##It scans all scraped SC13 filings and collect all funky CUSIPs --
## containing non alpha-digits or longer than 9
##The output is examined manually to create the CUSIPMapping file
##In the main scraping script, a function is called to clean all funky CUSIPs based
## on the CUSIPMapping file


locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )

def DumpTable(onetable,filename):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        onetable = sorted(onetable, key=itemgetter(2), reverse=True)
        cleanedData = []
        for line in onetable:
            curCUSIP = line[3]
            curCUSIP = curCUSIP.strip()
            if (len(curCUSIP) == 9):
                cleanedData += [line]
                continue
            elif re.match('[0-9a-zA-Z ]+\Z', curCUSIP):
                ##print(curCUSIP);
                p=re.compile(' ')
                ##print(p.sub('',curCUSIP))
                newCUSIP = p.sub('',curCUSIP)
                if (newCUSIP != curCUSIP):
                    ##print(newCUSIP)
                    newline = (line[0], line[1], line[2], newCUSIP, line[4], line[5],line[6], line[7])
                    cleanedData += [newline]
                else:
                    cleanedData += [line]
            else:
                cleanedData += [line]
        onetable = cleanedData

        
        with open(filename, 'w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(onetable)


def SaveSet2CSV(filename, datSet):
    ##print(datSet)
    datTmp = list(datSet)
    ##print(datTmp)
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        for line in datTmp :
            writer.writerow([line])



FundList = '/Users/Shared/Models/fullCIKs.csv'
with open(FundList, 'r') as csvfile:
    TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
FundList = '/Users/Shared/Models/skipCIKs.csv'
with open(FundList, 'r') as csvfile:
    SkipFunds = [tuple(line) for line in csv.reader(csvfile)]
TargetFunds = list(set(TargetFunds) - set(SkipFunds))


CUSIPMappingFile = '/Users/Shared/Models/CUSIPMapping.csv'
with open(CUSIPMappingFile, 'r', encoding='utf-8') as csvfile:
    CUSIPMappings = [tuple(line) for line in csv.reader(csvfile)]
CUSIPMappings = sorted(list(set(CUSIPMappings)))
with open(CUSIPMappingFile, 'w', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(CUSIPMappings)



##The basic consideration when cleaning up the scraped SC13 filings is to be as low-touch and conservative as possible
## as there are inevitably a lot of manual checks -- especially for new entries that can be used as trade signals

##Scan all SC13 filing data, collect all CUSIP fields
##Discard those that are only alpha-digits
##Dump everything else in a file -- add to existing file if there is one
##That file can then be manually checked and divide into two files:
## 1) a mapping file between funky cusips to real ones, e.g. those can be quickly fixed such as replacing an appending *
## 2) a to-be-removed list, e.g. things that are obviously wrong such as (e) or (item) which can be used to clean the filing data
## in fact the to-be-removed will simply be mapped to empty strings

def GetallFunkyCUSIPs():
    funkyCUSIPfile = '/Users/shared/models/funkycusips.csv'

    try:
        with open(funkyCUSIPfile, mode='r', encoding='utf-8') as infile:
            funkyCUSIPs = set([tuple(line) for line in csv.reader(infile)])
    except Exception as e:
        print(e)
        funkyCUSIPs = set()

    
    FinalFunds = sorted(list(set([line[0] for line in TargetFunds])))
        
    for fund in FinalFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund+'.csv'
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
            fundCUSIPs = [line[3] for line in fundData]
        except Exception as e:
            print(e)
            continue

        for curCUSIP in fundCUSIPs:
            if re.match('[0-9a-zA-Z ]+\Z', curCUSIP):
                continue
            else:
                funkyCUSIPs.update([curCUSIP])

    SaveSet2CSV(funkyCUSIPfile, funkyCUSIPs)


def GetIssuerNamesfromSC13():
    allIssuers = set()
    for fund in TargetFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund[0]+'.csv'
        
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
            IssuerNames = set([line[1] for line in fundData])
        except Exception as e:
            print(e)
            continue

        allIssuers.update(IssuerNames)

##        for curIssuer in IssuerNames:
##            match = re.match('^\W', curIssuer)
##            if not match:
##                allIssuers.update(curIssuer)
    print(len(allIssuers))
    SaveSet2CSV('/Users/Shared/models/sc13issuers.csv',allIssuers)


##Added on or around May 12th, 2015
##GetSharesfromSC13 and CleanShareNumbers are used to clean up scraped SC13 files
##Specifically, there are certain combinations of sharesNum and sharesPercentage
##that are clear signs that something is wrong
## a) when the sharesNum is less than 100 but greater than 0
## b) when a), and the sharesPercentage is certain integers
## c) when the sharesPercentage is greater than 100
## d) when the sharesNum is greater than zero but the sharesPercentage is zero
## e) when the sharesPercentage is greater than zero but the sharesNum is zero
## GetSharesfromSC13 scans SC13 files and enumerate/verify patterns in a) and b)
## CleanShareNumbers fixed a), b) and c). d) and e) are likely a result of early
## versions of code when 0 is used when scraping failed. This is no longer an issue
## as I switch to -1 for error flags
##The logic dealing with a)~c) is added to SrapeSC13 code so there is no need to run these
## two functions after correcting old files in one batch
def GetSharesfromSC13():
    outputlines = set()
    for fund in TargetFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund[0]+'.csv'
        
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            ##print(e)
            continue

        for line in fundData:
            sharesNum = line[5]
            sharesPercentage = line[6]
            sharesNum = sharesNum.strip()
            if (len(sharesNum) > 0) :
                sharesNum = locale.atoi(sharesNum)
                if (sharesNum < 200) and (sharesNum >0):
                    #print(str(sharesNum) + ' ' + sharesPercentage)
                    tmpstr = str(sharesNum) + ' , ' + sharesPercentage
                    print(tmpstr)
                    outputlines.add(tmpstr)
                    
    print(len(outputlines))
    print(outputlines)
    SaveSet2CSV('/Users/shared/tmp.csv', outputlines)

def CleanShareNumbers():
    for fund in TargetFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund[0]+'.csv'
        
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            ##print(e)
            continue

        newData = []
        fundModified = False
        
        for line in fundData:
            sharesNum = line[5]
            sharesPercentage = line[6]
            newsharesNum = sharesNum
            newsharesPCT = sharesPercentage
            if (len(sharesNum) > 0) :
                sharesNum = locale.atoi(sharesNum)
                
                if (sharesNum <=99) and (sharesNum >0):
                    sharesNum = -1
                    newsharesNum = -1
                    fundModified = True

                    if (len(sharesPercentage) > 0):
                        sharesPercentage = locale.atof(sharesPercentage)
                        if (sharesPercentage == 0) or (sharesPercentage ==1 ) or (sharesPercentage == 3) or (sharesPercentage == 4) or (sharesPercentage == 12) or (sharesPercentage == 13) or (sharesPercentage == 14):
                            sharesPercentage = -1
                            newsharesPCT = -1
                else:
                    if (len(sharesPercentage) > 0 ):
                        sharesPercentage = locale.atof(sharesPercentage)
                        if (sharesPercentage > 100):
                            sharesPercentage = -1
                            newsharesPCT = -1 
                            fundModified = True
            else:
                if (len(sharesPercentage) > 0 ):
                    sharesPercentage = locale.atof(sharesPercentage)
                    if (sharesPercentage > 100):
                        sharesPercentage = -1
                        newsharesPCT = -1 
                        fundModified = True

            newData += [(line[0], line[1], line[2], line[3], line[4], newsharesNum, newsharesPCT, line[7])]

        if fundModified:
            DumpTable(newData, '/Users/Shared/SC13Forms/@temp/'+fund[0]+'.csv')

                
        

##GetIssuerNamesfromSC13()
GetallFunkyCUSIPs()
##GetSharesfromSC13()
##CleanShareNumbers()
