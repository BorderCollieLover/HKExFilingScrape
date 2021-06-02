#!/usr/local/bin/python3.4


import datetime
import re
from dateutil import parser
import csv
import os
import unicodedata
import sys
import locale
import getpass
import time
import random
from operator import itemgetter
from glob import glob




##This script examines sc13 filings and identify funds where the scraped data is of relatively good quality.
##Specifically, an entry (filing) scraped is considered to be of good quality
## if the CUSIP is available and if the percentage and/or number of shares is available
##If a fund has a) at least 15 "good" entries; AND b) at least 60% of its entries are good
## then the fund will be selected for further analysis; specifically:
##   i) the Sharpe ratio (and other statistics) for each trade 
##   ii) the quality of the buy/sell signals as given by holding changes
##The thresholds of 15/60% good entries are arbitrary and is subject to calibration
##The idea is that meaningful performance statistics can be gleaned for selected funds 
## while keeping a reasonable (but not over-whelming) amount of funds to generate stock activity signals
##  and trying to minimize the amount of manual cleaning required

##However, there is a practical difficulty in evaluating the validity of CUSIPs
##I will consider any non-empty string valid for now
##But that means all files will have to be cleaned using the CUSIPMappings file where
## funky, invalid cusips (as a result of the imperfect scraping) will be cleaned up
##The preparation of the CUSIPMapping file is a bit tedious but manageable


##This script will create a list of funds that will be used by TradeSignalfromSC13.py

coreCIKfile = '/Users/Shared/Models/CIKs.csv'
CIKfile = '/Users/Shared/Models/fullCIKs.csv'
CorpCIKfile = '/Users/Shared/Models/CorpCIKs.csv'
skipCIKfile = '/Users/Shared/Models/skipCIKs.csv'

with open(CIKfile, 'r') as csvfile:
    TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
with open(coreCIKfile, 'r') as csvfile:
    coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
with open(skipCIKfile, 'r') as csvfile:
    SkipFunds = [tuple(line) for line in csv.reader(csvfile)]
with open(CorpCIKfile, 'r') as csvfile:
    CorpCIKs = [tuple(line) for line in csv.reader(csvfile)]


print(len(TargetFunds))
print(len(coreTargetFunds))
print(len(SkipFunds))
TargetFunds = list(set(TargetFunds) - set(SkipFunds))
print(len(TargetFunds))
TargetFunds = list(set(TargetFunds) - set(coreTargetFunds))
print(len(TargetFunds))
TargetFunds = list(set(TargetFunds) - set(CorpCIKs))
print(len(TargetFunds))


GoodLineThreshold = 15
GoodPercentThreshold = 0.8

goodfunds = []
for fund in TargetFunds:
    dataFile = '/Users/shared/SC13Forms/' +fund[0] +'.csv'
    try:
        with open(dataFile, 'r') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        fundData = None
        continue

    if (len(fundData) <GoodLineThreshold):
        continue

    totalLines = len(fundData) + 1
    goodLines = 0

    for line in fundData:
        curCUSIP = line[3]
        curCUSIP = curCUSIP.strip()
        if (curCUSIP == ''):
            continue
        
        sharesNum = line[5]
        sharesPercentage = line[6]
        sharesNum = sharesNum.strip()
        sharesPercentage = sharesPercentage.strip()

        if ((sharesNum == '') or (sharesNum =='-1')) and ((sharesPercentage == '') or (sharesPercentage =='-1')):
            continue

        goodLines += 1

    if (goodLines >= GoodLineThreshold):
        if ((goodLines/totalLines) >= GoodPercentThreshold):
            goodfunds += [fund]
            #print(fund)

goodfunds = list(set(goodfunds))
#print(goodfunds)
print(len(goodfunds))
        

filename = '/Users/Shared/Models/SC13 Candidate CIKs.csv'
goodfunds = sorted(list(set(goodfunds)))
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer=csv.writer(csvfile)
    writer.writerows(goodfunds)

##
##goodfunds = list(set(goodfunds) - set(SkipFunds))
##goodfunds += coreTargetFunds
##goodfunds = sorted(list(set(goodfunds)))
##SkipFunds = sorted(list(set(SkipFunds)))
##
##filename = '/Users/Shared/Models/CIKs.csv'
##with open(filename, 'w', encoding='utf-8') as csvfile:
##    writer=csv.writer(csvfile)
##    writer.writerows(goodfunds)
##
##with open(skipCIKfile, 'w', encoding='utf-8') as csvfile:
##    writer=csv.writer(csvfile)
##    writer.writerows(SkipFunds)
##
##UniqueFunds = list(sorted(set([fund[0] for fund in goodfunds])))
##print(UniqueFunds)
##filename = '/Users/Shared/Models/Funds.csv'
##with open(filename, 'w', encoding='utf-8') as csvfile:
##    writer=csv.writer(csvfile)
##    for fund in UniqueFunds :
##        writer.writerow([fund])


##TargetFunds += SkipFunds
##TargetFunds = sorted(list(set(TargetFunds)))
##with open(CIKfile, 'w', encoding='utf-8') as csvfile:
##    writer=csv.writer(csvfile)
##    writer.writerows(TargetFunds)
##
##
####Lastly, remove all funds (that have 13F files) from skipfunds
##targetdir = '/Users/shared/fund clone/*'
##for subdir in glob(targetdir):
##    print(subdir)
##    if os.path.isdir(subdir):
##        try:
##            os.rmdir(subdir)
##        except Exception as e:
##            print(e)
##
##fund13fs = set()
##for subdir in glob(targetdir):
##     if os.path.isdir(subdir):
##         fundname = os.path.basename(subdir)
##         fund13fs.add(fundname)
##print(fund13fs)
##
##NewSkipFunds = []
##for line in SkipFunds:
##    if line[0] in fund13fs:
##        print(line)
##    else:
##        NewSkipFunds += [line]
##SkipFunds = NewSkipFunds
##with open(skipCIKfile, 'w', encoding='utf-8') as csvfile:
##    writer=csv.writer(csvfile)
##    writer.writerows(SkipFunds)




    

