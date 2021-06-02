#!/usr/local/bin/python3.4

import datetime
import csv
import os
import gzip
from glob import glob
import io
from operator import itemgetter
import re
from shutil import copy2, rmtree


#this script handles delisted tickers.
#The main function is to save the price, sc13 and option data to a delisted tickers folder
# and, for price and options data, delete them from the source directories.
#It then removes the ticker from certain lists that are run routinely to download data
#The option data should be only in the HistOptionData directory -- maybe I should add this as a check
#Might add code to identify tickers in HistOptionData but not OptionData as a way to identify delisted tickers

def CompareData2CSV(data, csvfile):
    if os.path.isfile(csvfile):
        try:
            with open(csvfile, encoding='utf-8') as f:
                tmpData = [tuple(line) for line in csv.reader(f)]
            if (data == tmpData):
                return True
            else:
                return False
        except Exception as e:
            return False
    else:
        return False


def SaveDelistedOptionData(ticker):
    histOptionDir = '/Users/Shared/HistOptionData/' + re.sub("\^", '', ticker)
    if os.path.exists(histOptionDir):
        datafiles = glob(histOptionDir+ '/*.csv.gz')
        optData = []
        for datafile in datafiles:
            try:
                with io.TextIOWrapper(gzip.open(datafile, 'r')) as f:
                    tmpData = [tuple(line) for line in csv.reader(f)]
                optData += tmpData
            except Exception as e:
                print(e)

        if optData:
            optData = list(set(optData))
            optData = sorted(optData, key=itemgetter(10, 1), reverse=False)
            optTgt = '/Users/Shared/DelistedTickers/' + re.sub("\^", '', ticker) + '/' +  re.sub("\^", '', ticker) + '.opt.csv.gz'
            with gzip.open(optTgt, 'wt') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(optData)

        rmtree(histOptionDir, ignore_errors=True)



#Assume the first element in each line is ticker
#If not then we need a slightly different function
def RemoveDelistedTickerfromOneList(ticker, src, tgt, tickeridx=0):
    srcList = []
    try:
        with open(src, 'r') as csvfile:
            srcList = [tuple(line)  for line in csv.reader(csvfile) if not (line[tickeridx]==ticker)]
    except Exception as e:
        print(e)
        srcList = []

    if srcList:
        srcList = set(srcList)
        srcList.discard(ticker)
        srcList = sorted(list(srcList))
        if srcList:
            if not CompareData2CSV(srcList, tgt):
                with open(tgt, 'w', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(srcList)

#For sorted data, the order of the original file most likely won't be preserved. But will leave this out so as not to
# create cross-dependence and avoid the necessity to import logic of various files
def RemoveDelistedTickerfromLists(ticker):
    filelists = ['/Users/Shared/Models/CorpCIKs.csv',
                 '/Users/Shared/Models/skipCIKs.csv',
                 '/Users/Shared/Models/fullCIKs.csv',
                 '/Users/Shared/Models/ExtraUSTickers.csv',
                 '/Users/Shared/Models/ExtraUSTickers2.csv',
                 '/Users/Shared/Models/ExtraUSTickers3.csv',
                 '/Users/Shared/Models/R1000.csv',
                 '/Users/Shared/Models/R2000.csv',
                 '/Users/Shared/Models/Liquid Option Stocks.csv',
                 '/Users/Shared/SC13Monitor/Lists/chinese.adr.csv',
                 '/Users/Shared/SC13Monitor/Lists/HKOptionTickers.csv',
                 '/Users/Shared/OptionData/@OptionCount.csv'
                 ]
    for curFile in filelists:
        RemoveDelistedTickerfromOneList(ticker, curFile, curFile)

#RemoveDelistedTickerfromLists('PBY')


def SaveDelistedData (ticker):
# 1. If the directory already exists, raise a warning, something is wrong else create the target directory
# 2. Copy daily price data
# 3. Copy SC13 data if exists
# 4. Copy options data if possible: combine all option data into one zip file
# 5. Remove ticker from CorpCIKs, ExtraUSTickers, ChineseADR, HKOptionable files
    targetDir = '/Users/Shared/DelistedTickers/' + re.sub("\^", '', ticker)
    if os.path.exists(targetDir):
        print(ticker + ' already exists in the DelistedTickers directory!')
        return(None)
    else:
        os.makedirs(targetDir)

    pricefile = '/Users/Shared/Price Data/Daily/' + re.sub("\^", '', ticker) + '.csv'
    pricetgt =  '/Users/Shared/DelistedTickers/' + re.sub("\^", '', ticker) + '/' +  re.sub("\^", '', ticker) + '.csv'
    try:
        copy2(pricefile, pricetgt)
        os.remove(pricefile)
    except Exception as e:
        print(e)

    sc13file = '/Users/Shared/SC13Forms/' + re.sub("\^", '', ticker) + '.csv'
    sc13tgt = '/Users/Shared/DelistedTickers/' + re.sub("\^", '', ticker) + '/' +  re.sub("\^", '', ticker) + '.sc13.csv'
    try:
        copy2(sc13file, sc13tgt)
        os.remove(sc13file)
    except Exception as e:
        print(e)

    SaveDelistedOptionData(ticker)

    RemoveDelistedTickerfromLists(ticker)

# Find tickers that are in HistOptionData but not in OptionData
# It is one very effective way of looking for delisted tickers
def DiffHistandOptionData():
    srcDir = '/Users/shared/OptionData/'
    dataDirs = set(os.listdir(srcDir))
    print(len(dataDirs))
    srcDir  = '/Users/shared/HistOptionData/'
    histDirs = set(os.listdir(srcDir))
    staleTickers = sorted(list( histDirs  - dataDirs))
    print(len(staleTickers))
    print(staleTickers)

    #To err on the conservative size,
    # 1. remove any tickers starting with . or _
    # 2. keep only tickers where the last .csv.gz file in HistOptionData is at least 180 days old
    pruneTickers = []

    for curTicker in staleTickers:
        if re.match("\.", curTicker):
            continue;
        if (re.match("_", curTicker)):
            continue;
        newest = max(glob('/Users/Shared/HistOptionData/'+curTicker+'/*.csv.gz'), key=os.path.getmtime)
        if ((datetime.datetime.now() - datetime.datetime.fromtimestamp(os.path.getmtime(newest))).days > 180):
            #print(curTicker)
            #print(newest)
            pruneTickers += [curTicker]

    print(len(pruneTickers))
    return(pruneTickers)

def SaveDelistedByHistOptionDiff():
    delistedTickers = DiffHistandOptionData()
    if delistedTickers:
        for curTicker in delistedTickers:
            SaveDelistedData(curTicker)



def RemoveAllDelistedTickersfromLists():
    srcDir = '/Users/shared/DelistedTickers/'
    dataDirs = set(os.listdir(srcDir))
    for ticker in dataDirs:
        RemoveDelistedTickerfromLists(ticker)

#SaveDelistedData('HMIN')
#SaveDelistedData('DATE')

#RemoveAllDelistedTickersfromLists()

#SaveDelistedData('MR')
#DiffHistandOptionData()

SaveDelistedByHistOptionDiff()
RemoveAllDelistedTickersfromLists()