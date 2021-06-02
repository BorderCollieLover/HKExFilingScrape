#!/usr/local/bin/python3.4


import datetime
import re
from dateutil import parser
import csv
import os
import pandas as pd
import gzip
from pytz import timezone
import pytz
from bdateutil import isbday
import holidays
import time
from bdateutil import relativedelta
import getpass
from glob import glob, iglob
import io
from operator import itemgetter
import multiprocessing


def utf_8_encoder(unicode_csv_data):
    for line in unicode_csv_data:
        yield line.encode('utf-8')


def CreateDirectory(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)


##compare a data table to the csv file it is to be written to
## return true is the file exists and the data contained therein is identical to data
## else return false        
def CompareData2CSVGZ(data, csvfile):
    if os.path.isfile(csvfile):
        try:
            with io.TextIOWrapper(gzip.open(csvfile, 'r')) as f:
                tmpData = [tuple(line) for line in csv.reader(f)]
            if (data == tmpData):
                return True
            else:
                return False
        except Exception as e:
            return False
    else:
        return False

def RemoveDuplicatedLines(data):
    if (len(data) <= 0):
        return(data)
    else:
        newData = list(set(data))
        return(newData)
                


def DumpTable(onetable,filename, cleanup=False):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        if os.path.isfile(filename):
            with io.TextIOWrapper(gzip.open(filename, 'r')) as f:
                tmpData = [tuple(line) for line in csv.reader(f)]
            onetable = tmpData + onetable
        else:
            outputpath = os.path.dirname(os.path.realpath(filename))
            CreateDirectory(outputpath)
            
        if (cleanup):
            onetable = RemoveDuplicatedLines(onetable)
            onetable = sorted(onetable, key=itemgetter(10, 1), reverse=False)

        if not CompareData2CSVGZ(onetable, filename):
            with gzip.open(filename, 'wt') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(onetable)


##If an option data file is from 6 days (or earlier), then move the data to historical option data folder
##In historical folder, data is saved in yearly files
##This helps to save space on the harddrive, also keeps the number of files reasonable, which is helpful when scraping options in a distributed fashion (and hence synchronization btwn computers)
##It is also important to make sure that files are a few days old before saving.
##When this script is running at the same time when scrapeoption is running, and this script removes ALL option files,
## scrapeoption script might think that it hasn't scrape anythiny yet
def ProcessOneFile(inputFile):
    currDt = datetime.date.today()
    #checkDt = datetime.date(currDt.year, currDt.month, 1)
    checkDt = currDt - datetime.timedelta(days=6)  ### Decide how old a datafile need be before it is moved to histoptdata directory

    fileDt = os.path.splitext(os.path.basename(inputFile))[0]
    #print(fileDt)
    if ('csv' in  fileDt):
        fileDt = os.path.splitext(fileDt)[0]
    #print(fileDt)
    fileDtStr = fileDt
    fileDt = datetime.datetime.strptime(fileDt, '%Y%m%d').date()

    #If save once a month, use below else use if True
    if (fileDt < checkDt):
    #if True:
        #print(inputFile)
        ticker = os.path.basename(os.path.dirname(inputFile))
        outputfile = '/Users/shared/HistOptionData/' + ticker + '/' + str(fileDt.year) +'.csv.gz'
        optData = []
        try:
            with io.TextIOWrapper(gzip.open(inputFile, 'r')) as csvfile:
                optData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            print(e)
            try:
                with open(inputFile, 'r') as csvfile:
                    optData = [tuple(line) for line in csv.reader(csvfile)]
            except Exception as e:
                print(e)
        os.remove(inputFile)

        newData =[]
        for line in optData:
            newData += [(line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], fileDtStr,line[10])]

        DumpTable(newData, outputfile)

def SaveOldOptionData():
    srcDir = '/Users/shared/OptionData/'
    dataDirs = os.listdir(srcDir)
    #print(dataDirs)
##    dataDirs = dataDirs[:10]
    for curDir in dataDirs:
        print(curDir)
        datafiles = glob(srcDir+curDir+'/*.csv.gz')
        for datafile in datafiles:
            ProcessOneFile(datafile)
        datafiles = glob(srcDir+curDir+'/*.csv')
        for datafile in datafiles:
            ProcessOneFile(datafile)



def CleanupOneHistFile(datafile):
    with io.TextIOWrapper(gzip.open(datafile, 'r')) as f:
        tmpData = [tuple(line) for line in csv.reader(f)]

    DumpTable(tmpData, datafile, True)
    

def CleanupHistData():
    srcDir = '/Users/shared/HistOptionData/'
    dataDirs = os.listdir(srcDir)
    #print(dataDirs)
##    dataDirs = dataDirs[:10]
    for curDir in dataDirs:
        print(curDir)
        datafiles = glob(srcDir+curDir+'/*.csv.gz')
        for datafile in datafiles:
            CleanupOneHistFile(datafile)
    

##CleanupOneHistFile('/Users/shared/2014.csv.gz')
##CleanupOneHistFile('/Users/shared/2015.csv.gz')

def SaveOldOptionOneTicker(ticker):
    optdir =  '/Users/shared/OptionData/' + ticker
    if not os.path.exists(optdir):
        return

    datafiles = glob(optdir + '/*.csv.gz')
    for datafile in datafiles:
        ProcessOneFile(datafile)
    datafiles = glob(optdir + '/*.csv')
    for datafile in datafiles:
        ProcessOneFile(datafile)

    #all this part does is to identify the latest yearly data file
    # and call DumpTable with "cleanup" set to True, which will remove duplicated lines and sort the results in order
    #As the yearly data file gets bigger, this step takes longer time (sorting and scanning a much bigger file)
    # and a lot of it is unnecessary as the first, say 11 months' worth of data is cleanned up many times by the end of the year
    #What I need is to "clean up" only the latest data to be added to yearly file and then append it to existing data
    histoptdir = '/Users/shared/HistOptionData/' + ticker
    histdatafiles = glob(histoptdir + '/*.csv.gz')
    for histdatafile in histdatafiles:
        filetime = datetime.datetime.fromtimestamp(os.path.getmtime(histdatafile))
        if ((datetime.datetime.now() - filetime) < datetime.timedelta(hours=24)):
            CleanupOneHistFile(histdatafile)
            break

def SaveOldOptionBatch(tickerlist):
    if not tickerlist:
        return

    for ticker in tickerlist:
        try:
            print(ticker)
            SaveOldOptionOneTicker(ticker)
        except Exception as e:
            print(e)


def SaveOldOptionParallel():
    srcDir = '/Users/shared/OptionData/'
    tickers = os.listdir(srcDir)
    #tickers = tickers[1050:1116]


    NumOfThreads=multiprocessing.cpu_count()
    if (NumOfThreads >=2 ):
        NumOfThreads = NumOfThreads - 4
    threads = [None]* NumOfThreads
    out_q = multiprocessing.Queue()



    tickerListLen = len(tickers) // NumOfThreads
    for i in range(len(threads)):
        startIdx = i * tickerListLen
        if (i == (len(threads) - 1)):
            endIdx = len(tickers) - 1
        else:
            endIdx = (i + 1) * tickerListLen - 1
        threads[i] = multiprocessing.Process(target=SaveOldOptionBatch, args=(tickers[startIdx:(endIdx+1)], ))
        threads[i].start()


    for i in range(len(threads)):
        threads[i].join()

def OptionCount(ticker):
    newestfile = None;
    try:
        newestfile = max(iglob(os.path.join('/Users/Shared/OptionData/' + ticker + '/', '*.csv.gz')), key=os.path.getctime)
    except Exception as e:
        print(e)
        return(None)

    with io.TextIOWrapper(gzip.open(newestfile, 'r')) as csvfile:
        optData = [tuple(line) for line in csv.reader(csvfile)]

    #print(newestfile, len(optData))
    return([(ticker, len(optData))])

def OptionCount2(ticker):
    optdir = '/Users/shared/OptionData/' + ticker
    if not os.path.exists(optdir):
        return

    datafiles = glob(optdir + '/*.csv.gz')
    optNums = [0]
    for datafile in datafiles:
        with io.TextIOWrapper(gzip.open(datafile, 'r')) as csvfile:
            optData = [tuple(line) for line in csv.reader(csvfile)]
        optNums += [len(optData)]

    maxOptNum = max(optNums)
    if (maxOptNum > 0 ):
        return([(ticker, maxOptNum)])
    else:
        return(None)


def UpdateOptionCount():
    srcDir = '/Users/shared/OptionData/'
    tickers = os.listdir(srcDir)

    outputData = []
    for ticker in tickers:
        tmp = OptionCount2(ticker)
        if tmp:
            outputData += tmp

    outputData = sorted(outputData, key=itemgetter(1), reverse=True)

    filename = '/Users/Shared/OptionData/@OptionCount.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(outputData)


def CorrectColumbiaDay(dir):
    file1 = '20161007.csv.gz'
    file2 = '20161010.csv.gz'
    file1 = dir + '/' + file1
    file2 = dir + '/' + file2

    if os.path.exists(file1):
        if (os.path.exists(file2)):
            os.remove(file1)
        else:
            os.rename(file1, file2)

def CorrectColumbiaDayAll():
    srcDir = '/Users/shared/OptionData/'
    tickers = os.listdir(srcDir)

    outputData = []
    for ticker in tickers:
        optDir = '/Users/Shared/OptionData/' + ticker
        CorrectColumbiaDay(optDir)


#Go through a historical gzipped option data file
#Check if there is only ONE line entry for each option for each day
#If not, keep only the line entry with earliest scraping time for that option for that day
#Save the "cleaned" file if there are redundencies in the file
def CleanHistRedundant(optfile):
    print(optfile)
    tmpData = []
    if os.path.isfile(optfile):
        with io.TextIOWrapper(gzip.open(optfile, 'r')) as f:
            tmpData = [tuple(line) for line in csv.reader(f)]
    else:
        return

    if tmpData is None:
        return

    if len(tmpData) <=1 :
        return

    #print(tmpData[1])

    tmpData = sorted(tmpData,  key=itemgetter(1, 10,11), reverse=False)
    outputData = []

    redundantLines = False

    for lnIdx, line in enumerate(tmpData):
        if lnIdx == 0:
            outputData += [line]
        else:
            if not ((line[1] == tmpData[lnIdx-1][1]) and (line[10] == tmpData[lnIdx-1][10])):
                outputData += [line]
            else:
                #print(optfile)
                print(line)
                redundantLines = True

    if redundantLines:
        print(len(tmpData))
        print(len(outputData))
        outputData = sorted(outputData, key=itemgetter(10, 1), reverse=False)
        with gzip.open(optfile, 'wt') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(outputData)

    return

def CleanHistRedundantAll():
    srcDir = '/Users/shared/HistOptionData/'
    tickers = os.listdir(srcDir)

    for ticker in tickers:
        optDir = '/Users/Shared/HistOptionData/' + ticker
        datafiles = glob(optDir + '/2016.csv.gz')
        for datafile in datafiles:
            CleanHistRedundant(datafile)

def CleanDelistedRedundantAll():
    srcDir = '/Users/shared/DelistedTickers/'
    tickers = os.listdir(srcDir)

    for ticker in tickers:
        optDir = '/Users/Shared/DelistedTickers/' + ticker
        datafiles = glob(optDir + '/*.opt.csv.gz')
        for datafile in datafiles:
            CleanHistRedundant(datafile)


#CleanHistRedundant('/Users/Shared/HistOptionData/A/2015.csv.gz')
#CleanHistRedundantAll()
#CleanDelistedRedundantAll()
#CorrectColumbiaDay('/Users/Shared/OptionData/AA')

#CorrectColumbiaDayAll()

#Note: run UpdateOptionCount() before saving old option data
UpdateOptionCount()
SaveOldOptionParallel()
CleanHistRedundantAll()


#SaveOldOptionData()
#CleanupHistData()


    
    

