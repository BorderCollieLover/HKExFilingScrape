#!/usr/local/bin/python3.4

import datetime
import re
from dateutil import parser
import csv
import os
import pandas as pd
from pandas.tseries.offsets import BDay
import gzip
from pytz import timezone
import pytz
import unicodedata
import sys
import locale
import getpass
import time
from glob import glob
from operator import itemgetter
import shutil


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


outputpath = '/Users/Shared/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
CIKfile = '/Users/Shared/Models/CIKs.csv'
SkipCIKfile = '/Users/Shared/Models/skipCIKs.csv'

fileCUSIPTicker = '/Users/Shared/Models/CUSIPTicker.csv'
try:
    with open(fileCUSIPTicker, mode='rt') as csvfile:
        dictCUSIPTicker = dict(csv.reader(csvfile))
except Exception as e:
    print(e)
    dictCUSIPTicker = dict()


##keyidx is used to sort data before writing to files
def DumpTable(onetable,filename, keyidx, reverse=False):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        onetable = sorted(onetable, key=itemgetter(keyidx), reverse=reverse)
        if not CompareData2CSV(onetable, filename):
            if os.path.isfile(filename):
                try:
                    os.remove(filename)
                except Exception as e:
                    print(e)
            with open(filename, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(onetable)


##CUSIPScreen scans SC13 data files and writes CUSIPs that are not
## in the CUSIP -- Ticker dictionary to a file for later examination
##This is used as pointers for manual check/fix of SC13 data file
def CUSIPScreen(fundname):
    dataFile = outputpath + 'SC13Forms/' +fundname +'.csv'
    try:
        with open(dataFile, 'r') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print (e)
        fundData = None
        return(None)

    CUSIPs = [line[3] for line in fundData]
    CUSIPs = list(set(CUSIPs))
    CUSIPScreenOutput = open(outputpath + 'SC13Forms/@Unmatched CUSIPs.txt', 'a')
    for CUSIP in CUSIPs:
        if CUSIP in dictCUSIPTicker:
            continue
        else:
            CUSIPScreenOutput.write(fundname + ' : ' + CUSIP+'\n')
    CUSIPScreenOutput.close()

    
##The SC13 data (organized by fund) is then broken down by CUSIPs/tickers.
##Only positions whose CUSIP has a corresponding ticker is kept
##The first step, TradesfromSC13, stores raw positions file in SC13Trades
##The second step, SignalsfromSC13,
##   2.a first scans the raw positions file and keeps only underlyings where there is no missing data
##        which is stored in SC13TradeData
##   2.b the 'cleaned' trade data is then used to generate buy/sell signal files in SC13Signals


def TradesfromSC13(fundname):
    dataFile = outputpath + 'SC13Forms/' +fundname +'.csv'
    try:
        with open(dataFile, 'r') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        ##print (e)
        fundData = None
        return(None)

    outputdir = outputpath + 'SC13Backtest/Trades/' +fundname 
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    CUSIPs = [line[3] for line in fundData]
    CUSIPs = list(set(CUSIPs))
    for CUSIP in CUSIPs: 
        if CUSIP in dictCUSIPTicker:
            Ticker = dictCUSIPTicker[CUSIP]
            outputfile = outputdir + '/' + CUSIP + '.csv'
            FilingRows = [ tuple(line) for line in fundData if line[3]==CUSIP ]
            
            ##Collect the filing data structure
            ## 1. Number of shares
            ## 2. Percentage
            ## 3. Filing Accepted Date
            ## 4. Trade Data
            ## 5. Link 
            FilingData = []
            for line in FilingRows:
                #sharesNum = locale.atoi(line[5])
                #sharesPercent = locale.atof(line[6])
                sharesNum = line[5]
                sharesPercent = line[6]
                filingTime = line[2]
                try:
                    tradeDate= parser.parse(line[4])
                except Exception as e:
                    #print(e)
                    #print(line)
                    tradeDate=None
                filingLink = line[7]
                FilingData += [(sharesNum, sharesPercent, filingTime, tradeDate, filingLink)]

            DumpTable(FilingData, outputfile, 2)


def Trades2Signals(tradeData):
    tradeSignal = []
    ## screen and remove duplicated entries (filed on the same date) in tradeData
    ## tradeSignal: +1/0/-1, signal date, signal reason, stop date(if applicable)
    ##   trade signal: + -- buy; 0 -- do nothing (when reason is unchanged); - -- sell/short
    ##   signal reason: new, increase, decrease, unchanged  and close
    ##   negative signal date is set to 1000 days past 
    for lineIdx, line in enumerate(tradeData):
        if lineIdx == 0:
            if line[1] >= 4.5:
                tradeSignal += [(1, line[2], 'New', line[2]+datetime.timedelta(days=10000))]
            else:
                tradeSignal += [(0, line[2], 'Unknown', line[2]+datetime.timedelta(days=10000))]
                #print(line)

        else:
            ## 2nd or more SC13 filing of the same underlying, compare to previous filings
            ## If either shares number or sharesPercentage remains unchanged, consider position unchanged
            ## Else if both share numbers exists, use share numbers to determine position changes
            ## Else use sharesPercentage to determine position changes
            preLine = tradeData[lineIdx - 1]
            if (((line[0] == preLine[0]) and (line[0] > -1)) or ((line[1] == preLine[1]) and (line[1] > -1)))  :
                tradeSignal += [(0, line[2], 'Unch', line[2]+datetime.timedelta(days=10000))]                
            elif ((line[0] > -1) and (preLine[0]>-1)):
                if (line[0] > preLine[0]):
                    tradeSignal += [(1, line[2], 'Inc', line[2]+datetime.timedelta(days=10000))]
                elif (line[0] < preLine[0]):
                    tradeSignal += [(-1, line[2], 'Dec', line[2]+datetime.timedelta(days=10000))]
                else:
                    tradeSignal += [(0, line[2], 'Unch', line[2]+datetime.timedelta(days=10000))]
            elif((line[1] > -1) and (preLine[1]>-1)):
                if (line[1] > preLine[1]):
                    tradeSignal += [(1, line[2], 'Inc', line[2]+datetime.timedelta(days=10000))]
                elif (line[1] < preLine[1]):
                    tradeSignal += [(-1, line[2], 'Dec', line[2]+datetime.timedelta(days=10000))]
                else:
                    tradeSignal += [(0, line[2], 'Unch', line[2]+datetime.timedelta(days=10000))]
            else:
                print('Share number and percentage missing alternatively, please fix!!!!!!!!!!')
                print(line)
                

    ##Update stop date:
    if (len(tradeSignal) > 1):
        for lineIdx, line in enumerate(tradeSignal):
            #print(len(tradeSignal))
            if (lineIdx < len(tradeSignal)-1):
                for j in range(lineIdx+1, len(tradeSignal)):
                    laterLine = tradeSignal[j]
                    if (laterLine[0]*line[0] == -1):
                        tradeSignal[lineIdx] = (line[0], line[1], line[2], laterLine[1])
                        break;

    if len(tradeSignal) >= 1:
        lastTrade = tradeData[-1]
        lastSignal = tradeSignal[-1]
        if ((lastTrade[1] < 5) and (lastTrade[1] > -1)):
            tradeSignal.pop()
            tradeSignal += [(-1, lastSignal[1], 'Close', lastSignal[3])]

##    if (tradeSignal[0][0] == 0 ):
##        print(tradeData[0])
    return(tradeSignal)


## Load a positions file and then generate trade signals using Trades2Signals
def TradeFile2Signals(tradeFile):
    try:
        with open(tradeFile, 'r') as infile:
            tmpData = [tuple(line) for line in csv.reader(infile)]
    except Exception as e:
        print(e)
        tmpData = []

    print(tmpData)
    if (tmpData):
        tradeData=[]
        for line in tmpData:
            try:
                sharesNum = locale.atoi(line[0])
            except Exception as e:
                sharesNum = -1
            
            try:
                sharesPercent = locale.atof(line[1])
            except Exception as e:
                sharesPercent = -1
                                        
            tradeData +=[(sharesNum, sharesPercent, parser.parse(line[2]))]
            
        tradeSignal=Trades2Signals(tradeData)
    print(tradeSignal)
    

def SignalsfromSC13(fundname):
    inputdir = outputpath + 'SC13Backtest/Trades/' +fundname
    if not os.path.exists(inputdir):
        return(None)

    outputdir = outputpath + 'SC13Backtest/Signals/' +fundname
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)

    tradeDatadir = outputpath + 'SC13Backtest/TradeData/' +fundname
    if not os.path.exists(tradeDatadir):
        os.makedirs(tradeDatadir)


    tradesFiles = [f for f in os.listdir(inputdir) if f.endswith('.csv')]
    for oneTicker in tradesFiles:

        inputfile = inputdir + '/'+oneTicker
        outputfile = outputdir + '/'+oneTicker

        if (os.path.exists(outputfile)):
            inputTime = datetime.datetime.fromtimestamp( os.path.getmtime(inputfile))
            outputTime = datetime.datetime.fromtimestamp( os.path.getmtime(outputfile))
            if (outputTime > inputTime):
                continue
            
        tradeDatafile = tradeDatadir +'/' + oneTicker
        
        with open(inputfile, 'r') as infile:
            inputData = [tuple(line) for line in csv.reader(infile)]

        inputData = sorted(inputData, key=itemgetter(2))

        tradeData = []
        MissingPercent = False
        MissingNumber = False
        for inputRow in inputData:
            try:
                sharesNum = locale.atoi(inputRow[0])
            except Exception as e:
                sharesNum = -1 
            try:
                sharesPercent = locale.atof(inputRow[1])
            except Exception as e:
                sharesPercent = -1               
            ##if (sharesNum == -1) or (sharesPercent == -1):
            if (sharesPercent == -1):
                MissingPercent = True
                if (len(inputData)>1):
                    allPercents = set([line[1] for line in inputData])
##                    if (len(allPercents) > 1):
##                        print(inputRow)
                #break;
            if (sharesNum == -1):
                MissingNumber = True
                
            ## check if there are multiple filings on the same date, if so, keep only the latest
            if tradeData:
                filingDt = parser.parse(inputRow[2]).date()
                previousDt = tradeData[-1][2].date()
                if (filingDt == previousDt):
                    tradeData.pop()
            tradeData += [(sharesNum, sharesPercent, parser.parse(inputRow[2]))]
            
        if (MissingPercent and MissingNumber):
            continue;
        
        DumpTable(tradeData, tradeDatafile, 2)
        tradeSignal=Trades2Signals(tradeData)
        DumpTable(tradeSignal, outputfile, 1)


##os.remove(outputpath + 'SC13Forms/@Unmatched CUSIPs.txt')
##for fund in TargetFunds:
##    CUSIPScreen(fund[0])

def GenerateTradeSignals4Fund(fundname):
    tradeDir = '/Users/shared/SC13Backtest/Trades/'+fundname+'/'
    sc13File = '/Users/shared/SC13Forms/' + fundname + '.csv'
    if (os.path.exists(tradeDir)):
        dirTime = datetime.datetime.fromtimestamp( os.path.getmtime(tradeDir))
        if (os.path.exists(sc13File)):
            dataTime = datetime.datetime.fromtimestamp( os.path.getmtime(sc13File))
            #print(dirTime)
            #print(dataTime)
            if (dirTime > dataTime):
                return None
            else:
                ##generate signals
                TradesfromSC13(fundname)
                SignalsfromSC13(fundname)
                return None
        else:
            #something is wrong
            print(sc13File + ' is missing')
            return None
    else:
        if (os.path.exists(sc13File)):
            TradesfromSC13(fundname)
            SignalsfromSC13(fundname)
            return None
    
##    try:
##        shutil.rmtree('/Users/shared/SC13Backtest/Trades/'+fundname+'/')
##    except Exception as e:
##        print(e)
##    print(fundname)    
##    TradesfromSC13(fundname)
##
##    try:
##        shutil.rmtree('/Users/shared/SC13Backtest/TradeData/'+fundname+'/')
##        shutil.rmtree('/Users/shared/SC13Backtest/Signals/'+fundname+'/')
##    except Exception as e:
##        print(e)
##    SignalsfromSC13(fundname)


def GenerateAllTradeSignals():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    with open(SkipCIKfile, 'r') as csvfile:
        SkipFunds = [tuple(line) for line in csv.reader(csvfile)]

    TargetFunds = list(set(TargetFunds) - set(SkipFunds))
    FinalFunds = list(set([line[0] for line in TargetFunds]))


    for fund in FinalFunds:
        GenerateTradeSignals4Fund(fund)

   


##scan for CUSIPs where the holdings data is good but have no corresponding tickers
##these are likely underlyings that have been delisted
##add some of such tickers back can correct some survivalship bias
## 
def StaleCUSIPs():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    with open(SkipCIKfile, 'r') as csvfile:
        SkipFunds = [tuple(line) for line in csv.reader(csvfile)]

    TargetFunds = list(set(TargetFunds) - set(SkipFunds))
    FinalFunds = list(set([line[0] for line in TargetFunds]))

    outputdata = []
    stalecusips = set()
    for fund in FinalFunds:
        dataFile = outputpath + 'SC13Forms/' +fund +'.csv'
        try:
            with open(dataFile, 'r') as csvfile:
                fundData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            fundData = None
            continue

        CUSIPs = [line[3] for line in fundData]
        CUSIPs = list(set(CUSIPs))
        for CUSIP in CUSIPs: 
            if CUSIP in dictCUSIPTicker:
                continue
            if CUSIP in stalecusips:
                continue
            if CUSIP =='':
                continue

            FilingRows = [ tuple(line) for line in fundData if line[3]==CUSIP ]
            if (len(FilingRows) <=1):
                continue

            allPercents = set([line[6] for line in FilingRows])
            allNumbers = set([line[5] for line in FilingRows])
            allPercents.discard('')
            allNumbers.discard('')
            if (len(allPercents)<=1 or len(allNumbers)<=1):
                continue
            
            if (("-1" not in allPercents) and ("-1" not in allNumbers)):
                ##print(FilingRows[0])
                outputdata += [FilingRows[0]]
                stalecusips.update(CUSIP)

    DumpTable(outputdata, '/Users/shared/SC13Backtest/@stale cusips.csv',2, reverse=True)


##Generate a list of filings for manual fix
##For CUSIPs with an underlying ticker:
##A row is considered a "bad" row if both percentage and share numbers are missing;
##A row is considered a good row otherwise
##If the number of good rows is greater than the number of bad rows, AND if the number of bad rows is
## no greater than the threshold, 
##The dump the bad rows to a file for checking
def Filings2Fix(threshold=2):
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    with open(SkipCIKfile, 'r') as csvfile:
        SkipFunds = [tuple(line) for line in csv.reader(csvfile)]

    TargetFunds = list(set(TargetFunds) - set(SkipFunds))
    FinalFunds = list(set([line[0] for line in TargetFunds]))
    #print(FinalFunds)

    outputdata = []
    for fund in FinalFunds:
        dataFile = outputpath + 'SC13Forms/' +fund+'.csv'
        try:
            with open(dataFile, 'r') as csvfile:
                fundData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            fundData = None
            continue

        
        CUSIPs = [line[3] for line in fundData]
        CUSIPs = list(set(CUSIPs))
        for CUSIP in CUSIPs: 
            if not (CUSIP in dictCUSIPTicker):
                continue

            FilingRows = [ tuple(line) for line in fundData if line[3]==CUSIP ]
            goodrowctr = 0 ;
            badrowctr = 0 ;
            badrowidx = set()
            for idx, line in enumerate(FilingRows):
                if (((line[5] == '') or (line[5] == '-1')) and ((line[6]=='') or (line[6]=='-1'))):
                    badrowctr += 1
                    badrowidx.update([idx])
                else:
                    goodrowctr += 1

            if ((badrowctr <threshold) and (goodrowctr > badrowctr) and (badrowctr>0)):
                for idx in list(badrowidx):
                    outputdata += [FilingRows[idx]]

    
    DumpTable(outputdata, '/Users/shared/SC13Backtest/@manual fix lines.csv',2, reverse=True)
                    
             
GenerateAllTradeSignals()
StaleCUSIPs()
Filings2Fix(2)   
                        
            

            
        
        
    
