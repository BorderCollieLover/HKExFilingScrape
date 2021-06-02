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
import time
from glob import glob

##This script scans through the 13F filings for each fund
##For each fund, it creates/updates a fund-specific issuer name and CUSIP list if the CUSIP has a corresponding stock ticker
##In this process this script also creates/updates a universal CUSIP to Ticker mapping file
##Each new CUSIP (if not yet in the CUSIP to Ticker mapping file) is submitted to the Fidelity website to see if a valid ticker is returned


## Query for stock ticker using CUSIP
## Returns:  a) '' : when there was a problem connecting to the website
##           b) None: when the CUSIP doesn't have a stock ticker
##           c) the stock ticker if the underlying is a listed stock 
def GetTickerfromCUSIP(CUSIP):
    CUSIP = CUSIP.strip()
    tickerQueryURL = 'http://activequote.fidelity.com/mmnet/SymLookup.phtml?reqforlookup=REQUESTFORLOOKUP&productid=mmnet&isLoggedIn=mmnet&rows=50&for=stock&by=cusip&criteria='+CUSIP+'&submit=Search'
    print(tickerQueryURL)
    try:
        tickerPage = urlopen(tickerQueryURL)
        soup = BeautifulSoup(tickerPage.read().decode('utf-8', 'ignore'))
    except Exception as e:
        print(e)
        return('')

    if soup is None:
        return('')

    if soup(text=re.compile('The security you entered is not recognized')):
        return(None)

    for a in soup.findAll('a', href=True):
        m = re.match('\/webxpress\/get_quote\?QUOTE_TYPE\=', a['href'])
        if m:
            ticker = a.get_text()
            p = re.compile('\/')
            ticker = p.sub('-', ticker)
            return(ticker)

    return(None)


def SaveDict2CSV(filename, datDict):
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for key, value in datDict.items():
            writer.writerow([key, value])

def SaveSet2CSV(filename, datSet):
    ##print(datSet)
    datTmp = list(datSet)
    ##print(datTmp)
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        for line in datTmp :
            writer.writerow([line])

    

CIKfile = '/Users/Shared/Models/fullCIKs.csv'
with open(CIKfile, 'r') as csvfile:
    TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
coreCIKfile = '/Users/Shared/Models/CIKs.csv'
with open(coreCIKfile, 'r') as csvfile:
    coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
TargetFunds = coreTargetFunds + TargetFunds
TargetFunds = list(set(TargetFunds))

fileCUSIPTicker = '/Users/Shared/Models/CUSIPTicker.csv'
try:
    with open(fileCUSIPTicker, mode='rt') as csvfile:
        tmpdata = [tuple(line) for line in csv.reader(csvfile)]
        tmpdata = sorted(list(set(tmpdata)))
    with open(fileCUSIPTicker, mode='w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(tmpdata)
except Exception as e:
    print(e)
try:
    with open(fileCUSIPTicker, mode='rt') as csvfile:
        dictCUSIPTicker = dict(csv.reader(csvfile))
except Exception as e:
    print(e)
    dictCUSIPTicker = dict()

fileCUSIPIgnore = '/Users/Shared/Models/CUSIPIgnore.csv'
try:
    with open(fileCUSIPIgnore, mode='rt') as csvfile:
        CUSIPIgnore = set([row[0] for row in csv.reader(csvfile)])
    ##remove any CUSIPs from the Ignore set if it is found in CUSIPTicker
    tmpCUSIPs = dictCUSIPTicker.keys()
    CUSIPIgnore = CUSIPIgnore.difference(set(tmpCUSIPs))
except Exception as e:
    print(e)
    CUSIPIgnore = set()


dictCUSIPTickerUpdated = False
CUSIPIgnoreUpdated = False
##TargetFunds = TargetFunds[55:70]
def Do13FCUSIPs():
    dictCUSIPTickerUpdated = False
    CUSIPIgnoreUpdated = False

    for fund in TargetFunds:
        print(fund)
        dictCUSIPFile = '/Users/Shared/CUSIP Lookup/'+fund[0]+'.csv'
        dictCUSIP = {}
        try:
            with open(dictCUSIPFile, mode='r') as infile:
                reader = csv.reader(infile)
                dictCUSIP = dict(reader)
        except Exception as e:
            print(e)

        if (os.path.isfile(dictCUSIPFile)):
            dictCUSIPFileDt =  datetime.datetime.fromtimestamp(os.stat(dictCUSIPFile).st_mtime)
        else:
            dictCUSIPFileDt = None

        HldgDir = '/Users/shared/fund clone/'+fund[0]
        if not os.path.exists(HldgDir):
            continue

        p = re.compile('[,\s]')
        HldgFiles = glob(HldgDir+'/*.csv')

        dictCUSIPUpdated = False
        for HldgFile in HldgFiles:
            onefilename = os.path.splitext(os.path.basename(HldgFile))[0]
            if (onefilename <= '20160630'):
                continue

            try:
                with open(HldgFile, mode='rt') as infile:
                    HldgData  = [tuple(line) for line in csv.reader(infile)]
            except Exception as e:
                print(e)
                continue

            if (dictCUSIPFileDt):
                HldgFileDt= datetime.datetime.fromtimestamp(os.stat(HldgFile).st_mtime)
                if (dictCUSIPFileDt > HldgFileDt):
                    ##print(HldgFile+' is older than ' + dictCUSIPFile + '. Skipping.....')
                    continue

            HldgData = HldgData[3:]
            HldgPairs = [(p.sub('',row[0]), row[2]) for row in HldgData]


            ##print(len(HldgPairs))
            if (dictCUSIP):
                HldgPairs = set.difference(set(HldgPairs), set(dictCUSIP.items()))
            ##print(len(HldgPairs))



            diffCtr = 0
            i = 0
            j = len(HldgPairs)
            for Issuer, CUSIP in HldgPairs:
                i += 1
                ##print(str(i)+' of '+ str(j)+' : '+Issuer+' '+CUSIP)
                if (Issuer ==''):
                    next;

                ## if it's a known non-stock CUSIP
                ## continue
                if CUSIPIgnore:
                    if CUSIP in CUSIPIgnore:
                        continue

                if (dictCUSIPTicker):
                    if CUSIP in dictCUSIPTicker:
                        if dictCUSIP:
                            if Issuer in dictCUSIP:
                                if (dictCUSIP[Issuer] != CUSIP):
                                    diffCtr +=1
                                    dictCUSIP[Issuer] = CUSIP
                                    dictCUSIPUpdated = True
                            else:
                                dictCUSIP[Issuer] = CUSIP
                                dictCUSIPUpdated = True
                        else:
                            dictCUSIP[Issuer] = CUSIP
                            dictCUSIPUpdated = True
                    else:
                        curTicker = GetTickerfromCUSIP(CUSIP)
                        if (curTicker==''):
                            continue
                        elif curTicker is None:
                            CUSIPIgnore.update([CUSIP])
                            CUSIPIgnoreUpdated = True
                        else:
                            dictCUSIPTicker[CUSIP]=curTicker
                            dictCUSIPTickerUpdated = True

                            if dictCUSIP:
                                if Issuer in dictCUSIP:
                                    if (dictCUSIP[Issuer] != CUSIP):
                                        diffCtr +=1
                                        dictCUSIP[Issuer] = CUSIP
                                        dictCUSIPUpdated = True
                                else:
                                    dictCUSIP[Issuer] = CUSIP
                                    dictCUSIPUpdated = True
                            else:
                                dictCUSIP[Issuer] = CUSIP
                                dictCUSIPUpdated = True

                else:
                    curTicker = GetTickerfromCUSIP(CUSIP)
                    if (curTicker==''):
                        continue
                    elif curTicker is None:
                        CUSIPIgnore.update([CUSIP])
                        CUSIPIgnoreUpdated = True
                    else:

                        dictCUSIPTicker[CUSIP]=curTicker
                        dictCUSIPTickerUpdated = True

                        dictCUSIP[Issuer] = CUSIP
                        dictCUSIPUpdated = True

            ##print(HldgFile + ': ' + str(diffCtr))

            if dictCUSIPUpdated:
                SaveDict2CSV(dictCUSIPFile, dictCUSIP)
                dictCUSIPUpdated = False
            if dictCUSIPTickerUpdated:
                SaveDict2CSV(fileCUSIPTicker, dictCUSIPTicker)
                dictCUSIPTickerUpdated = False
            if CUSIPIgnoreUpdated:
                SaveSet2CSV(fileCUSIPIgnore, CUSIPIgnore)
                CUSIPIgnoreUpdated = False


    if dictCUSIPTickerUpdated:
        SaveDict2CSV(fileCUSIPTicker, dictCUSIPTicker)

#Find the different instrument types  and option types from 13F filings
# As it turns out, there are really just two types of instruments: PRN and SH
# and two types of options: Call, Put
def Collect13FShareType():
    ShareTypes = set()
    CallPutTypes = set()
    CUSIPSet = set()
    for fund in TargetFunds:
        #print(fund)
        HldgDir = '/Users/shared/fund clone/' + fund[0]
        if not os.path.exists(HldgDir):
            continue

        p = re.compile('[,\s]')
        HldgFiles = glob(HldgDir + '/*.csv')

        for HldgFile in HldgFiles:
            try:
                with open(HldgFile, mode='rt') as infile:
                    HldgData  = [tuple(line) for line in csv.reader(infile)]
            except Exception as e:
                print(e)
                continue

            HldgData = HldgData[3:]
            try:
                fileShareTypes = set([p.sub('',row[5]) for row in HldgData])
            except Exception as e:
                print(HldgFile)
                print(e)
                fileShareTypes = set()

            try:
                fileOptTypes = set([p.sub('', row[6]) for row in HldgData])
            except Exception as e:
                print(HldgFile)
                print(e)
                fileOptTypes = set()

            try:
                CUSIPs = set([p.sub('', row[2]) for row in HldgData])
            except Exception as e:
                print(HldgFile)
                print(e)
                CUSIPs = set()

            ShareTypes.update(fileShareTypes)
            CallPutTypes.update(fileOptTypes)
            CUSIPSet.update(CUSIPs)

    ShareTypes = sorted(list(ShareTypes))
    CallPutTypes = sorted(list(CallPutTypes))
    CUSIPSet = sorted(list(CUSIPSet))
    print(ShareTypes)
    print(CallPutTypes)
    with open('/Users/Shared/Models/ShareTypes13F.csv', 'w', encoding='utf-8') as csvfile:
        # writer = csv.writer(csvfile, delimiter=',', lineterminator='\r\n', quotechar = "'")
        writer = csv.writer(csvfile)
        writer.writerows([ShareTypes])

    with open('/Users/Shared/Models/OptionTypes13F.csv', 'w', encoding='utf-8') as csvfile:
        # writer = csv.writer(csvfile, delimiter=',', lineterminator='\r\n', quotechar = "'")
        writer = csv.writer(csvfile)
        writer.writerows([CallPutTypes])

    with open('/Users/Shared/Models/CUSIP13F.csv', 'w', encoding='utf-8') as csvfile:
        # writer = csv.writer(csvfile, delimiter=',', lineterminator='\r\n', quotechar = "'")
        writer = csv.writer(csvfile)
        for cusip in CUSIPSet:
            writer.writerow([cusip])


    return

def CUSIPUpperINLookup(fund):
    fundfile = '/Users/Shared/CUSIP Lookup/' + fund + '.csv'
    try:
        with open(fundfile, mode='r', encoding='utf-8') as infile:
            fundData = [tuple(line) for line in csv.reader(infile)]
    except Exception as e:
        print(e)
        return

    newData = []
    fundModified = False

    for line in fundData:
        curCUSIP = line[1]
        newCUSIP = curCUSIP.upper()

        if (curCUSIP == newCUSIP):
            newData += [line]
        else:
            newData += [(line[0], newCUSIP)]
            fundModified = True

    if fundModified:
        dictCUSIP = dict([line for line in newData])
        SaveDict2CSV(fundfile, dictCUSIP)
    return




def RemoveFilesCreatedAfter(cutoffDt):
    skipFunds = ['MAVERICKCAPITALLTDADV', 'SIGMACAPITALMANAGEMENTLLC','CRIntrinsicInvestorsLLC', 'CubistSystematicStrategiesLLC','RubricCapitalManagementLLC','EverPoint','SACCAPITALADVISORSLLC','SACCapitalAdvisorsLP','Maverick', 'Point72']
    i = 0
    for fund in TargetFunds:
        # print(fund)
        if fund[0] in skipFunds:
            continue
        HldgDir = '/Users/shared/fund clone/' + fund[0]

        if not os.path.exists(HldgDir):
            continue

        p = re.compile('[,\s]')
        HldgFiles = glob(HldgDir + '/*.csv')

        for HldgFile in HldgFiles:
            outputfiledt = datetime.datetime.fromtimestamp(os.stat(HldgFile).st_mtime)
            if outputfiledt > cutoffDt:
                os.remove(HldgFile)
                #print(HldgFile, ' created after ', str(cutoffDt))

        #i += 1
        #if i >= 10:
        #    break





Do13FCUSIPs()
#Collect13FShareType()
#for fund in TargetFunds:
#    CUSIPUpperINLookup(fund[0])

#cutoffDt = datetime.datetime.strptime("20160908", '%Y%m%d')
#RemoveFilesCreatedAfter(cutoffDt)


##dictCUSIPTickerUpdated = False
##for fund in TargetFunds:
##    print(fund)
##    fundSC13File = '/Users/Shared/SC13Forms/'+fund[0]+'.csv'
##    try:
##        with open(fundSC13File, mode='r', encoding='utf-8') as infile:
##            fundData = [tuple(line) for line in csv.reader(infile)]
##        fundCUSIPs = [line[3] for line in fundData]
##    except Exception as e:
##        print(e)
##        continue
##
##    fundCUSIPs = list(set(fundCUSIPs))
##
##    for CUSIP in fundCUSIPs:
##        if CUSIP in dictCUSIPTicker:
##            continue
##        else:
##            curTicker = GetTickerfromCUSIP(CUSIP)
##            if (curTicker==''):
##                continue
##            elif curTicker is None:
##                continue
##            else:
##                dictCUSIPTicker[CUSIP]=curTicker
##                dictCUSIPTickerUpdated = True
##                
##    if dictCUSIPUpdated:
##        SaveDict2CSV(dictCUSIPFile, dictCUSIP)
##        dictCUSIPUpdated = False
                    
        
        
        #print(IssuerNames)
##        #print(CUSIPs)
##        IssuerNames = [p.sub('',row[0]) for row in HldgData]
##        CUSIPs = [row[2] for row in HldgData]
##        tmpdict = dict(zip(IssuerNames, CUSIPs))
##
##        if dictCUSIP:
##            commonkeys = set(dictCUSIP).intersection(tmpdict)
##            diffCtr = 0
##            for key in commonkeys:
##                diffCtr += (dictCUSIP[key] != tmpdict[key])
##            print(diffCtr)
##            dictCUSIP.update(tmpdict)
##        else:
##            dictCUSIP=tmpdict.copy()
##        print(HldgFile + ':  ' + str(len(dictCUSIP)))
        
        
            

