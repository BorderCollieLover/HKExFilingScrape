#!/Users/mintang/anaconda3/bin/python

##Sample: 
##https://adesquared.wordpress.com/2013/06/16/using-python-beautifulsoup-to-scrape-a-wikipedia-table/

from bs4 import BeautifulSoup, NavigableString
from urllib.request import urlopen
import datetime
import re
from dateutil import parser
import csv
import os
import multiprocessing
from urllib.error import URLError, HTTPError
from pytz import timezone
import pytz
import locale
import logging
import time
from operator import itemgetter
import subprocess
import requests
from dateutil.relativedelta import relativedelta



isdst_now_in = lambda zonename: bool(datetime.datetime.now(pytz.timezone(zonename)).dst())

################################################################
##This script screens SC 13 filings and extracts the holding information
## Version 2.0
##The main change is on the part of scraping for share number and share percentage in a HTML filing
##Instead of having one big block of code within the ScrapeFilingData function, I move the related code out into two separate functions
##I also changed the part of finding nodes to anchor share number and share percentage search so that it is more flexible
##This is partly motivated by trying to scrape ICAHN filing data.

outputpath = '/Users/Shared/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
filescraped = 0
#filescrapedok = 0
#fundstat = []
logfilename = outputpath+ 'SC13Forms/@log/' +  format(datetime.datetime.now(), '%Y%m%d')+'.txt'
TargetFunds = []



##Added: July 30, 2015
##Adapted from a similar function in scrape options.py
##Calculate time to wait
##Filings are accepted and released during US working hours, roughly from 8am to 8pm
##During the working hour, return the usual one hour break
##Outside the working hour, wait until the next business day 
def Time2NextUSBusinessDay():
    currDt = datetime.datetime.now(timezone('EST'))
    MktCloseHour = 16
    MktOpenHour = 8 
    if isdst_now_in("America/New_York"):
        MktCloseHour -=1
        MktOpenHour -=1
    if (datetime.date.weekday(currDt) >= 5) or (currDt.hour >= MktCloseHour) or(currDt.hour < MktOpenHour)  :
        if (datetime.date.weekday(currDt) >= 5) or (currDt.hour >= MktCloseHour):
            nextDt = currDt + relativedelta(days=3)
        else:
            nextDt = currDt
        nextDt = nextDt.replace(hour=MktOpenHour, minute=0)
        print('wait until')
        print(nextDt)
        print((nextDt - currDt).total_seconds())
        return((nextDt - currDt).total_seconds())
    else:
        #minOffset = 180
        nextDt = currDt
        nextDt =  nextDt.replace(hour=MktCloseHour, minute=30)
        print('wait until')
        print(nextDt)
        return((nextDt - currDt).total_seconds())


##compare a data table to the csv file it is to be written to
## return true is the file exists and the data contained therein is identical to data
## else return false        
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

def RemoveDuplicatedLines(data):
    if (len(data) <= 0):
        return(data)
    else:
        newData = []
        lastLn = None
        for lnIdx, line in enumerate(data):
            if lnIdx == 0:
                newData += [line]
                lastLn = line
            else:
                if not (lastLn == line):
                    newData += [line]
                    lastLn = line
        return(newData)
                


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

        onetable = RemoveDuplicatedLines(onetable)

        if not CompareData2CSV(onetable, filename):
            with open(filename, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(onetable)

def CleanString(inputStr):
    if not inputStr:
        return('')
    else:
        p = re.compile(',')
        inputStr = p.sub(' ',inputStr)
        p = re.compile('_')
        inputStr = p.sub(' ',inputStr)
        p = re.compile('-')
        inputStr = p.sub(' ',inputStr)
        p = re.compile('\<\s*FONT\s*.*\>')
        inputStr = p.sub('',inputStr)
        p = re.compile('\<\s*DIV\s*.*\>')
        inputStr = p.sub('',inputStr)
        
        p = re.compile('\<\s*br\s*(\/)?\>', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\<\s*\/FONT\s*\>', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\<\s*\/TD\s*\>', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\<\s*\/B\s*\>', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\<\s*\/p\s*\>', re.I)
        inputStr = p.sub(' ',inputStr)
        p = re.compile('\<\s*\/div\s*\>', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\<\s*\/CENTER\s*\>', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\*', re.I)
        inputStr = p.sub('',inputStr)
        p = re.compile('\s+')
        inputStr = p.sub(' ',inputStr)
        inputStr = inputStr.strip()
        if (inputStr ==' '):
            inputStr=''
        return(inputStr)
        
##CleanString2 is from Scrape13F
def CleanString2(inputStr):
    if not inputStr:
        return('')
    else:
        p = re.compile('\s')
        inputStr = p.sub(' ',inputStr)
        p = re.compile(',')
        inputStr = p.sub('',inputStr)
        inputStr = inputStr.strip()
        return(inputStr)

def FileMissingorStale(filename):
    if not os.path.exists(filename):
        return True

    filetime = datetime.datetime.fromtimestamp( os.path.getmtime(filename))
    if ((datetime.datetime.now() - filetime) > datetime.timedelta(hours=240)):
        return True
    else:
        return False

def CleanAllDuplicatedLines():
    FinalFunds = list(set([line[0] for line in TargetFunds]))

    for fund in FinalFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund+'.csv'
        if FileMissingorStale(fundfile):
            continue
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            ##print(e)
            continue

        newData = RemoveDuplicatedLines(fundData)
        DumpTable(newData, fundfile)
    

def CUSIPUpper(fund):
    fundfile = '/Users/Shared/SC13Forms/' + fund + '.csv'
    try:
        with open(fundfile, mode='r', encoding='utf-8') as infile:
            fundData = [tuple(line) for line in csv.reader(infile)]
    except Exception as e:
        print(e)
        return

    newData = []
    fundModified = False

    for line in fundData:
        curCUSIP = line[3]
        newCUSIP = curCUSIP.upper()

        if (curCUSIP == newCUSIP):
            newData += [line]
        else:
            newData += [(line[0], line[1], line[2], newCUSIP, line[4], line[5], line[6], line[7])]
            fundModified = True

    if fundModified:
        DumpTable(newData, fundfile)
    return

#CUSIPUpper('Point72')

def CleanCUSIPviaMapping():
    CUSIPMappingfile = '/Users/shared/models/CUSIPMapping.csv'
    CUSIPMapping = {}
    try:
        with open(CUSIPMappingfile, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            CUSIPMapping = dict(reader)
    except Exception as e:
        print(e)
        return(None)

    print('Cleaning up CUSIPs.....')
    FinalFunds = list(set([line[0] for line in TargetFunds]))


    for fund in FinalFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund+'.csv'
        if FileMissingorStale(fundfile):
            continue
        
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            print(e)
            continue

        newData = []
        fundModified = False

        for line in fundData:
            curCUSIP = line[3]
            if curCUSIP in CUSIPMapping:
                
                newCUSIP = CUSIPMapping[curCUSIP]
                newData += [(line[0], line[1], line[2], newCUSIP, line[4], line[5],line[6], line[7])]
                fundModified=True
            else:
                inputStr = curCUSIP
                p = re.compile(',')
                inputStr = p.sub(' ',inputStr)
                p = re.compile('_')
                inputStr = p.sub(' ',inputStr)
                p = re.compile('-')
                inputStr = p.sub(' ',inputStr)
                p = re.compile('\<\s*FONT\s*.*\>')
                inputStr = p.sub('',inputStr)
                p = re.compile('\<\s*DIV\s*.*\>')
                inputStr = p.sub('',inputStr)
                
                p = re.compile('\<\s*br\s*(\/)?\>', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\<\s*\/FONT\s*\>', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\<\s*\/TD\s*\>', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\<\s*\/B\s*\>', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\<\s*\/p\s*\>', re.I)
                inputStr = p.sub(' ',inputStr)
                p = re.compile('\<\s*\/div\s*\>', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\<\s*\/CENTER\s*\>', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\*', re.I)
                inputStr = p.sub('',inputStr)
                p = re.compile('\s+')
                inputStr = p.sub('',inputStr)
                inputStr = inputStr.strip()
                newCUSIP = inputStr
                if (curCUSIP == newCUSIP):
                    newData += [line]
                else:
                    newData += [(line[0], line[1], line[2], newCUSIP, line[4], line[5],line[6], line[7])]
                    fundModified = True
                    

        if fundModified:
            DumpTable(newData, fundfile)

## Add on June 8, 2015
## Add a function to find the most probable CUSIP from a set
## Find the string that is of 9 characters, if none exists, find the string whose length is least greater than 9
def FindMostProbableCUSIP(CUSIPset):
    CUSIPset = list(CUSIPset)
    if (len(CUSIPset) == 1):
        return(CUSIPset[0])
    else:
        #find the string of length 9 
        for curCUSIP in CUSIPset:
            if (len(curCUSIP) == 9):
                return(curCUSIP)
            
        #find the string that is least longer than 9
        tmpCUSIPlen = 100;
        tmpCUSIP = None
        for curCUSIP in CUSIPset:
            curLenDiff = len(curCUSIP) -9
            if (curLenDiff > 0):
                if (curLenDiff < tmpCUSIPlen):
                    tmpCUSIPlen = curLenDiff
                    tmpCUSIP = curCUSIP

        if tmpCUSIP:
            return(tmpCUSIP)
        else:
            return(CUSIPset[0])

##This function fills in missing CUSIPs by checking the SC13 filing data for underlyings with the same issuer name
##For each line:
##  1) If the CUSIP field is empty but there is an issuer name:
##      1.a) Scan the file to see if there is another entry with a CUSIP number
def FillCUSIPbySC13Data():
    FinalFunds = list(set([line[0] for line in TargetFunds]))

    for fund in FinalFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund+'.csv'
        if FileMissingorStale(fundfile):
            continue
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            ##print(e)
            continue

        newData = []
        fundModified = False
        for line in fundData:
            curCUSIP = line[3]
            curIssuer = line[1]
            if (curIssuer ==''):
                newData +=[line]
                continue
            else:
                if (curCUSIP ==''):
                    issuerData = [ tuple(line) for line in fundData if line[1]== curIssuer ]
                    issuerCUSIPs = set([line[3] for line in issuerData])
                    issuerCUSIPs.discard(curCUSIP)
                    if (len(issuerCUSIPs)>0):
                        ## Add on June 8, 2015
                        ## Add a function to find the most probable CUSIP from a set
                        newCUSIP = FindMostProbableCUSIP(issuerCUSIPs)
                        if (line[5] ==''):
                            newShareNum=-1
                        else:
                            newShareNum=line[5]
                        if (line[6] ==''):
                            newSharePct=-1
                        else:
                            newSharePct=line[6]
                        
                        newData += [(line[0], line[1], line[2], newCUSIP, line[4], newShareNum,newSharePct, line[7])]
                        fundModified=True
                    else:
                        newData += [line]
                else:
                    newData += [line]

        if (fundModified):
            DumpTable(newData, fundfile)

def CleanWrongShareNumfromCUSIP(fund):
    fundfile = '/Users/Shared/SC13Forms/' + fund + '.csv'
    if FileMissingorStale(fundfile):
        return

    try:
        with open(fundfile, mode='r', encoding='utf-8') as infile:
            fundData = [tuple(line) for line in csv.reader(infile)]
    except Exception as e:
        return

    fundModified = False
    newData = []
    for line in fundData:
        curCUSIP = line[3]
        curShareNum = line[5]

        if ((curCUSIP == curShareNum) and (curCUSIP != '')):
            newData += [(line[0], line[1], line[2], line[3], line[4], '-1', line[6], line[7])]
            fundModified = True
        else:
            newData += [line]
    if (fundModified):
        DumpTable(newData, fundfile)

#CleanWrongShareNumfromCUSIP('Maverick')
def CleanAllWrongShareNumfromCUSIP():
    FinalFunds = list(set([line[0] for line in TargetFunds]))

    for fund in FinalFunds:
        try:
            CleanWrongShareNumfromCUSIP(fund)
        except Exception as e:
            print(e)

#CleanAllWrongShareNumfromCUSIP()
##This function fills in missing CUSIPs by checking the issuer--cusip mapping from Qtrly filing
def FillCUSIPbyQtrFiling():
    FinalFunds = list(set([line[0] for line in TargetFunds]))

    for fund in FinalFunds:
        fundfile =  '/Users/Shared/SC13Forms/'+fund+'.csv'
        if FileMissingorStale(fundfile):
            continue
        
        try:
            with open(fundfile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            ##print(e)
            continue

        dictCUSIPFile = '/Users/Shared/CUSIP Lookup/'+fund+'.csv'
        dictCUSIP = {}
        try:
            with open(dictCUSIPFile, mode='r') as infile:
                reader = csv.reader(infile)
                dictCUSIP = dict(reader)
        except Exception as e:
            ##print(e)
            continue;

        p = re.compile('[,\s]')

        newData = []
        fundModified = False
        for line in fundData:
            curCUSIP = line[3]
            curIssuer = line[1]
            if (curIssuer ==''):
                newData +=[line]
                continue
 
            if (curCUSIP ==''):
                curIssuer = p.sub('',CleanString2(curIssuer))

                if (curIssuer in dictCUSIP):
                    newCUSIP = dictCUSIP[curIssuer]
                    if (line[5] ==''):
                        newShareNum=-1
                    else:
                        newShareNum=line[5]
                    if (line[6] ==''):
                        newSharePct=-1
                    else:
                        newSharePct=line[6]
                    
                    newData += [(line[0], line[1], line[2], newCUSIP.upper(), line[4], newShareNum,newSharePct, line[7])]
                    fundModified=True
                else:
                    newData += [line]
            else:
                newData += [line]

        if (fundModified):
            DumpTable(newData, fundfile)


####Find the sibling node that has the same tag
####direction: next sibling or previous sibling
####The script first looks for "marker texts" such as "cusip number" in an HTML
####It then tries to look for the actual data item in the preceeding (or following) node with the same Tag
####Works for most well-structured HTML filings
def findSiblingwithTag(thisNode, NodeTag, direction):
    if (thisNode is None):
        return(None)

    try:
        SiblingNode = None
        if (direction == 'next'):
            if ( thisNode.find_next_sibling(NodeTag) is not None):
                thisNode = thisNode.find_next_sibling(NodeTag)
                if thisNode.get_text().strip():
                    SiblingNode = thisNode
                else:
                    SiblingNode = findSiblingwithTag(thisNode, NodeTag, direction)
            else:
                thisNode = thisNode.parent;

                while ((not thisNode is None) and (thisNode.find_next_sibling(thisNode.name) is None)):
                    thisNode = thisNode.parent

                if (thisNode is not None):
                    thisNode = thisNode.find_next_sibling(thisNode.name)
                    while True:
                        if (thisNode.find(NodeTag) is not None) and thisNode.find(NodeTag).get_text().strip():
                            SiblingNode = thisNode.find(NodeTag)
                            break;
                        elif (thisNode.find_next_sibling(thisNode.name) is not None):
                            thisNode = thisNode.find_next_sibling(thisNode.name)
                        else:
                            SiblingNode = findSiblingwithTag(thisNode, NodeTag, direction)
                            break
                else:
                    SiblingNode = None
        elif (direction == 'previous'):
            if (thisNode.find_previous_sibling(NodeTag) is not None):
                thisNode = thisNode.find_previous_sibling(NodeTag)
                if thisNode.get_text().strip():
                    SiblingNode = thisNode
                else:
                    SiblingNode = findSiblingwithTag(thisNode, NodeTag, direction)
            else:
                thisNode = thisNode.parent;
                
                while ((thisNode is not None) and (thisNode.find_previous_sibling(thisNode.name) is None)):
                    thisNode = thisNode.parent
                if (thisNode is not None):
                    thisNode = thisNode.find_previous_sibling(thisNode.name)
                    while True:
                        if (thisNode.find(NodeTag) is not None) and thisNode.find(NodeTag).get_text().strip():
                            SiblingNode = thisNode.find(NodeTag)
                            break;
                        elif (thisNode.find_previous_sibling(thisNode.name) is not None):
                            thisNode = thisNode.find_previous_sibling(thisNode.name)
                        else:
                            SiblingNode = findSiblingwithTag(thisNode, NodeTag, direction)
                            break
                else:
                    SiblingNode = None
        else:
            SiblingNode = None
            print(direction + ' is a wrong parameter for finding sibling.')

        return(SiblingNode)
    except Exception as e:
        print(e)
        return(None)


def FindIssuerTxtFiling(data):
    IssuerName = None
    for lineidx, line in enumerate(data):
        line=line.strip()
        IssuerAnchored=False
        if re.match('^[\(]*\s*Name\s+of\s+Issuer\s*[\)]*', line, re.IGNORECASE):
            IssuerAnchored=True
            for tmpidx in reversed(range(lineidx)):
                line=data[tmpidx]
                line=line.strip()
                if re.search('(\()?\s*Amendment', line, re.I):
                    break;
                if re.search('Under\s+the\s+Securities', line, re.I):
                    break; 
                if re.match('^[a-zA-Z0-9]', line):
                    IssuerName = line
                    break;
            if IssuerAnchored:
                break;
    return(IssuerName)


def FindCUSIPTxtFiling(data):
    CUSIPNumber = None
    CUSIPAnchored = False
    for lineidx, dataline in enumerate(data):
        dataline=dataline.strip()
        CUSIPAnchored=False
        if re.match('^[\(]*\s*(CUSIP|SEDOL|ISIN)\s+Number\s*[\)]*', dataline, re.IGNORECASE):
            CUSIPAnchored=True
            for tmpidx in reversed(range(lineidx)):
                line=data[tmpidx]
                line=line.strip()
                if re.search('(\()?\s*Title', line, re.I):
                    break;
                if re.search('par\s+value', line, re.I):
                    break;
                if re.match('^[a-zA-Z0-9]', line):
                    CUSIPNumber = line
                    break;
            if CUSIPAnchored:
                break;

    ##July 26, 2015
    ##Look for CUSIP number at the same line where "CUSIP Number" 
    if ((CUSIPNumber is None) and CUSIPAnchored):
        leadingpos = re.match('[\(]*\s*(CUSIP|SEDOL|ISIN)\s+Number\s*(\:)?[\)]*(\:)?', dataline, re.I)
        if leadingpos:
            CUSIPStr = dataline[leadingpos.end():]
            CUSIPStr = CUSIPStr.strip()
            if (len(CUSIPStr) >=9):
                CUSIPNumber = CUSIPStr
    ##end of July 26

    return(CUSIPNumber)

def FindEventDtTxtFiling(data):
    EventDt = None
    for lineidx, line in enumerate(data):
        line=line.strip()
        DtAnchored=False
        if re.match('^[\(]*\s*Date\s+of\s+Event\s+Which', line, re.IGNORECASE):
            DtAnchored=True
            for tmpidx in reversed(range(lineidx)):
                line=data[tmpidx]
                line=line.strip()
                if re.search('\(', line):
                    break;
                if re.search('CUSIP', line, re.I):
                    break;
                if re.match('^[a-zA-Z0-9]', line):
                    try:
                        tmpEventDt = parser.parse(line)
                    except Exception as e:
                        tmpEventDt =None
                    if tmpEventDt:
                        EventDt = line
                        break; 
            if DtAnchored:
                break;
    return(EventDt)

def FindShareNumTxtFiling(data):
    ShareNum = None
    tmpShareNum = None
    for lineidx, dataline in enumerate(data):
        dataline=dataline.strip()
        tmpShareNum = None
        #^[9]{,1}\s*[\.]{,1}
        if re.match('((\()?(9|11)(\s*[\.]\s*)?(\))?\s+)?Aggregate\s+Amount\s+(Beneficially\s+)?(Owned\s+)?(By\s+)?(Each\s+)?(Reporting\s+)?(Person)?', dataline, re.IGNORECASE):
            for tmpidx in range((lineidx+1), len(data)):
                line=data[tmpidx]
                line=line.strip()
                
                if re.match('Check\s+if', line):
                    break;
                if re.match('^[0-9]', line):
                    ShareNumEnd = re.search('[^0-9\,]', line)
                    if ShareNumEnd is None:
                        ShareNumStr = line
                    else:
                        ShareNumStr = line[:(ShareNumEnd.start())]
                    try:
                        tmpShareNum = locale.atoi(ShareNumStr)
                    except Exception as e:
                        tmpShareNum = None

                    if (tmpShareNum < 100) and (tmpShareNum >0):
                        tmpShareNum = None

                    if tmpShareNum:
                        if ShareNum is None:
                            ShareNum = tmpShareNum
                        else:
                            if tmpShareNum > ShareNum:
                                ShareNum = tmpShareNum
                                
                    break;
            ##July 24, 2015
            ##If no number if found in the following lines, try the remainder of the line
            #print(tmpShareNum)
            if tmpShareNum is None:
                ShareNumEnd = re.search('((\()?(9|11)(\s*[\.]\s*)?(\))?\s+)?Aggregate\s+Amount\s+(Beneficially\s+)?(Owned\s+)?(By\s+)?(Each\s+)?(Reporting\s+)?(Person)?', dataline, re.IGNORECASE)
                ShareNumStr = dataline[(ShareNumEnd.end()):]
                ShareNumEnd = re.search('[^0-9\,]', ShareNumStr)
                if ShareNumEnd:
                    ShareNumStr = ShareNumStr[:(ShareNumEnd.start())]
                try:
                    tmpShareNum = locale.atoi(ShareNumStr)
                except Exception as e:
                    tmpShareNum = None

                if tmpShareNum:
                    if ShareNum is None:
                        ShareNum = tmpShareNum
                    else:
                        if tmpShareNum > ShareNum:
                            ShareNum = tmpShareNum

    return(ShareNum)
    
def FindSharePercentTxtFiling(data):
    SharePercent = None
    tmpSharePercent = None 
    for lineidx, dataline in enumerate(data):
        dataline=dataline.strip()
        tmpSharePercent = None
        #^[9]{,1}\s*[\.]{,1}
        if re.match('((\()?(11|13)(\s*[\.]\s*)?(\))?\s+)?Percent\s+of\s+Class\s+Represented\s+By\s+(Amount\s+in\s+)?(Row)?', dataline, re.IGNORECASE):
            for tmpidx in range((lineidx+1), len(data)):
                line=data[tmpidx]
                line=line.strip()
                
                if re.match('Type\s+of', line):
                    break;
                if re.match('^[0-9]', line):
                    SharePercentEnd = re.search('[^0-9\.]', line)
                    if SharePercentEnd is None:
                        SharePercentStr = line
                    else:
                        SharePercentStr = line[:(SharePercentEnd.start())]
                    try:
                        tmpSharePercent = locale.atof(SharePercentStr)
                    except Exception as e:
                        tmpSharePercent = None

                    if ((tmpSharePercent == 12) or (tmpSharePercent == 13)or (tmpSharePercent == 14) or (tmpSharePercent >=100) ):
                        tmpSharePercent = None

                    if tmpSharePercent:
                        if SharePercent is None:
                            SharePercent = tmpSharePercent
                        else:
                            if tmpSharePercent > SharePercent:
                                SharePercent = tmpSharePercent
                    break;

            ##July 24, 2015
            ##If no number if found in the following lines, try the remainder of the line
            #print(tmpShareNum)
            if tmpSharePercent is None:
                SharePercentEnd = re.search('[1-9]?\d(\.\d+)?(\s*)%', dataline, re.IGNORECASE)
                if (SharePercentEnd):
                    SharePercentStr = dataline[(SharePercentEnd.start()):(SharePercentEnd.end()-1)]
                    #print(SharePercentStr)
                    try:
                        tmpSharePercent = locale.atof(SharePercentStr)
                    except Exception as e:
                        tmpSharePercent = None

                    if tmpSharePercent:
                        if SharePercent is None:
                            SharePercent = tmpSharePercent
                        else:
                            if tmpSharePercent > SharePercent:
                                SharePercent = tmpSharePercent

    return(SharePercent)

##Added on June 8, 2015        
def ScrapeTxtFilingData(dataurl, fundname):
    try:
        data = requests.get(dataurl)
        data = data.text
        data = data.split('\n')
        time.sleep(2)
    except Exception as e:
        print(e)
        return None
    IssuerName = FindIssuerTxtFiling(data)
    if IssuerName is None:
        IssuerName = ''
    CUSIPNumber = FindCUSIPTxtFiling(data)
    if CUSIPNumber is None:
        CUSIPNumber = ''
    EventDt  = FindEventDtTxtFiling(data)
    if EventDt is None:
        EventDt =''
    ShareNum = FindShareNumTxtFiling(data)
    if ShareNum is None:
        ShareNum = -1
    SharePercent= FindSharePercentTxtFiling(data)
    if SharePercent is None:
        SharePercent = -1 
    if ((ShareNum <=99) and (ShareNum >0)):
        ShareNum = -1 
    if (ShareNum == -1) and ((SharePercent == 12) or (SharePercent == 13)or (SharePercent == 14) ):
        SharePercent = -1
    if (SharePercent > 100):
        SharePercent = -1 
    return(CleanString(IssuerName), CleanString(CUSIPNumber).upper(), CleanString(EventDt), ShareNum, SharePercent, dataurl)

#dataurl = 'http://www.sec.gov/Archives/edgar/data/728014/000072801415000008/vrxa.txt'
#print(ScrapeTxtFilingData(dataurl, 'foo'))

## April 19, 2016:  ScrapeFilingData is changed in version 2.
##  Move the code scraping for share numbers and share percentages out as two separate functions
##

## Find the Share Number data
## thisNode is a pointer to a tree that contains the text "AGGREGATE ......."
def ScrapeShareNumberHTML(shareCountlink, dataTag):
    inputLink = shareCountlink
    shareCountNum = -1
    if shareCountlink:
        shareCountNumStr = ""
        while not shareCountNumStr:
            shareCountlink = findSiblingwithTag(shareCountlink, dataTag, 'next')
            if shareCountlink:
                shareCount = shareCountlink.get_text().strip().split()
                if (len(shareCount) > 0):
                    for shareCountStr in shareCount:
                        if re.match('[0-9,]+', shareCountStr):
                            shareCountNumStr = shareCountStr
                            break;
            else:
                break;
        if (shareCountNumStr):
            if re.search('[^\d,]', shareCountNumStr) is None:
                shareCountNum = locale.atoi(shareCountNumStr)
            else:
                m = re.search('[^\d,]', shareCountNumStr)
                shareCountNumStr = shareCountNumStr[:m.start(0)]
                shareCountNum = locale.atoi(shareCountNumStr)
        else:
            shareCountNum = -1

        if (shareCountNum <= 99) and (shareCountNum > 0):
            shareCountNum = -1

        ##July 24, 2015
        ##Try to recoup shareCountNum from the same line as the lead text 'AGGREGATE AMOUNT BENEFICIALLY OWNED'
        ## when the above fails.
        ## This is useful for filings from ThirdAve.
        # print(shareCountNum)
        if (shareCountNum == -1):
            shareCountlink = inputLink
            shareCountStr = shareCountlink.get_text()
            if (shareCountStr):
                shareCountPos = re.search('[1-9]\d{0,2}(\,\d{3})*', shareCountStr)
                if shareCountPos:
                    try:
                        shareCountNum = locale.atoi(shareCountStr[shareCountPos.start():shareCountPos.end()])
                    except Exception as e:
                        shareCountNum = -1


    if (shareCountNum <= 99) and (shareCountNum > 0):
        shareCountNum = -1

    return(shareCountNum)


def ScrapeSharePercentHTML(sharePercentlink, dataTag):
    inputLink = sharePercentlink
    sharePercentNum = -1

    if sharePercentlink:
        sharePercentNumStr = ""
        while not sharePercentNumStr:
            sharePercentlink = findSiblingwithTag(sharePercentlink, dataTag, 'next')
            if (not sharePercentlink is None):
                sharePercent = sharePercentlink.get_text().strip().split()
                if (len(sharePercent) > 0):
                    for sharePercentStr in sharePercent:
                        if (re.match('\d+(\.\d+)*(\s)*(%){0,1}', sharePercentStr)):
                            sharePercentNumStr = sharePercentStr
                            break;
            else:
                break;
        if (sharePercentNumStr):
            if re.search('%', sharePercentNumStr) is None:
                sharePercentNumStr = sharePercentNumStr.strip('%')
                sharePercentNum = locale.atof(sharePercentNumStr)
            else:
                sharePercentNumStr = sharePercentNumStr.split('%')[0].strip()
                p = re.compile('[^\d\.]')
                sharePercentNumStr = p.sub('', sharePercentNumStr)
                sharePercentNum = locale.atof(sharePercentNumStr.split('%')[0].strip())
        else:
            sharePercentNum = -1

        if (sharePercentNum == 1) or (sharePercentNum == 3) or (sharePercentNum == 4) or (
            sharePercentNum == 12) or (sharePercentNum == 13) or (sharePercentNum == 14):
            sharePercentNum = -1
        if (sharePercentNum > 100):
            sharePercentNum = -1


        ##July 24, 2015
        ##Try to recoup sharePercentage from the same line as the lead text 'AGGREGATE AMOUNT BENEFICIALLY OWNED'
        ## when the above fails.
        ## This is useful for filings from ThirdAve.
        if (sharePercentNum == -1):
            sharePercentlink = inputLink
            sharePercentNumStr = sharePercentlink.get_text()
            if (sharePercentNumStr):
                sharePercentPos = re.search('[1-9]?\d(\.\d+)?(\s*)%', sharePercentNumStr)
                if sharePercentPos:
                    try:
                        sharePercentNum = locale.atof(
                            sharePercentNumStr[sharePercentPos.start():(sharePercentPos.end() - 1)])
                    except Exception as e:
                        sharesPercentNum = -1
        # end of July 24, 2015


        if (sharePercentNum == 1) or (sharePercentNum == 3) or (sharePercentNum == 4) or (
                    sharePercentNum == 12) or (sharePercentNum == 13) or (sharePercentNum == 14)  or (sharePercentNum == 16):
            sharePercentNum = -1
        if (sharePercentNum > 100):
            sharePercentNum = -1

    return(sharePercentNum)

def TreeFind (thisNode, tgtTag, tgtStr, debugging=False):
    if thisNode is None:
        return None

    if not (tgtTag or tgtStr):
        return [thisNode]

    #tgtStr is empty, find all subtrees with the particular tag
    outputNodes = []
    if not tgtStr:
        outputNodes = thisNode.findAll(tgtTag)
        if (thisNode.name == tgtTag):
            outputNodes += [thisNode]
        return(outputNodes)

    #tgtTag is empty, find only subtrees with the particular string
    #or if the tag of the current node matches tgtTag, just check if there is a subtree that contains a navigable string matching the input pattern
    # if such a subtree exists, return thisNode ; in addition, if the subtree also has a tag matching tgtTag, return the subtree as well
    if (debugging):
        print(outputNodes)
    if (not tgtTag) or (thisNode.name == tgtTag):
        for child in thisNode.contents:
            if isinstance(child, NavigableString):
                if debugging:
                    print('comparing strings......')
                if re.search(re.compile(tgtStr, re.I), child.string):
                    if debugging:
                        print(tgtStr)
                        print(child.string)
                    outputNodes += [thisNode]
            else:
                if debugging:
                    print(child)
                    print(tgtStr)
                subtreeNodes = TreeFind(child, '', tgtStr, debugging)
                if subtreeNodes:
                    if tgtTag:
                        for curNode in subtreeNodes:
                            if (curNode.name == tgtTag):
                                outputNodes += [curNode]
                    if not outputNodes:
                        outputNodes += [thisNode]
                if debugging:
                    print('output....')
                    print(outputNodes)
                    print('end of output')
    else:
        #tgtTag and tgtStr are specified, and current Node doesn't match the tgtTag:
        taggedNodes = thisNode.findAll(tgtTag)
        if debugging:
            print(len(taggedNodes))
            print(len(taggedNodes[0].contents))

        for taggedNode in taggedNodes:
            subtreeNodes = TreeFind(taggedNode, tgtTag, tgtStr, debugging)
            if subtreeNodes:
                outputNodes += subtreeNodes

    return(outputNodes)


def ScrapeFilingData(dataurl, fundname):
    summaryURL = dataurl
    global filescrapedok, filescraped

    if (summaryURL.find(".txt") >0 ) :
        ##Added on June 8, 2015
        resulttuple = ScrapeTxtFilingData(dataurl, fundname)
        return (resulttuple)
    elif summaryURL.endswith('/'):
        resulttuple = ('','', '', '', '', dataurl)
        return(resulttuple)
    else:
        filescraped += 1;
        cusipNum=""
        issuerName =''
        eventDate =""
        sharesNum = -1
        sharesPercentage = -1 

        try:
            summaryPage = urlopen(summaryURL, timeout=100)
            soup = BeautifulSoup(summaryPage)
            time.sleep(2)

            #### broadly speaking, header items are tagged with one of the three tags below
            #### header items are: CUSIP Number, Issuer Name, and Date of Event
            #### Tags of data items, i.e. aggregate share numbers and percentage of shares,
            #### may also vary, sometimes depending on the header item tags
            #### There is no strict and hard rules though 
            headerTags = ['div', 'td', 'p']
            headerTag = headerTags[0]
            for headerTag in headerTags:
                cusiplink = soup.find(headerTag, text=re.compile('\(\s*(CUSIP|SEDOL|ISIN)\s+Number\s*\)', re.I))
                if not cusiplink is None:
                    break;
            if cusiplink is None:
                for headerTag in headerTags:
                    cusiplink = soup.find(headerTag, text=re.compile('(CUSIP|SEDOL|ISIN)\s+Number', re.I))
                    if not cusiplink is None:
                        break;
                
            if (headerTag == 'td') | (headerTag == 'p'):
                if (fundname == 'Cooperman'):
                    dataTag = 'td'
                else:
                    dataTag = 'p'
            else:
                dataTag ='div'


            if (cusiplink is None):
                print('Cannot find CUSIP number for ' + fundname + ' at ' + dataurl)
                logging.warning('Cannot find CUSIP number for ' + fundname + ' at ' + dataurl)
            else:
                cusiplink = findSiblingwithTag(cusiplink, headerTag, 'previous')
                if (not cusiplink is None):
                    cusipNum = cusiplink.get_text().strip()

            datelink = soup.find(headerTag, text=re.compile('Date\s+of\s+Event', re.I))
            datelink = findSiblingwithTag(datelink, headerTag, 'previous')
            if (not datelink is None):
                eventDate = datelink.get_text().strip()

            
            ##Add on June 16, 2015 to remove wrong eventDate strings
            try:
                parser.parse(eventDate)
            except Exception as e:
                eventDate =''



            #April 19, 2016
            #Change the code here:
            #1. If we find a list of sharesNum link and sharesPercentlink in tables:
            # 1.a Find shareCountLink and sharePercentLink pairs within tables
            # 1.b scrape for shareNum and sharesPercentage and keep only the
            #2. If we have tried the above and both sharesNum and sharesPercent are still -1
            #2.a. Scrape for sharesNum use TreeFind to identify sharesCount links to scrape, keep the highest number
            #2.b Similarly using TreeFind to identify links and scrape for sharesPercent
            summarytables = soup.findAll('table')
            for onetable in summarytables:
                #print('here again-------------------------------------------------------')
                shareCountlink = None
                shareCountNum = -1
                sharePercentNum = -1
                try:
                    shareCountlink = onetable.find(dataTag, text=re.compile('AGGREGATE\s+AMOUNT\s+BENEFICIALLY\s+OWNED', re.I))
                except Exception as e:
                    shareCountlink = None

                if shareCountlink:
                    shareCountNum = ScrapeShareNumberHTML(shareCountlink, dataTag)

                try:
                    sharePercentlink = onetable.find(dataTag, text=re.compile('PERCENT\s+OF\s+CLASS\s+REPRESENTED', re.I))
                except Exception as e:
                    sharePercentlink = None

                if sharePercentlink:
                    sharePercentNum = ScrapeSharePercentHTML(sharePercentlink, dataTag)

                if shareCountNum and (shareCountNum > sharesNum):
                    sharesNum = shareCountNum
                    sharesPercentage = sharePercentNum

                if ((sharesNum <= 99) and (sharesNum >0)):
                    sharesNum = -1
                    if (sharesPercentage == 1) or (sharesPercentage == 3) or (sharesPercentage ==4) or (sharesPercentage == 12) or (sharesPercentage == 13) or (sharesPercentage == 14):
                        sharesPercentage = -1

                if (sharesPercentage > 100):
                    sharesPercentage = -1

                if (sharesNum >=3000) and (sharesPercentage ==0):
                    sharesPercentage = -1

            if (sharesPercentage == -1 ) or (sharesNum == -1):
                shareCountlinks = TreeFind(soup, dataTag, 'AGGREGATE\s+AMOUNT\s+BENEFICIALLY\s+OWNED')
                for shareCountlink in shareCountlinks:
                    shareCountNum = ScrapeShareNumberHTML(shareCountlink, dataTag)
                    if (shareCountNum > sharesNum):
                        sharesNum = shareCountNum

                sharePercentlinks = TreeFind(soup, dataTag, 'PERCENT\s+OF\s+CLASS\s+REPRESENTED')
                for sharePercentlink in sharePercentlinks:
                    sharePercentNum = ScrapeSharePercentHTML(sharePercentlink, dataTag)
                    if (sharePercentNum > sharesPercentage):
                        sharesPercentage = sharePercentNum

                if ((sharesNum <= 99) and (sharesNum > 0)):
                    sharesNum = -1
                    if (sharesPercentage == 1) or (sharesPercentage == 3) or (
                        sharesPercentage == 4) or (sharesPercentage == 12) or (sharesPercentage == 13) or (
                        sharesPercentage == 14):
                        sharesPercentage = -1

                if (sharesPercentage > 100):
                    sharesPercentage = -1

                if (sharesNum >= 3000) and (sharesPercentage == 0):
                    sharesPercentage = -1

            ##IssuerName should be empty here
            ##Added IssuerName on June 8 to be consistent with scraping txt filing
            resulttuple = (issuerName, CleanString(cusipNum).upper(), CleanString(eventDate), sharesNum, sharesPercentage, dataurl)
            return(resulttuple)
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
        except Exception as e:
            print('Error occured when scraping for ' + fundname)
            print(e)
            print(dataurl)
            logging.warning(fundname + ': Error at ' + dataurl)
            logging.warning(e)
            ##IssuerName should be empty here
            ##Added IssuerName on June 8 to be consistent with scraping txt filing
            resulttuple = (issuerName,CleanString(cusipNum).upper(), CleanString(eventDate), sharesNum, sharesPercentage, dataurl)
            return(resulttuple)
        return None
    
# tmpurl = 'http://www.sec.gov/Archives/edgar/data/859737/000092846416000189/holxsch13damd9041316.htm'
# x = ScrapeFilingData(tmpurl, 'foo')
# print(x)

def ScrapeOneFiling(fundname, filinglink, scrapedData):
    try:

        filingpage = urlopen(filinglink)
        time.sleep(3) # Add this delay to slow down the rate of requests sent to EDGAR database. EDGAR imposes a 10 requests per second cap and would block the senders who exceed this cap
        soup = BeautifulSoup(filingpage)
        filingtable = soup.find('table', attrs={'class' : "tableFile"})
        lines = filingtable.findAll('tr')

        ##the filing page contains a table in which there are several line items
        ##The first line contains the link to the Schedule 13 html file that we'll scrape
        ##In the future maybe I can check for the right line item that contains the link to SC 13
        processedfiling = False
        if (len(lines) >= 1 ):

            ##Add May 14, 2015, to improve efficiency. 
            ##check against scrapedData to see if the link is already scraped, if so
            ##return the existing data
            ##This can poentially replace the code added on May 13 for preview
            ##This is also useful now that the script stores partial scraping results and a complete scan becomes more useful and relevant
            ## in which case skipping pages already scraped can boost efficiency
            datalink = 'http://www.sec.gov'+lines[1].findAll('a')[0]['href']
            processedfiling = False
            resultLine = None
            if datalink:
                for lineIdx, scrapedLine in enumerate(scrapedData):
                    if (datalink == scrapedLine[7]):
                        processedfiling = True
                        resultLine = scrapedLine
                        break; 
                        #return(scrapedLine)

            ## end of Add May 14, 2015

            ## July 24, 2015:
            ## The part of scanning for fileby and subjectName is modified to remove a bug
            ## Try to recover fileby and subjectName in existing filings
            if processedfiling:
                if (resultLine[0] !='') and (resultLine[1] !='') and (resultLine[5]!='') and (resultLine[5]!='-1')  and (resultLine[6]!='') and (resultLine[6]!='-1'):
                    return(resultLine)
                

            tmp = soup.findAll('div', attrs={'id' : 'contentDiv'})
            filingAcceptedDt = ''
            if (tmp):
                formDiv = tmp[0].findAll('div', attrs={'class' : 'formContent'})
                if (formDiv):
                    filingDtsForm = formDiv[0].findAll('div', attrs={'class' : 'formGrouping'})
                    if (filingDtsForm):
                        filingDtsHeader = filingDtsForm[0].findAll('div', attrs={'class' : 'infoHead'})
                        filingDtsInfo = filingDtsForm[0].findAll('div', attrs={'class' : 'info'})
                        if (re.search('Accepted', filingDtsHeader[1].get_text())):
                            filingAcceptedDt = filingDtsInfo[1].get_text()
            compNames = soup.findAll('span', attrs={'class' : 'companyName'})
            fileby = ''
            subjectName = ''
            shareNum = ''
            sharePercent = ''
            for compName in compNames:
                nameStr = compName.get_text()
                if re.search('\(\s*Filed\s+by', nameStr, re.I):
                    m = re.search('\(\s*Filed\s+by', nameStr, re.I)
                    tmpidx = m.start(0)
                    fileby = nameStr[:tmpidx]
                    fileby = CleanString(fileby)
                    
                if re.search('\(\s*Subject', nameStr, re.I):
                    m = re.search('\(\s*Subject', nameStr, re.I)
                    tmpidx = m.start(0)
                    subjectName = nameStr[:tmpidx]
                    subjectName = CleanString(subjectName)

            if not processedfiling:
                scrapedtuple = ScrapeFilingData('http://www.sec.gov'+lines[1].findAll('a')[0]['href'],fundname)

                if (scrapedtuple):
                    ##Add on June 8th, pass on IssuerName from scrapedtuple if the subjectname is empty
                    ##The added function that scrapes txt filings will return the issuer's name
                    ##When scraping html filings, still skipping scanning for IssuerName for html filings 
                    if subjectName == '':
                        subjectName = scrapedtuple[0]
                    return((fileby, subjectName, filingAcceptedDt) + scrapedtuple[1:])
                else:
                    return scrapedtuple
            else:
                ##July 24, 2015
                ## Try to add fileby, subjectName and filing dt after modified code above
                ## Use existing ones which might be from manual checks
                if resultLine[0] !='':
                    fileby = resultLine[0]
                if resultLine[1] !='':
                    subjectName = resultLine[1]
                if resultLine[2] !='':
                    filingAcceptedDt = resultLine[2]

                

                if ((resultLine[5]=='') or (resultLine[5]=='-1')  or (resultLine[6]=='') or (resultLine[6]=='-1')):
                    scrapedtuple = ScrapeFilingData('http://www.sec.gov'+lines[1].findAll('a')[0]['href'],fundname)
                    if scrapedtuple:

                        if (scrapedtuple[3] =='-1') and (scrapedtuple[4] == '-1'):
                            return((fileby, subjectName, filingAcceptedDt) + resultLine[3:])
                        #
                        if (resultLine[5] !='') and (resultLine[5]!='-1'):
                            shareNum = resultLine[5]
                        else:
                            shareNum = scrapedtuple[3]

                        if (resultLine[6] !='') and (resultLine[6]!='-1'):
                            sharePercent=resultLine[6]
                        else:
                            sharePercent = scrapedtuple[4]

                        #print((fileby, subjectName, filingAcceptedDt) + resultLine[3:5] + (str(shareNum), str(sharePercent), resultLine[7]))

                        return((fileby, subjectName, filingAcceptedDt) + resultLine[3:5] + (str(shareNum), str(sharePercent), resultLine[7]))
                    else:
                        return((fileby, subjectName, filingAcceptedDt) + resultLine[3:])
                        
                else:
                    return((fileby, subjectName, filingAcceptedDt) + resultLine[3:])
        else:
            return None
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return None
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
        return None
    except Exception as e:
        print('Error occured when scraping for ' + fundname)
        print(e)
        logging.warning(e)
        return None
        

def ScrapeOneFund(fundname, CIK, CompleteScan=False):
    #global filescrapedok, filescraped, fundstat
    #filescrapedok = 0
    #filescraped=0
    logging.warning(fundname + ' '+ format(datetime.datetime.now(), '%Y-%m-%d-%H-%M-%S'))
    #load data to scrapedData
    outputfile = outputpath + 'SC13Forms/' +fundname +'.csv'
    if os.path.isfile(outputfile):
        with open(outputfile, encoding='utf-8') as f:
            scrapedData=[tuple(line) for line in csv.reader(f)]
    else:
        scrapedData = []
    newData = []

    fundurl = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+CIK+'&type=SC+13&owner=exclude&count=100'
    fundurlCtr = 0
    #print(fundurl)
    while True:
        try:
            fundpage = urlopen(fundurl, timeout = 100)
            #print(fundname)
            time.sleep(2)
            soup = BeautifulSoup(fundpage)
            #print(soup)
            filingstable = soup.find('table', attrs={'class':"tableFile2"})
            processedfiling=False
            print(fundname)

            if filingstable is None:
                break;

            if filingstable.findAll('tr') is None:
                break;

            ## Add on May 13, 2015
            ## Before this modification, the script will always scrape and analyze the first SC13 filing
            ## It will return the scraped data together with the link
            ## At which point the link will be checked against scrapedData
            ## If found, the scraped data is discarded and scraping for the current fund is skipped
            ## The code below will take a sneak preview of the first link without actually scrape it
            ##  hence improves the efficiency of the code
            if ((len(scrapedData) > 0) and (not CompleteScan)):
                filingLink = None
                processedfiling = False
                for onefiling in filingstable.findAll('tr'):
                    cells = onefiling.findAll('td')
                    if (len(cells) > 0 ):
                        if (cells[0].contents[0].find('SC 13') == 0):
                            filingLink = 'http://www.sec.gov'+ cells[1].findAll('a')[0]['href']
                            if filingLink:
                                break;

                ## There is not one link to follow for scraping, break altogether
                if filingLink is None:
                    break;

                previewpage = urlopen(filingLink)
                time.sleep(1)
                previewsoup = BeautifulSoup(previewpage)
                previewtable = previewsoup.find('table', attrs={'class' : "tableFile"})
                previewlines = previewtable.findAll('tr')
                if (len(previewlines) >= 1):
                    previewurl = 'http://www.sec.gov'+previewlines[1].findAll('a')[0]['href']

                for lineIdx, scrapedLine in enumerate(scrapedData):
                    if (previewurl == scrapedLine[7]):
                        processedfiling = True
                        break;

                if (processedfiling):
                    break;
            ## end of Add on May 13, 2015    

            for onefiling in filingstable.findAll('tr'):
                cells = onefiling.findAll('td')
                processedfiling=False
                if (len(cells) > 0 ):
                    if (cells[0].contents[0].find('SC 13') == 0):
                        filingLink = 'http://www.sec.gov'+ cells[1].findAll('a')[0]['href']
                        tmpScrapeData = ScrapeOneFiling(fundname,filingLink, scrapedData)
                        time.sleep(1)

                        if (tmpScrapeData):
                            if (len(scrapedData) > 0):
                                processedfiling = False;

                                for lineIdx, scrapedLine in enumerate(scrapedData):
                                    if (tmpScrapeData == scrapedLine):
                                        processedfiling = True;
                                        break;
                                    elif (tmpScrapeData[7] == scrapedLine[7]):

                                        #July 24, 2015
                                        #Modified the code for better retrieving fileby and subjectName
                                        #Try to add the new data back to the existing file
                                        if ( ((scrapedLine[0] =='') and (tmpScrapeData[0] !=''))
                                             or ((scrapedLine[1] =='') and (tmpScrapeData[1] !=''))
                                             or  (((scrapedLine[5] =='-1') or (scrapedLine[5] =='') or (scrapedLine[5] ==' ')) and ((tmpScrapeData[5] !='-1') and (tmpScrapeData[5] !='')))
                                             or  (((scrapedLine[6] =='-1') or (scrapedLine[6] =='') or (scrapedLine[6] ==' ')) and ((tmpScrapeData[6] !='-1') and (tmpScrapeData[6] !='')))):
                                            #remove scrapedLine from scrapedData
                                            del scrapedData[lineIdx]
                                            processedfiling = False
                                        else:
                                            processedfiling = True;
                                        break; 
                                        
                                if processedfiling:
                                    ##print(fundname + ' filing at ' + tmpScrapeData[7] + ' already processed.')
                                    if not CompleteScan:
                                        break
                                else:
                                    newData += [tmpScrapeData]
                            else:
                                newData += [tmpScrapeData]
                ## add on May 13, 2015
                ## Save the data once when 100 new lines are scraped
                ## This can be helpful when we first scrape for thousands of funds
                ## A partially saved result for particularly large funds can prevent multiple instances
                ## of the same python script get hang on the same fund
                if (len(newData) == 100):
                    if (len(scrapedData)>0):
                        newData +=scrapedData
                    DumpTable(newData, outputfile)
                    scrapedData = newData
                    newData = []
                    
            if processedfiling:
                if not CompleteScan:
                    break;

            fundurlCtr = fundurlCtr + 1 
            nextButton = soup.find('input', attrs={'value' : 'Next 100'})
            if nextButton is None:
                break;
            else:
                fundurlStart = fundurlCtr*100
                fundurl = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+CIK+'&type=SC+13&owner=exclude&start='+str(fundurlStart)+'&count=100'
                #print(fundurl)

        except HTTPError as e:
            print('The server couldn\'t fulfill the request for ' + fundname)
            print('Error code: ', e.code)
            break
        except URLError as e:
            print('We failed to reach a server for ' + fundname)
            print('Reason: ', e.reason)
            break
        except Exception as e:
            print('Error occured when scraping for ' + fundname)
            print(e)
            logging.warning(e)
            break
    #print(fundname)
    if (len(newData)>0):
        if (len(scrapedData)>0):
            newData +=scrapedData
        DumpTable(newData, outputfile)
    #fundstat += [fundname, filescraped, filescrapedok]
    
    #print(fundname)
    return
        

CIKfile = '/Users/Shared/Models/fullCIKs.csv'
CorpCIKfile = '/Users/Shared/Models/CorpCIKs.csv'
skipCIKfile = '/Users/Shared/Models/skipCIKs.csv'
coreCIKfile = '/Users/Shared/Models/CIKs.csv'

def AmendTxtFilingOneFund(fundname):
    fundfile =  '/Users/Shared/SC13Forms/'+fundname+'.csv'
    try:
        with open(fundfile, mode='r', encoding='utf-8') as infile:
            fundData = [tuple(line) for line in csv.reader(infile)]
    except Exception as e:
        return None

    newData = []
    fundModified = False
    for line in fundData:
        curURL = line[7]
        lineModified = False
        if curURL.find(".txt"):
            curCUSIP = line[3]
            curIssuer = line[1]
            curEventDt = line[4]
            curShareNum = line[5]
            curSharePercent = line[6]

            if ((curCUSIP =='') or (curIssuer=='') or (curEventDt =='') or (curShareNum =='') or (curShareNum =='-1') or (curSharePercent =='') or (curSharePercent =='-1')):
                resTuple = ScrapeTxtFilingData(curURL, fundname)

                if resTuple is None:
                    continue

                if ((curIssuer == '') and (resTuple[0] != '')):
                    curIssuer=resTuple[0]
                    lineModified = True

                if ((curCUSIP == '') and (resTuple[1] != '')):
                    curCUSIP=resTuple[1]
                    lineModified = True
               
                if ((curEventDt == '') and (resTuple[2] != '')):
                    curEventDt=resTuple[2]
                    lineModified = True

                if (((curShareNum =='') or (curShareNum =='-1')) and (resTuple[3] > -1)):
                    curShareNum = resTuple[3]
                    lineModified = True

                if (((curSharePercent =='') or (curSharePercent =='-1')) and (resTuple[4] > -1)):
                    curSharePercent = resTuple[4]
                    lineModified = True

                if lineModified:
                    newData += [(line[0], curIssuer, line[2], curCUSIP,curEventDt, curShareNum,curSharePercent, line[7])]
                    fundModified = True
                else:
                    newData += [line]
            else:
                newData += [line]
        else: # if curURL.find(".txt"):
            newData += [line]

    if (fundModified):
        DumpTable(newData, fundfile)
    

#AmendTxtFilingOneFund('BlackRock')
#AmendTxtFilingOneFund('GSAM')

def AmendAllTxtFilings():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
    FinalFunds = list(set([line[0] for line in TargetFunds]))

    for fund in FinalFunds:
        AmendTxtFilingOneFund(fund)
        with open('/Users/Shared/SC13Forms/@log/subprocess.txt', 'a') as f:
            f.write(fund)
            f.write('\n')


def AmendAllFilings(completescan=False):
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
    TargetFunds = list(set(TargetFunds))
    for fund in TargetFunds:
        ScrapeOneFund(fund[0], fund[1],completescan)


def AmendListedFilings(funds, completescan=False, stalefileOnly=False):
    for fund in funds:
        #print(fund)
        try:
            ScrapeOneFund(fund[0], fund[1], completescan)
        except Exception as e:
            print(e)
        #print(fund)
        #print(funds)
#        if stalefileOnly:
#            print('here')
#            fundfile = '/Users/Shared/SC13Forms/' + fund[0] + '.csv'
#            if FileMissingorStale(fundfile):
#                ScrapeOneFund(fund[0], fund[1], completescan)
#        else:
#            print('here')
#            ScrapeOneFund(fund[0], fund[1], completescan)
#            print('here2')
    return


def AmendListParallel(TargetFunds, NumOfThreads=4,  completescan=False, stalefileOnly=False, waittime=-1):

    if not TargetFunds:
        return None

    TargetFunds = list(set(TargetFunds))
    if (NumOfThreads <1):
        NumOfThreads = 2
    if (NumOfThreads > 8):
        NumOfThreads = 8
    threads = [None] * NumOfThreads
    fundListLen = len(TargetFunds) // NumOfThreads
    
    #print('here')

    for i in range(len(threads)):
        startIdx = i * fundListLen
        if (i == (len(threads) - 1)):
            endIdx = len(TargetFunds)-1
        else:
            endIdx = (i+1) * fundListLen-1
        threads[i] = multiprocessing.Process(target=AmendListedFilings, args=(TargetFunds[startIdx:(endIdx+1)], completescan, stalefileOnly))
        threads[i].start()

# a positive wait time will allow the calling process to proceed after the specified time
# a negative wait time (default) will block the process until all parallel prcesses finish
    if (waittime>0):
        for i in range(len(threads)):
            threads[i].join(waittime)
    else:
        for i in range(len(threads)):
            threads[i].join()
#        for i in range(len(threads)):
#            if threads[i].is_alive():
#                threads[i].terminate()

    return(threads)


#with open(CIKfile, 'r') as csvfile:
#    TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
#AmendAllFundParallel(TargetFunds, 16, True, True)

#AmendAllTxtFilings()
#AmendAllFilings()        
#ScrapeOneFund('SACCAPITALADVISORSLLC','0001018103', True)
#ScrapeOneFund('MAVERICKCAPITALLTDADV','0000928617')

while True:
    start_time = time.time()

    logfilename = outputpath+ 'SC13Forms/@log/' +  format(datetime.datetime.now(), '%Y%m%d')+'.txt'
    logging.basicConfig(filename=logfilename,level=logging.DEBUG)

    with open(CorpCIKfile, 'r') as csvfile:
        CorpFunds = [tuple(line) for line in csv.reader(csvfile)]
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
    if os.path.isfile(skipCIKfile):
        with open(skipCIKfile, 'r') as csvfile:
            SkipCIKs = [tuple(line) for line in csv.reader(csvfile)]
    else:
        SkipCIKs = []
    with open(coreCIKfile, 'r') as csvfile:
        coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
    coreTargetFunds = sorted(list(set(coreTargetFunds) - set(SkipCIKs)))
    
    #print(len(coreTargetFunds))

    AmendListParallel(coreTargetFunds,4,  False, False)
    AmendListParallel(CorpFunds, 4, False, False)

    #for fund in [line[0] for line in TargetFunds]:
    #    CUSIPUpper(fund)

    TargetFunds = list(set(TargetFunds) | set(SkipCIKs) | set(CorpFunds) | set(coreTargetFunds))
    print('Scraping time %s seconds ......' %(time.time()-start_time))
    start_time=time.time()
    
    CleanCUSIPviaMapping()
    print('Clean CUSIP time %s seconds ......' %(time.time()-start_time))
    start_time=time.time()

    FillCUSIPbySC13Data()
    print('Fill CUSIP time %s seconds ......' %(time.time()-start_time))
    start_time=time.time()
##
    FillCUSIPbyQtrFiling()
    print('Cross check 13F time time %s seconds ......' %(time.time()-start_time))
    start_time=time.time()

    CleanAllWrongShareNumfromCUSIP()
    print('Clean wrong ShareNum mistaken from CUSIPs time %s seconds ......' %(time.time()-start_time))
    start_time=time.time()
    backgroundProcesses=None

    try:
        subprocess.call(outputpath + 'Models/Python/RZRQ.py', shell=True)
        if datetime.datetime.now(timezone('EST')).weekday()>=5:
            for fund in [line[0] for line in TargetFunds]:
                CUSIPUpper(fund)
            backgroundProcesses = AmendListParallel(TargetFunds, 2, False, False, 10)
            time.sleep(Time2NextUSBusinessDay())
            if backgroundProcesses:
                for p in backgroundProcesses:
                    try:
                        if p.is_alive():
                            p.terminate()
                            print('terminating a process......')
                    except Exception as e:
                        print(e)
    except Exception as e:
        print(e)

    break


##
##dataurl = 'http://www.sec.gov/Archives/edgar/data/65100/'
##print(dataurl.endswith('/'))
##dataurl = 'http://www.sec.gov/Archives/edgar/data/1022596/000119312508031360/dsc13ga.htm'
##print(dataurl.endswith('/'))