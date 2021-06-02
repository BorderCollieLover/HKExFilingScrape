#!/Users/mintang/anaconda3/envs/Legacy/bin/python

import datetime
from dateutil import parser
import csv
import os
from pytz import timezone
import locale
from glob import glob
from operator import itemgetter
#from bdateutil import relativedelta
#import holidays
import math


##Fund2QtrlyDict maps fund name to related 13F holding. Some funds file SC 13 forms and 13F in two entities
##Should double check if the data is consistent in SC13 and 13F reportings
##September 5, 2016: expand the mapping to more than one related entities so as to consolidate holdings, e.g. Point72 and its subsidiaries
## this will make the filings more consistant with the SC13 data
Fund2QtrlyDict = {'GSAM': ['GOLDMANSACHSGROUPINC'],
                  'KCGAMERICASLLC': ['KCGHoldingsInc'],
                  'SagardCapitalPartnersLP': ['SagardCapitalPartnersManagementCorp'],
                  'Point72': ['Point72', 'EverPoint', 'RubricCapitalManagementLLC', 'CubistSystematicStrategiesLLC', 'CRIntrinsicInvestorsLLC', 'SIGMACAPITALMANAGEMENTLLC'],
                  'JOHAMBROCAPITALMANAGEMENTLTD': ['Pendal']
    }


##The two lists below identify funds whose SC13 data and 13f data is consistent versus those whose data is inconsistent
## A relative simple entity, e.g. with only one reporting entity, will likely have consistent data.
## A more complex entity can have inconsistent data when 13F reports only the holdings of certain sub entity.
## Keep both lists as funds are examined manually over time. 
##Allow 13F data to be merged together with SC13 data 
Allow13F = [ 'GSAM',
             'AdageCapital',
             'Wellington',
             'GreenWoods'
             ]


##Disallow 13F data to be merged with SC13 data
Disallow13F = [ 'BlackRock',
                'OZMANAGEMENTLP',
                'TemasekHoldingsPrivateLtd'
                ]

##OpenPosition Monitor Exclude -- exclude big mutual funds which tend to flood the candidate pool
##It's not that those filings are not valuable, just that at the moment I cannot handle it
MonitorUpdateExclude = [ 'BlackRock',
                         'Royce',
                         'ORBIMEDADVISORSLLC'
                         ]


fileCUSIPTicker = '/Users/Shared/Models/CUSIPTicker.csv'
try:
    with open(fileCUSIPTicker, mode='rt') as csvfile:
        dictCUSIPTicker = dict(csv.reader(csvfile))
except Exception as e:
    print(e)
    dictCUSIPTicker = dict()


def CreateDirectory(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)


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
    

def DumpTable(onetable,filename):
    if (onetable is None):
        return None
    
    if (len(onetable) <=0 ):
        print('No data')
    else:
        outputpath = os.path.dirname(os.path.realpath(filename))
        CreateDirectory(outputpath)
        if not CompareData2CSV(onetable, filename):
            with open(filename, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(onetable)

def TickerLookup(CUSIP):
    if (not dictCUSIPTicker):
        print('CUSIP Ticker file missing.')
        return (None)

    if (CUSIP in dictCUSIPTicker):
        return ([(dictCUSIPTicker[CUSIP], CUSIP, str(datetime.datetime.now()))])
    else:
        print('CUSIP not found: ' + CUSIP)
        return (None)


########################################################################################################################
########################################################################################################################
######## 1. General Holding Analysis Functionalities
########################################################################################################################
########################################################################################################################


##Holding info from one 13F-HR filing
##October 7, 2015
##Add the showClosed parameter, which is used to generate a holding history for backtesting
##If showClosed is True, and when the CUSIP is not found in the given file (via filename)
## it is assumed that any position in that CUSIP, if exists, is closed during the quarter
##In this case, we return a 0 in share count, return the positionDt (extracted from the filename) and
## return a reporting dt as the 15th of the month following the positionDt
def Holdingfrom13F(CUSIP, filename, fundname, showClosed=True):
    try:
        positionDt = os.path.splitext(os.path.basename(filename))[0]
        positionDt = datetime.datetime.strptime(positionDt, '%Y%m%d')
    except Exception as e:
        print(e)
        return(None)

    if positionDt.month not in [3,6,9,12]:
        return None

    try:
        with open(filename, 'r',encoding='utf-8') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)
        return None

    #positionDt = os.path.splitext(os.path.basename(filename))[0]
    #positionDt = datetime.datetime.strptime(positionDt, '%Y%m%d')

    matchedData = [line for line in fundData if line[2] == CUSIP]
    if ((matchedData is None) or (len(matchedData)==0)):
        #print('here')
        if (not showClosed):
            return None
        else:
            retData = []
            filingDt = positionDt + datetime.timedelta(days=45)
            #filingDt = filingDt + relativedelta(bdays=-1, holidays=holidays.US())
            #filingDt = filingDt + relativedelta(bdays=1, holidays=holidays.US())
            retData +=[(fundname,'',CUSIP,  str(0), 'SH','', str(-1), str(positionDt), str(filingDt), '13F')]
            return(retData)
    else:
        retData = []
        for line in matchedData:
            retData +=[(fundname, line[0], line[2], line[4], line[5], line[6],str(-1), str(positionDt), line[len(line)-1], '13F')]

    return(retData)
##end of October 7, 2015 modification

##x = Holdingfrom13F('208242107', '/Users/shared/Fund Clone/Greenlight/20150630.csv', 'Greenlight', showClosed=True)
##print(x)

##Holding history from all available 13F-HR filings
#September 6, 2016: set default showClosed to True, in fact it will always be true as it won't be changed when this function is called
def HoldingHistory13F(CUSIP, fund, doCall=False, doPut=False):
    holding13F = []

    if fund in Fund2QtrlyDict:
        fundlist = Fund2QtrlyDict[fund]
    else:
        fundlist = [fund]

    for fundname in fundlist:
        filingpath = '/Users/shared/fund clone/'+fundname
        if os.path.exists(filingpath):
            QtrlyFilings = glob(filingpath + '/*.csv')
        else:
            continue

        if QtrlyFilings is None:
            continue

        for oneFiling in QtrlyFilings:
            holdingInfo = Holdingfrom13F(CUSIP, oneFiling, fund)
            if holdingInfo is None:
                continue
            else:
                holding13F += holdingInfo

    #if all the 13F entries are 0, maybe the CUSIP is missing from 13F scraping
    #discard the 13F history populated by 0
    if holding13F and (len(holding13F)>1):
        try:
            allShareNums = list(set([locale.atof(line[3]) for line in holding13F]))
        except Exception as e:
            print(e)
            print(CUSIP)
            print(fund)
            return(None)
        sumShareNum = sum(allShareNums)
        if (sumShareNum < 1):
            return(None)

    #if holding13F data is not empty and length of fundlist is greater than 1, consolidate holdings from different 13Fs
    #print(holding13F)
    if holding13F and (len(fundlist)>1):
        holding13F = Consolidate13FHoldings(holding13F, doCall, doPut)

    if holding13F:
        holding13F = sorted(holding13F, key=itemgetter(7,5))
    return holding13F

#September 5, 2016
#When several related entities file 13F holdings separately, e.g. Point72
# use this process to consolidate holdings from multiple 13Fs so that the 13F filings is consistent with SC13 FILINGS
# If set to True, doCall and doPut will further trigger consolidating calls and puts to SHs by increasing/decreasing share count
def Consolidate13FHoldings(holding13F, doCall=False, doPut=False):
    if not holding13F:
        return(None)

    allDts = list(set([line[7] for line in holding13F]))
    for curDt in allDts:
        #do PRNs
        HoldingOneDt = [tuple(line) for line in holding13F if (line[7] == curDt) and (line[4] == 'PRN')]
        if (len(HoldingOneDt) > 1):
            holding13F= ConsolidateReplace(HoldingOneDt, holding13F)

        #do SHs
        HoldingOneDt = [tuple(line) for line in holding13F if (line[7] == curDt) and (line[4] == 'SH') and (line[5] =='')]
        if (len(HoldingOneDt) > 1):
            holding13F = ConsolidateReplace(HoldingOneDt, holding13F)

        #do Calls
        HoldingOneDt = [tuple(line) for line in holding13F if (line[7] == curDt) and (line[4] == 'SH') and (line[5] =='Call')]
        if (len(HoldingOneDt) > 1):
            holding13F = ConsolidateReplace(HoldingOneDt, holding13F)

        #do Puts
        HoldingOneDt = [tuple(line) for line in holding13F if (line[7] == curDt) and (line[4] == 'SH') and (line[5] == 'Put')]
        if (len(HoldingOneDt) > 1):
            holding13F = ConsolidateReplace(HoldingOneDt, holding13F)

        #By now for each date, each instrument (RPN, SH, Call, Put) should have at most one line entry in the holding13F data
        #If to combine call/puts to shares counts, do only when there is a SH line and an option line
        if doCall:
            HoldingOneDt = [tuple(line) for line in holding13F if
                            (line[7] == curDt) and (line[4] == 'SH') and ((line[5] == 'Call') or (line[5] ==''))]
            if (len(HoldingOneDt) == 2):
                holding13F = ConsolidateReplaceOption(HoldingOneDt, holding13F)

        if doPut:
            HoldingOneDt = [tuple(line) for line in holding13F if
                            (line[7] == curDt) and (line[4] == 'SH') and ((line[5] == 'Put') or (line[5] == ''))]
            if (len(HoldingOneDt) == 2):
                holding13F = ConsolidateReplaceOption(HoldingOneDt, holding13F)

    #holding13F = sorted(fundhistory, key=itemgetter(8, 7))
    return(holding13F)


def ConsolidateReplace(tgtData, srcData):
    srcData = list(set(srcData) - set(tgtData))
    allShareNums = [locale.atof(line[3]) for line in tgtData]
    totalShares = sum(allShareNums)
    tmpLine = list(tgtData[0])
    tmpLine[3] = str(totalShares)

    srcData += [tuple(tmpLine)]
    return(srcData)

#this is similar to ConsolidateReplace but with one line entry for SH and the other line entry that of an option
def ConsolidateReplaceOption(tgtData, srcData):
    srcData = list(set(srcData) - set(tgtData))
    tgtData = sorted(tgtData, key=itemgetter(5))
    #print(tgtData)
    #tmpLine = list(tgtData[0])
    #print(tmpLine[3])
    #print(tgtData[1][3])
    if (tgtData[1][5] == 'Call'):
        newShareNum = locale.atof(tgtData[0][3]) + locale.atof(tgtData[1][3])
    else:
        newShareNum = locale.atof(tgtData[0][3])  - locale.atof(tgtData[1][3])
    #print(newShareNum)
    tmpLine = [tgtData[0][0], tgtData[0][1],tgtData[0][2], str(newShareNum),   tgtData[0][4],tgtData[0][5],tgtData[0][6],tgtData[0][7],tgtData[0][8],tgtData[0][9]]
    #print(tmpLine)
    srcData += [tuple(tmpLine)]
    return(srcData)

#x = HoldingHistory13F('91911K102', 'Point72')
#print(x)
#x = HoldingHistory13F('91911K102', 'Point72', doCall=True)
#print(x)

## Added March 5, 2016
## Remove repeated SC13 data filed on the same day
## Not sure what's the reason behind those duplicated filings but they exist
## e.g. GreenWoods filed two SC13 on Jumei on Feb 24, 2016
## The input is the SC13 holding history of ONE UNDERLYING IN ONE FUND
def SameDayDuplicatedSC13(fundhistory):
    if (not fundhistory):
        return None

    if (len(fundhistory) <= 1 ):
        return fundhistory

    newfundhistory = []
    for idx, line in enumerate(fundhistory):
        if (idx == 0):
            newfundhistory += [line]
        else:
            if (parser.parse(line[7]).date() == parser.parse(fundhistory[idx-1][7]).date()):
                if  (not (fundhistory[idx][3] == line[3])):
                    newfundhistory +=[line]
            else:
                newfundhistory +=[line]
    return(newfundhistory)

## End added march 5, 2016

def HoldingHistorySC13(CUSIP, fundname):
    filename = '/Users/shared/SC13Forms/'+fundname +'.csv'
    if os.path.exists(filename):
        try:
            with open(filename, 'r',encoding='utf-8') as csvfile:
                fundData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            print(e)
            return None
    else:
        return None

    matchedData = [line for line in fundData if line[3] == CUSIP]
    if matchedData is None:
        return None

    retData = []
    for line in matchedData:
        if (line[4]==''):
           positionDt = line[2]
        else:
            try:
                positionDt = parser.parse(line[4])
            except Exception as e:
                positionDt = line[2]

        retData +=[(fundname, line[1], line[3], line[5], '','', line[6], str(positionDt) , line[2], 'SC13')]

    return(SameDayDuplicatedSC13(retData))


def RemoveSpuriousHoldings(fundhistory, fromBeginning=True):
    if fundhistory:
        fundhistory = sorted(fundhistory, key=itemgetter(7,8))
        if (len(fundhistory) == 1):
            if (fundhistory[0][3] == '0') or (fundhistory[0][3] == '0.0'):
                return None
        else:
            shareNums = list(set([line[3] for line in fundhistory]) - {'0', '0.0'})
            if (shareNums is None) or  (len(shareNums)<1):
                return None
            if fromBeginning:
                if (fundhistory[0][3]=='0'):
                    fundhistory = fundhistory[1:]
                    fundhistory = RemoveSpuriousHoldings(fundhistory)
            else:
                if ((fundhistory[len(fundhistory)-1][3] == '0') or (fundhistory[len(fundhistory)-1][3] == '0.0')) and ((fundhistory[len(fundhistory)-2][3] == '0') or (fundhistory[len(fundhistory)-2][3] == '0.0')) and (fundhistory[len(fundhistory)-1][9] == fundhistory[len(fundhistory)-2][9]):
                    fundhistory = fundhistory[:(len(fundhistory)-1)]
                    fundhistory = RemoveSpuriousHoldings(fundhistory, False)

    return(fundhistory)

# Segment holding history into continuous holding segments
# Refer notes in Min's Notebook ==> Screen Process for  logic
# September 6 2016: add a new parameter, i.e. the date of the first available 13F filing
# this is because when there are two consecutive SC13 filings the code initially checks only if they are within a year
# but if there are 13F filings in between but contain no particular underlying, it is a sign that the holding history should be segmented
# This is tricky however because holding history can be generated without 13F data, either a) by setting the use13F tag or, b) by excluding the fund in DisAllow13F, and c) when there is indeed no 13F filings
# In these three cases the reference date should be set to NONE. However, the responsibility lies within the process that calls SegmentHoldingHistory and the function itself won't be able to distinguish the situations
# See notes for details and examples.
def SegmentHoldingHistory(fundhistory):
    if (not fundhistory):
        return (None)

    listofhistories = []

    if (len(fundhistory) <= 1):
        listofhistories.append(fundhistory[:])
    else:
        ##        fundhistory = sorted(fundhistory, key=itemgetter(7))
        curHistory = []
        curHistoryEnd = False
        lastDt = None
        lastType = None
        idx = 0
        for idx, line in enumerate(fundhistory):
            if (idx == 0):
                curHistory += [line]
                lastDt = parser.parse(line[7]).date()
                lastType = line[9]
                continue
            curType = line[9]
            curDt = parser.parse(line[7]).date()
            # there can be multiple filings on the same date, e.g. SC13 and 13F data on Dec 31; and multiple 13F entries because of options
            if (curDt == lastDt):
                curHistory += [line]
                lastDt = curDt
                lastType = curType
                continue

            if (lastType == 'SC13'):
                if (curType == 'SC13'):
                    if ((curDt - lastDt).days <= 369):  #
                        curHistory += [line]
                        lastDt = curDt
                        lastType = curType
                    else:
                        curHistoryEnd = True
                else:
                    if (curDt.year == lastDt.year):
                        if (math.ceil(curDt.month / 3.) == math.ceil(lastDt.month / 3.)):
                            curHistory += [line]
                            lastDt = curDt
                            lastType = curType
                        else:
                            curHistoryEnd = True
                    else:
                        if ((lastDt.month == 12) and (lastDt.day == 31) and (curDt.year - lastDt.year == 1) and (
                            curDt.month == 3)):
                            curHistory += [line]
                            lastDt = curDt
                            lastType = curType
                        else:
                            curHistoryEnd = True
            else:
                if (((curDt.year == lastDt.year) and (
                        math.ceil(curDt.month / 3.) - math.ceil(lastDt.month / 3.) == 1)) or (
                    (curDt.year - lastDt.year == 1) and (
                        math.ceil(curDt.month / 3.) - math.ceil(lastDt.month / 3.) == -3))):
                    curHistory += [line]
                    lastDt = curDt
                    lastType = curType
                else:
                    #September 16, 2016
                    #sometimes there can a 13F filing marked on the wrong month, e.g. 20090730 for Point72
                    # it is difficult to track all such wrong filings
                    # but allow the spurious filing to continue the holding history
                    if (curType =="13F"):
                        if (curDt.year == lastDt.year) and (
                                        math.ceil(curDt.month / 3.) - math.ceil(lastDt.month / 3.) == 0):
                            curHistory += [line]
                            lastDt = curDt
                            lastType = curType
                        else:
                            curHistoryEnd = True
                    else:
                        curHistoryEnd = True

            if curHistoryEnd:
                break

        listofhistories.append(curHistory[:])

        if (curHistoryEnd):
            listofhistories += SegmentHoldingHistory(fundhistory[idx:])


    #remove segments that all holdings numbers are 0:
    for i, curHistory in enumerate(listofhistories):
        shareNums = [line[3] for line in curHistory if (line[3]!='0') and (line[3]!='0.0')]
        if (shareNums is None) or (len(shareNums) <1):
            listofhistories.pop(i)


    return (listofhistories)


#This script extrapolates for a possible missing 13F filing, e.g. 63934E108, for Point72 for June 30, 2008
#if a 13F filing has a 0 shareNum, and a) it follows an SC13 filing in the same quarter with a 5% or higher holding; and b) it is followed by another filing in the following quarter
# and c) the holding in this folloiwing filing is equal to or higher than the preceeding sc13 filing
# THEN use the preceeding sc13 filing number to replace the 0 13F filing

def ExtrapolatePossibleMissing13Fentries(fundhistory):
    for idx, line in enumerate(fundhistory):
        if (idx == 0 ) or (idx == len(fundhistory)-1):
            continue
        if line[9]=='13F':
            shareNum = 0
            try:
                shareNum =  locale.atof(line[3])
            except Exception as e:
                print(e)
                continue
            if shareNum >0 :
                continue

            if fundhistory[idx-1][9] == 'SC13':
                sc13percent1 = fundhistory[idx-1][6]
                if (sc13percent1 != '') and (sc13percent1 != '-1'):
                    sc13percentcheck = -1
                    try:
                        sc13percentcheck = locale.atof(sc13percent1)
                    except Exception as e:
                        print(e)
                    if (sc13percentcheck) < 5:
                        continue
                else:
                    continue
            else:
                continue

            sc13date = fundhistory[idx-1][7]
            try:
                tmpdate = parser.parse(sc13date)
            except Exception as e:
                sc13date = fundhistory[idx - 1][8]
                tmpdate = parser.parse(sc13date)
            sc13date = tmpdate.date()

            f13date = parser.parse(line[7]).date()
            if (sc13date.year==f13date.year) and (math.ceil(sc13date.month / 3.) ==  math.ceil(f13date.month / 3.)):
                nextShareNum = 0
                try:
                    nextShareNum = locale.atof(fundhistory[idx+1][3])
                except Exception as e:
                    print(e)

                nextSharePercent = 0
                try:
                    nextSharePercent = locale.atof(fundhistory[idx + 1][6])
                except Exception as e:
                    print(e)

                if (nextShareNum <=0) and (nextSharePercent <=0):
                    continue

                nextdt = fundhistory[idx+1][7]
                try:
                    tmpdate = parser.parse(nextdt)
                except Exception as e:
                    nextdt = fundhistory[idx +1][8]
                    tmpdate = parser.parse(nextdt)
                nextdt = tmpdate.date()

                #if nextdt is within the following quarter, then extrapolate shareNum in line
                if ((nextdt.year == f13date.year) and (math.ceil(nextdt.month / 3.) - math.ceil(f13date.month / 3.) == 1)) or ((nextdt.year - f13date.year == 1) and (math.ceil(nextdt.month / 3.) - math.ceil(f13date.month / 3.) == -3)):
                    extrapolatedLine = tuple([line[0], line[1], line[2], fundhistory[idx-1][3], line[4], line[5], line[6], line[7], line[8], line[9]])
                    #print('Extrapolate share number for ', extrapolatedLine)
                    fundhistory[idx] = extrapolatedLine
    return(fundhistory)

## If two SC13 filings are less than a year  apart, and the first one reports holding >= 5%
## then if all the 13F holdings in between are 0, these are likely scraping errors and the 13F lines should be removed
def RemovePossibleMissing13Fentries(fundhistory):
    sc13lines = [tuple(line) for line in fundhistory if line[9]=='SC13']
    f13lines = [tuple(line) for line in fundhistory if line[9]=='13F']
    if (sc13lines is None ) or (len(sc13lines)<=1) or (f13lines is None) or (len(f13lines)<1):
        return(fundhistory)

    removed13flines = False

    for lineidx in range(len(sc13lines)-1):
        sc13line1 = sc13lines[lineidx]
        sc13line2 = sc13lines[lineidx+1]
        sc13date1 = sc13line1[7]
        sc13date2 = sc13line2[7]
        if ((parser.parse(sc13date2).date() - parser.parse(sc13date1).date()).days > 369):
            continue
        inbetweenF13lines = [tuple(line) for line in f13lines if (line[7]>=sc13date1) and (line[7]<=sc13date2)]
        if (inbetweenF13lines is None) or (len(inbetweenF13lines)<1):
            continue

        #Note: initially I consider holdings missing from 13F filings if ALL in-between 13f filings report 0 shares
        # but there are incidents such as 63934E108, for Point72,
        # SC13 filing indicates that 3530450 shares were bought on June 24, 2008 but the June 30, 2008 13f filings showed nothing
        # The September 30, 2008 13f filings shows a 3630600-share holding
        # So I remove all intermediate 13f holdings that show 0 shares if they are between two sc13 filings and the share count of the two sc13 filings indicates that 13f filings should have been reported
        # DUH! But missing 13F filings will be considered as exiting position anyway, by segmenthistory.
        # Besides, for 863236105 for Maverick, it is a legitimate assumption to consider the 0 shares on 20060930 an exit of position.

        allShareNums = [0]
        try:
            allShareNums = list(set([locale.atof(line[3]) for line in inbetweenF13lines]))
        except Exception as e:
            print(e)
            continue
        sumShareNum = sum(allShareNums)
        if (sumShareNum > 1):
            continue

        #there are one or more 13F lines between two sc13 lines that have only 0 share numbers
        #check if the first line has a percentage holding > 5%
        sc13percent1 = sc13line1[6]
        if (sc13percent1 !='') and (sc13percent1 != '-1'):
            sc13percentcheck = -1
            try:
                sc13percentcheck = locale.atof(sc13percent1)
            except Exception as e:
                print(e)
            if (sc13percentcheck) < 5:
                continue
        else:
            continue

        #check if the second line has a non-zero holding:
        sc13percent2 = sc13line2[6]
        sc13percentcheck = -1
        if (sc13percent2 != '') and (sc13percent2 != '-1'):
            sc13percentcheck = -1
            try:
                sc13percentcheck = locale.atof(sc13percent2)
            except Exception as e:
                print(e)
        sc13num2 = sc13line2[3]
        sc13num2check = -1
        if (sc13num2 != '') and (sc13num2 != '-1'):
            sc13num2check = -1
            try:
                sc13num2check = locale.atof(sc13percent2)
            except Exception as e:
                print(e)

        if (sc13percentcheck > 0) or (sc13num2check > 0):
            #print('discard the following 13f lines between')
            #print(sc13line1)
            #print(sc13line2)
            #print('13f lines:')
            #print(inbetweenF13lines)
            f13lines = list(set(f13lines) - set(inbetweenF13lines))
            removed13flines = True

        if (f13lines is None) or (len(f13lines)<1):
            break

    if removed13flines:
        if f13lines:
            fundhistory  = sc13lines + f13lines
            fundhistory = sorted(fundhistory, key=itemgetter(7, 8))
        else:
            fundhistory = sc13lines

    if (fundhistory is not None) and (len(fundhistory)>0):
        fundhistory = ExtrapolatePossibleMissing13Fentries(fundhistory)

    return(fundhistory)

#Mar 5, 2016:
# modify the HoldingHistory function:
#  a) Add a use13F parameter to indicate whether 13F data is merged with SC13 data to create holding history
#     When this flag is True, funds whose 13F data is inconsistent with SC13 data still won't have 13F data included
#  b) Remove the OnlyRecent parameter and move the code for keeping only recent data to a separate function
#  c) Add a separate function to segment holding history data if applicable 
def HoldingHistory(CUSIP, fundname, use13F=True, doCall=False, doPut=False):
    #print("here")
    holdingsSC13 = HoldingHistorySC13(CUSIP, fundname)
    if holdingsSC13:
        fundhistory = holdingsSC13
    else:
        fundhistory = None

    holdings13F = HoldingHistory13F(CUSIP, fundname, doCall, doPut)
    #print(holdings13F)

        #print(fundhistory)
    if fundhistory:
        if holdings13F:
            fundhistory += holdings13F
    else:
        if holdings13F:
            fundhistory = holdings13F


    ##October 7, 2015
    ##if showClosed is true
    ##then remove spurious data entries where a 0 shareNum is followed/preceded by another 0 shareNum
    if (fundhistory):
        #print(fundhistory)
        fundhistory = RemoveSpuriousHoldings(fundhistory, True)
        fundhistory = RemoveSpuriousHoldings(fundhistory, False)
    ## end October 7, 2015

    if (fundhistory is None) or (len(fundhistory) < 1):
        return(None)

    #September 13, 2016: despite the best efforts there can be instances when a 13F and a SC13 filing diverge
    #For example, when both are filed on year-end, but the holding is too small thus omitted from 13F filings,
    #e.g. G10082140 for Point72, for 2013-12-31
    #In this case, simply remove the 13F filings as there will be a place holder from the SC13 filing
    #print(fundhistory)
    ClosedPositionIdx = [i for i, x in enumerate(fundhistory) if ((x[3] == '0') or (x[3] == '0.0')) and x[9] == '13F']
    remove13Fidx = []
    for curIDX in ClosedPositionIdx:
        #print(curIDX)
        #print(fundhistory[curIDX])
        sc13Line = [tuple(line) for line in fundhistory if line[7] == fundhistory[curIDX][7] and line[9]=='SC13']
        if (sc13Line is None) or (len(sc13Line)<1):
            continue
        if (sc13Line[0][3] !='0') and (sc13Line[0][3] !='0.0'):
            #print(sc13Line)
            remove13Fidx += [curIDX]
    if len(remove13Fidx)>=1:
        remove13Flines = [tuple(x) for i, x in enumerate(fundhistory) if i in remove13Fidx]
        fundhistory = list(set(fundhistory) - set(remove13Flines))
        fundhistory = sorted(fundhistory, key=itemgetter(7, 8))

    ## If two SC13 filings are less than a year  apart, and the first one reports holding >= 5%
    ## then if all the 13F holdings in between are 0, these are likely scraping errors and the 13F lines should be removed
    fundhistory = RemovePossibleMissing13Fentries(fundhistory)


    # now break fund history down into a series of holding histories, using 0 share entries as separator
    #print(fundhistory)
    ClosedPositionIdx = [i for i, x in enumerate(fundhistory) if ((x[3]=='0') or (x[3]=='0.0')) and x[9]=='13F']
    #print(ClosedPositionIdx)

    if len(ClosedPositionIdx) < 1 :
        tmpHistories = [fundhistory]
    else:
        tmpHistories = []
        startPos = 0
        endPos = 0
        for endPos in ClosedPositionIdx:
            HistorySegment = fundhistory[startPos:(endPos+1)]
            HistorySegment = RemoveSpuriousHoldings(HistorySegment, True)
            HistorySegment = RemoveSpuriousHoldings(HistorySegment, False)

            if HistorySegment:
                tmpHistories += [HistorySegment]

            startPos = endPos + 1

        if endPos < (len(fundhistory) - 1):
            HistorySegment = fundhistory[(endPos + 1):]
            HistorySegment = RemoveSpuriousHoldings(HistorySegment, True)
            HistorySegment = RemoveSpuriousHoldings(HistorySegment, False)

            if HistorySegment:
                tmpHistories += [HistorySegment]

    #for i, x in enumerate(tmpHistories):
    #    print(i, x)

    #print(tmpHistories)
    outputList = []
    for curHistory in tmpHistories:
        curHistory = sorted(curHistory, key=itemgetter(7))
        segmentedList = SegmentHoldingHistory(curHistory)
        if segmentedList:
            outputList += segmentedList

    #for i, x in enumerate(outputList):
    #    print(i, x)

    if ( not use13F)  or (fundname in Disallow13F):
        finaloutputlist = []
        for i, curHistory in enumerate(outputList):
            curHistory = [tuple(line) for line in curHistory if ((line[9]=='SC13') or ((line[9]=='13F') and ((line[3]=='0') or (line[3]=='0.0'))))]
            if (curHistory is None) or (len(curHistory) < 1 ):
                continue
            else:
                finaloutputlist += [curHistory]

    #  remove non-0 13F  entries in the output
        outputList = finaloutputlist

    return (outputList)


#x = HoldingHistory('98978V103', 'Point72')
#print(x)
#x = HoldingHistory('23282W605', 'Point72', showClosed=True)
#print(x)
#x= HoldingHistory('00751Y106', 'BlackRock')
#print(x)

#Added Mar 5, 2016
#Was part of the HoldingHistory function 
def KeepOnlyRecent (fundhistory, winlen=140):
    #print(fundhistory)
    if fundhistory:
        #print(fundhistory)
        ##Added June 2, 2015
        ##Keep only data where the last holding history data  is more than 6 months old
        ## which leaves enough margin as we wait for the next quarterly report (3 mos + 1.5 mos)
        ##

        ##Modified Mar 8, 2016
        ##Before we use "Event Date" when possible
        ##Now we use "filing date" to decide whether the filing is recent
        ##Use filing date is more consistent across SC13 and 13F, and is more realistic for backtest
        
##        try:
##            LastPositionDt = parser.parse(fundhistory[len(fundhistory)-1][7])
##        except Exception as e:
##            LastPositionDt = parser.parse(fundhistory[len(fundhistory)-1][8])
        #lastPositionIdx = len(fundhistory)-1
        #while fundhistory[lastPositionIdx]

        #September 6, 2016: add a tmpFundHistory to include only non-0 holdings as I move to showClosed as default
        #print(fundhistory)
        tmpFundHistory = [tuple(line) for line in fundhistory if (line[3]!='0') and (line[3]!='0.0')]
        #print(tmpFundHistory)
        if (tmpFundHistory is None) or (len(tmpFundHistory) < 1 ):
            return None

        LastPositionDt = parser.parse(tmpFundHistory[len(tmpFundHistory)-1][8])
        ## End of Modified Mar 8, 2016

        tz = timezone("EST")
        LastPositionDt = tz.localize(LastPositionDt)
        currDt = datetime.datetime.now(timezone('EST'))
        deltaDays = abs((currDt - LastPositionDt).days)


        if (deltaDays > winlen):
            fundhistory = None

        #print(fundhistory)
        
        return(fundhistory)
    else:
        return None





########################################################################################################################
########################################################################################################################
######## 2. Holding history monitor for a particular underlying and a target set of funds
########################################################################################################################
########################################################################################################################

#check if a fund's SC13 and 13F filings (where applicable) are updated versus current OpenPosition summary
# the aim is to scan only filings that are updated for each ticker when updating open positions
# This helps improve efficiency,  when the holding analysis gets more involved and time consuming
# 
def FilingDataUpdated(tickerfile, fundname, use13F=True):
    # if sc13 file is newer, return True
    # else if use13F and fund is allowed, and latest quarterly data is newer, return True
    tickerfiletime = datetime.datetime.fromtimestamp( os.path.getmtime(tickerfile))

    sc13file = '/Users/shared/SC13Forms/'+fundname +'.csv'
    if os.path.isfile(sc13file):
        sc13time = datetime.datetime.fromtimestamp( os.path.getmtime(sc13file))
        if ((sc13time - tickerfiletime).total_seconds() >0):
            return True
        else:
            if (use13F and (fundname not in Disallow13F)):
                # check only one entity even if the fund is linked to multiple entities in 13F filings:
                if fundname in Fund2QtrlyDict:
                    fund13Fname = Fund2QtrlyDict[fundname][0]
                else:
                    fund13Fname = fundname
                if os.path.exists('/Users/shared/fund clone/'+ fund13Fname):
                    new13F = max(glob('/Users/shared/fund clone/'+ fund13Fname +'/*.csv'), key=os.path.getmtime)
                    if (new13F):
                        f13time = datetime.datetime.fromtimestamp( os.path.getmtime(new13F))
                        if ((f13time - tickerfiletime).total_seconds() >0):
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            else:
                return False


def UpdatedFundsforTicker(ticker, fundlist, use13F=True):
    # if openposition summary file doesn't exist, return fundlist
    # else return funds that have updated filings
    csvfile = '/Users/shared/SC13Monitor/OpenPosition/'+ticker+'.csv'
    if os.path.isfile(csvfile):
        newfundlist = []
        for fund in fundlist:
            if (FilingDataUpdated(csvfile, fund, use13F)):
                newfundlist += [fund]
        return(newfundlist)
    else:
        return(fundlist)
    

def HoldingAnalysis(CUSIP, fundlist, onlyRecent=True):
    retData = []
    if fundlist:
        for fund in fundlist:
    ##        print(fund)
            fundData = HoldingHistory(CUSIP, fund)
            #print(fundData)
            if fundData:
                if (onlyRecent):
                    fundHistory = KeepOnlyRecent(fundData[len(fundData)-1])
                else:
                    fundHistory = [ item for segment in fundData for item in segment]
            else:
                fundHistory = None
            if fundHistory:
                retData += fundHistory
    return(retData)

#x = HoldingAnalysis('71377G100', ['SagardCapitalPartnersLP', 'ICAHN'])
#print(x)

def OpenPositionsCleanup(tickers):
    for ticker in tickers:
        curTickerData = []
        try:
            with open('/Users/shared/SC13Monitor/OpenPosition/'+ticker+'.csv', encoding='utf-8') as f:
                curTickerData = [tuple(line) for line in csv.reader(f)]
        except Exception as e:
            curTickerData = []

        if curTickerData:
            curTickerData = sorted(list(set(curTickerData)), key=itemgetter(0,7))
            DumpTable(curTickerData, '/Users/shared/SC13Monitor/OpenPosition/'+ticker+'.csv')
    return None
    

def HoldingAnalysisOpenPositions(OpenPositions, coreFunds):
    tickers = sorted(list(set([line[0] for line in OpenPositions]) - {'SPY', 'QQQ', 'VXX', 'GLD', 'VXZ', 'IBB', 'TLT', 'IWM', 'SLV'}))
    #print(tickers)
    for ticker in tickers:
##        print(ticker)
        ## if one ticker is mapped to multiple CUSIPs, e.g. for certain ADRs
        updatedFunds = UpdatedFundsforTicker(ticker, coreFunds)
##        print(len(updatedFunds))
        # load current open position data
        #print(updatedFunds)
        if (updatedFunds):
            curTickerData = []
            try:
                with open('/Users/shared/SC13Monitor/OpenPosition/'+ticker+'.csv', encoding='utf-8') as f:
                    curTickerData = [tuple(line) for line in csv.reader(f)]
            except Exception as e:
                curTickerData = []

            # if there are existing data, and there are funds with updated filing
            #  remove data associated with these funds from curTickerData
            #print(len(curTickerData))
            if curTickerData:
                curTickerData = [line for line in curTickerData if line[0] not in updatedFunds]
            #print(len(curTickerData)

            tickerRows = [tuple(line) for line in OpenPositions if line[0] == ticker]
            tickerdata = []
            for line in tickerRows:
                #print(line[1])
                #print(updatedFunds)
                x=HoldingAnalysis(line[1], updatedFunds)
                #print(x)
                if x:
                    tickerdata += x
                #print(tickerdata)
            if tickerdata:
                tickerdata = sorted(list(set(tickerdata+curTickerData)), key=itemgetter(0,7))
            else:
                if curTickerData:
                    tickerdata = sorted(list(set(curTickerData)), key=itemgetter(0,7))
            #if there is data to save, save it
            #if not, i.e. there is no relevant and recent holdings of the underlying
            # we'll leave the existing data file unchanged
            # if the file is missing, updatedFunds will return all funds and we might waste time recalculating it
            # although the ticker should probably be teased out from the monitor list if it updated automatically
            if tickerdata:
                DumpTable(tickerdata, '/Users/shared/SC13Monitor/OpenPosition/'+ticker+'.csv')
    return None

#HoldingAnalysisOpenPositions()

##Analyze the holding history of a CUSIP from the fundname
## for interested funds in fundlist
def HoldingAnalysisfromOneFund(CUSIP, fundname, fundlist, onlyRecent=True, use13F=True):
    fundlist =set(fundlist)
    fundlist.discard(fundname)
    fundlist = sorted(list(fundlist))

    outputdir = '/Users/Shared/HoldingAnalysis/' + fundname
    outputfile = outputdir +'/'+CUSIP + '.csv'

    curFundHoldings = HoldingHistory(CUSIP, fundname, use13F)
    if curFundHoldings:
        if (onlyRecent):
            curFundHoldings = KeepOnlyRecent(curFundHoldings)
        if (curFundHoldings):
            historiesList = SegmentHoldingHistory(curFundHoldings)
            if historiesList:
                curFundHoldings = historiesList[len(historiesList)-1]

    if curFundHoldings is None:
        print(CUSIP + ' is not found in ' + fundname)
        return None
    else:
        tmpFundHoldings = HoldingAnalysis(CUSIP, fundlist, onlyRecent)
        if tmpFundHoldings:
            curFundHoldings += tmpFundHoldings

        DumpTable(curFundHoldings, outputfile)
        return curFundHoldings

    
## holding analysis of CUSIPs coming from a particular fund
def FundHoldingAnalysis(fundname, fundlist):
    #retrieve CUSIPs from filings filed after March 1, 2015
    #The date is arbitrary
    try:
        with open('/Users/shared/SC13Forms/'+fundname+'.csv', 'r',encoding='utf-8') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)
        return None

    CUSIPset = set()
    cutoffDt = datetime.date(2015, 3, 1)
    for line in fundData:
        filingDt = parser.parse(line[2])
        if (filingDt.date() >= cutoffDt):
            curCUSIP = line[3]
            CUSIPset.add(curCUSIP)

    if CUSIPset:
        CUSIPset = list(CUSIPset)
        for CUSIP in CUSIPset:
            HoldingAnalysisfromOneFund(CUSIP, fundname, fundlist)


def CreateMonitorLog(coreFunds):
    LatestAnalyzedData=[]
    for fund in coreFunds:
        try:
            with open('/Users/shared/SC13Forms/'+fund+'.csv', 'r',encoding='utf-8') as csvfile:
                fundData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            continue
        LatestAnalyzedData += [(fund, fundData[0][len(fundData[0])-1])]
    DumpTable(LatestAnalyzedData, '/Users/shared/SC13Monitor/MonitorLog.csv')

def Scan4Fundswithout13F(coreFunds):
    for fund in coreFunds:
        SC13file = '/Users/shared/SC13Forms/'+fund +'.csv'
        QtrfilePath = '/Users/shared/Fund clone/'+fund
        if os.path.exists(SC13file):
            if not os.path.exists(QtrfilePath):
                print(fund)


## Mar 29, 2017: BUG. If a fund is newly added to coreFunds and it is not yet in the MonitorLog, its contents will initially be ignored.
##  But it will be added to the MonitorLog and subsequent entries will be processed. This is fixed.
## However, it seems that this is really just for printing purposes. The procedure that generates sc13tickers doesn't take this output or has this issue.
def HoldingMonitor(coreFunds):
    coreFunds = list(set(coreFunds) - set(MonitorUpdateExclude))

    NewData = []
    NewFundCUSIPs = []

    MonitorLogFile = '/Users/shared/SC13Monitor/MonitorLog.csv'
    try:
        with open(MonitorLogFile, 'r', encoding='utf-8') as csvfile:
            MonitorLog = dict(csv.reader(csvfile))
    except Exception as e:
        print(e)

    for fund in coreFunds:
        try:
            with open('/Users/shared/SC13Forms/' + fund + '.csv', 'r', encoding='utf-8') as csvfile:
                fundData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            continue

        if fund in MonitorLog:
            for line in fundData:
                if (line[len(line) - 1] == MonitorLog[fund]):
                    break;
                else:
                    NewData += [line]
                    NewFundCUSIPs += [(fund, line[3])]
        else:
            for line in fundData:
                NewData += [line]
                NewFundCUSIPs += [(fund, line[3])]

    if (len(NewData) > 0):
        DumpTable(NewData,
                  '/Users/shared/SC13Monitor/Daily/' + datetime.date.today().strftime('%Y%m%d') + '.csv')

    return (NewFundCUSIPs)

def HoldingMonitorUpdate(coreFunds):
    coreFunds = list(set(coreFunds) - set(MonitorUpdateExclude))
    FundCUSIPpairs = HoldingMonitor(coreFunds)
    DailySnapshot = []
    if FundCUSIPpairs:
        for line in FundCUSIPpairs:
            ##            print(line)
            if (len(line[1]) > 0):
                tmpData = HoldingHistory(line[1], line[0])
            else:
                tmpData = None
            if tmpData:
                DailySnapshot += tmpData
                ##            HoldingAnalysisfromOneFund(line[1], line[0], coreFunds)
        CreateMonitorLog(coreFunds)

    if (len(DailySnapshot) > 0):
        DumpTable(DailySnapshot, '/Users/shared/SC13Monitor/Snapshots/' + datetime.date.today().strftime(
            '%Y%m%d') + '.csv')
        DumpTable(DailySnapshot, '/Users/shared/SC13Monitor/Snapshots/CurrentSnapshot.csv')

    print(FundCUSIPpairs)

def UpdateOpenPositionList(fundlist, winlen=45, incThreshold=0.3):
#1. If the OpenPosition list doesn't exist, create a new one by looping over all funds
#2. If it exists, then
#     2.1. Clean existing data of stale entries
#     2.2  Process filings data that are newer than latest entry in the existing data
#     2.3  Add data, or if the underlying already exists, update the date
    outputfile = '/Users/shared/SC13Monitor/Lists/sc13tickers.csv'

# Exclude major mutual funds such as BlackRock as the filings tend to flood the monitor and there is no way to track and monitor them manually
# The funds to be excluded are defined in MonitorUpdateExclude
    fundlist = sorted(list(set(fundlist) - set(MonitorUpdateExclude)))

    if ( not os.path.isfile(outputfile)):
        outputdata = []
        for fund in fundlist:
            print(fund)
            fundresult = ActiveLongCandidatefromFund(fund, winlen, incThreshold)
            if fundresult:
                outputdata += fundresult
                with open(outputfile, 'w', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(outputdata)
        return(None)
    else:
        curData = []
        with open(outputfile, 'r', encoding='utf-8') as csvfile:
            curData = [tuple(line) for line in csv.reader(csvfile) ]

        outputdata=[]
        for fund in fundlist:
            fundresult = ActiveLongCandidatefromFund(fund, winlen, incThreshold)
            if fundresult:
                outputdata += fundresult

        if outputdata:
            #remove lines in exisiting data where the CUSIP is in the new outputdata:
            newCUSIPs = set([Line[1] for Line in outputdata])
            curData = [Line for Line in curData if not (Line[1] in newCUSIPs)]

        if curData:
            curData = [Line for Line in curData if (datetime.datetime.now() - parser.parse(Line[2])).days <= winlen ]

        curData += outputdata

        with open(outputfile, 'w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(curData)

    return(None)


def ActiveLongCandidatefromFund(fundname, winlen=45, incThreshold=0.3):
    if (not dictCUSIPTicker):
        print('CUSIP Ticker file is missing')
        return (None)

    filename = '/Users/shared/SC13Forms/' + fundname + '.csv'
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as csvfile:
                fundData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            print(e)
            return None
    else:
        return None

    TickerList = []
    FundOutput = []

    if (fundData):
        cutoffDt = datetime.datetime.now() - datetime.timedelta(days=winlen)
        cutoffDt = cutoffDt.replace(hour=0, minute=0)
        cutoffDt = str(cutoffDt)

        fundData = [line for line in fundData if line[2] >= cutoffDt]
        if (fundData):
            CUSIPs = sorted(list(set([line[3] for line in fundData]) - {''}))
            for curCUSIP in CUSIPs:
                curTradeSheet = TradeSheet(curCUSIP, fundname, True, True, winlen)
                if (curTradeSheet):
                    TickerCUSIP = TS2ActiveSignal(curTradeSheet[len(curTradeSheet) - 1], incThreshold)
                    if TickerCUSIP:
                        TickerList += TickerCUSIP
                        ##                    FundOutput += [curTradeSheet[len(curTradeSheet)-1]]

    return (sorted(TickerList))


##    return(FundOutput)


## This function update the active long candidate list from a fund's SC13 filing, while referencing the monitor log
##  which keeps track of the last sc13 filing for each fund that is processed
##
def UpdateActiveLongfromFund(fundname, incThreshold=0.3):
    # First check if the sc13 file is newer than the Monitor Log file
    MonitorLogFile = '/Users/shared/SC13Monitor/MonitorLog.csv'
    sc13file = '/Users/shared/SC13Forms/' + fundname + '.csv'

    if (os.path.isfile(sc13file)):

        sc13time = datetime.datetime.fromtimestamp(os.path.getmtime(sc13file))
        monitorfiletime = datetime.datetime.fromtimestamp(os.path.getmtime(MonitorLogFile))

        # if sc13 file is newer than the monitor log, read monitor log for further processing
        # else nothing needs to be done
        if ((sc13time - monitorfiletime).total_seconds() > 0):
            try:
                with open(MonitorLogFile, 'r', encoding='utf-8') as csvfile:
                    MonitorLog = dict(csv.reader(csvfile))
            except Exception as e:
                print(e)
                return (ActiveLongCandidatefromFund(fundname, 45, incThreshold))
        else:
            return (None)

        # if fund in Monitor log then use the new filings to calculate cutoff date for the fund
        # else run the ActiveLongCandidatefromFund function with the standard cutoff date
        if fundname in MonitorLog:
            # Then see if there are new filings to be processed
            try:
                with open(sc13file, 'r', encoding='utf-8') as csvfile:
                    fundData = [tuple(line) for line in csv.reader(csvfile)]
            except Exception as e:
                return (None)

            newData = []
            for line in fundData:
                if (line[len(line) - 1] == MonitorLog[fundname]):
                    break;
                else:
                    newData += [line]

            if newData:
                newDataDts = sorted(set([line[2] for line in newData]))
                newDataDt = newDataDts[0]
                fundCutoff = (datetime.datetime.now() - parser.parse(newDataDt)).days
                print(fundCutoff)
                return (ActiveLongCandidatefromFund(fundname, fundCutoff, incThreshold))
            else:
                return (None)
        else:
            return (ActiveLongCandidatefromFund(fundname, 45, incThreshold))
    else:
        # SC13 file missing, do nothing
        return (None)


########################################################################################################################
########################################################################################################################
######## 3. Backtesting related
########################################################################################################################
########################################################################################################################


#1. check  that both quarterly filing and SC13 filings exist
#2. Collect all CUSIPs
#3. Generate data for backtesting
#September 5, 2016: this function (GenerateBacktestDataOneFund) is now obsolete
def GenerateBacktestDataOneFund(fund):
    try:
        with open('/Users/shared/SC13Forms/'+fund+'.csv', 'r',encoding='utf-8') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)
        return None

    if fund in Fund2QtrlyDict:
        filingpath = '/Users/shared/fund clone/'+ Fund2QtrlyDict[fund]
    else:
        filingpath = '/Users/shared/fund clone/'+ fund
    if os.path.exists(filingpath):
        QtrlyFilings = glob(filingpath + '/*.csv')
    else:
        return None
    if QtrlyFilings is None:
        return None

    CUSIP13Fset = []
    for oneFiling in QtrlyFilings:
        try:
            with open(oneFiling, 'r',encoding='utf-8') as csvfile:
                filingData = [tuple(line) for line in csv.reader(csvfile)]
        except Exception as e:
            print(e)
            continue
        CUSIP13Fset += [line[2] for line in filingData[2:]]
    CUSIPSC13set = [line[3] for line in fundData]

    CUSIPSet = set(CUSIP13Fset + CUSIPSC13set)
    skipCUSIPs = {'', ' ', 'CUSIP'}
    CUSIPSet = list(CUSIPSet - skipCUSIPs)
    #print(CUSIPSet)
    for curCUSIP in CUSIPSet:
        #print(curCUSIP)
        x = HoldingHistory(curCUSIP, fund, OnlyRecent=False, showClosed=True)
        DumpTable(x, '/Users/shared/HoldingHistoryBacktest/'+fund+'/'+curCUSIP+'.csv')
        #print('here')
        #print(x)

# Generate historical trading history for a fund from its SC13 filings
def TradeDatafromSC13Signals(fund):
    try:
        with open('/Users/shared/SC13Forms/'+fund+'.csv', 'r',encoding='utf-8') as csvfile:
            fundData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)
        return None

    CUSIPSC13set = set([line[3] for line in fundData])
    skipCUSIPs = {'', ' ', 'CUSIP'}
    CUSIPSet = list(CUSIPSC13set - skipCUSIPs)

    #CUSIPSet=['00767E102']
    for curCUSIP in CUSIPSet:
        historiesList = HoldingHistory(curCUSIP, fund,  doCall=True)
        #historiesList = SegmentHoldingHistory(x)

        if historiesList is None:
            continue

        if len(historiesList)<1:
            continue
        if (len(historiesList) == 1):
            DumpTable(historiesList[0], '/Users/shared/HoldingHistoryBacktest/' + fund + '/' + curCUSIP + '.csv')
        else:

            outputCtr = 0
            for curHist in historiesList:
                #check if the holding history contains at least one SC13 filing, as signals are from SC13 filings
                filingTypes = set([line[9] for line in curHist])
                if 'SC13' in filingTypes:
                    if outputCtr ==0 :
                        outputfile = '/Users/shared/HoldingHistoryBacktest/'+fund+'/'+curCUSIP+'.csv'
                    else:
                        outputfile = '/Users/shared/HoldingHistoryBacktest/' + fund + '/' + curCUSIP + '_' + str(outputCtr) + '.csv'
                    DumpTable(curHist, outputfile)
                    outputCtr += 1

def GenerateBacktestData(coreFunds):
    for fund in coreFunds:
        GenerateBacktestDataOneFund(fund)


def TradeSheet (CUSIP, fundname, use13F=True, onlyRecent=False, winlen=999999):
    fundhistory = HoldingHistory(CUSIP, fundname, False, use13F)
    if (fundhistory):
        if onlyRecent:
            retData = KeepOnlyRecent(fundhistory[len(fundhistory)-1], winlen)
        else:
            retData = [line for curHist in fundhistory for line in curHist]
        return([retData])

    else:
        return(None)

#Take a tradesheet and decide if the underlying is a ticker to be monitorred for trade, i.e. put in OpenPositions.csv
#This is used only for scanning for sc13 filings for long candidates for monitoring
#The trade sheet has to be recent, i.e. the last filing is within 45 (or another specified number) days
# Returns a Ticker CUSIP pair if a) a one line filing with percentage > 4.5 ; or b) last share count is at least 30% more than before (defined by incThreshold)
# Print out if the CUSIP doesn't have a matching Ticker
# What it doesn't take into account is option positions !!!
def TS2ActiveSignal(curTradeSheet, incThreshold = 0.2):
    if curTradeSheet:
        #print('here2')
        #print(curTradeSheet)
        # first sort the data by the actual event date (usually data is sorted by filing date) and then 
        #  by the holding type (SH, call, put or other)
        # the complexity always rises when a sc13 is filed after the previous quarter end but before the
        #  13f is filed 45 days later

        #####What needs to be done further is to probably remove options lines from 13F!!!
        
        curTradeSheet = sorted(curTradeSheet, key=itemgetter(7, 5 ))
            
##        print(curTradeSheet)
        lastShareNum = None
        priorShareNum = None
        lastSharePct = None
        priorSharePct = None
        outputCUSIP = None
        if (len(curTradeSheet) > 1 ):

            #if last line is an SC13 filing and the percentage number is less than 4.5, skip this underlying
            if (curTradeSheet[len(curTradeSheet)-1][9] == 'SC13'):
                try:
                    lastSharePct = locale.atof(curTradeSheet[len(curTradeSheet)-1][6])
                except Exception as e:
                    lastSharePct = None
                if ((not lastSharePct is None) and (lastSharePct < 4.5)):
                    return(None)
            
            try:
                lastShareNum = locale.atoi(curTradeSheet[len(curTradeSheet)-1][3])
            except Exception as e:
                lastShareNum = None
            try:
                priorShareNum = locale.atoi(curTradeSheet[len(curTradeSheet)-2][3])
            except Exception as e:
                priorShareNum = None
##            print(curTradeSheet[len(curTradeSheet)-1][3])
##            print(lastShareNum)
##            print(priorShareNum)
            if ((not lastShareNum is None) and (not priorShareNum is None)):
##                print(lastShareNum)
##                print(priorShareNum)
                if (lastShareNum >= math.ceil((1+incThreshold)*priorShareNum)):
                    ## return(TickerLookup(curTradeSheet[len(curTradeSheet)-1][2]))
                    outputCUSIP = curTradeSheet[len(curTradeSheet)-1][2]
            else:
                try:
                    lastSharePct = locale.atof(curTradeSheet[len(curTradeSheet)-1][6])
                except Exception as e:
                    lastSharePct = None
                try:
                    priorSharePct = locale.atof(curTradeSheet[len(curTradeSheet)-2][6])
                except Exception as e:
                    priorSharePct = None
                if ((not lastSharePct is None) and (not priorSharePct is None)):
                    if (lastSharePct>= (1+incThreshold)*priorSharePct):
                        ##return(TickerLookup(curTradeSheet[len(curTradeSheet)-1][2]))
                        outputCUSIP = curTradeSheet[len(curTradeSheet) - 1][2]
                else:
                    print('Retrieved data incomplete:')
                    print(curTradeSheet[len(curTradeSheet)-1])
                    return(None)
        else:
            try:
                lastSharePct = locale.atof(curTradeSheet[len(curTradeSheet)-1][6])
            except Exception as e:
                lastSharePct = None
            if ((not lastSharePct is None) and (lastSharePct > 4.5)):
                ##return(TickerLookup(curTradeSheet[len(curTradeSheet)-1][2]))
                outputCUSIP = curTradeSheet[len(curTradeSheet) - 1][2]

        if outputCUSIP:
            if (not dictCUSIPTicker):
                print('CUSIP Ticker file missing.')
                return (None)

            if (outputCUSIP in dictCUSIPTicker):
                return([(dictCUSIPTicker[outputCUSIP], outputCUSIP, curTradeSheet[len(curTradeSheet) - 1][8])])
            else:
                print('CUSIP not found: ' + outputCUSIP)
                print(curTradeSheet[len(curTradeSheet) - 1])
                return(None)
    else:
        return(None)




##print(x[len(x)-1])
##y = TS2ActiveSignal(x[len(x)-1], incThreshold=0.2)
##y = ActiveLongCandidatefromFund('LONDONCOOFVIRGINIA',45)
##print(y)
##y = UpdateActiveLongfromFund('LONDONCOOFVIRGINIA')
##print(y)

#x = HoldingHistory("786578104", "Point72")
#print(len(x))
#print(x[len(x)-1])


#x = HoldingAnalysis('29265N108', ["Corvex"])
#print(x)


#HoldingAnalysisOpenPositions([('TCK', '878742204')], ["DIMENSIONALFUNDADVISORSLP", 'ICAHN'])
#TradeDatafromSC13Signals('Maverick')
#TradeDatafromSC13Signals('Point72')
#TradeDatafromSC13Signals('CANADAPENSIONPLANINVESTMENTBOARD')
#TradeDatafromSC13Signals('KnollCapital')