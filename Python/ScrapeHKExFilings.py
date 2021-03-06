#!/Users/mintang/anaconda3/envs/Legacy/bin/python

from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import re
import csv
import os
import pandas as pd
from urllib.error import URLError, HTTPError
from operator import itemgetter
import FileToolsModule as FTM
import locale
import shutil
import HKExBuybackSummary as HKBuyback
from glob import glob

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

#Filings b4 2017.07.02 has an extra "filing" in its url
##B4 2017.07.02: http://sdinotice.hkex.com.hk/filing/di/NSAllFormDateList.aspx?sa1=da&scsd=08/04/2015&sced=08/04/2016&src=MAIN&lang=EN
##After 2017.07.02: http://sdinotice.hkex.com.hk/di/NSAllFormDateList.aspx?sa1=da&scsd=26/03/2018&sced=27/03/2018&src=MAIN&lang=EN

HKFunds = ["Fuhui Capital Investment Limited",
           "香港大生投資控股有限公司",
           "大生控股有限公司",
           "深圳前海大生股權投資基金有限公司",
           "Argyle Street Management Holdings Limited",
           "PX Capital Management Ltd",
           "PX Capital Partners",
           "PX Global Advisors LLC",
           "Mawer Investment Management Ltd",
           "Keytone Capital Partners",
           "Keytone Investment Group Ltd",
           "Beijing Jiankun Investment Group Co",
           'Tsinghua Unigroup Co',
           'DT Capital Management Co',
           "Spring Asset Management Limited",
           "Wykeham Capital Limited",
           "Wykeham Capital Asia Value Fund",
           "YM INVESTMENT LIMITED",
           "CS Asia Opportunities Master Fund",
           "China Silver Asset Management Limited",
           "VMS Investment Group Limited",
           "Guo Guangchang",
           "China Minsheng Investment Group Corp",
           "Head and Shoulders Direct Investment Limited",
           "Central China International Investment Company Limited",
           "DA Equity Partners Limited",
           "Chen Ningdi",
           "Edgbaston Investment Partners LLP"
           ]

def SaveFundsFile():
    CleanedFundNames = [(FTM.CleanString(line),) for line in HKFunds]
    FTM.SafeAddData("/Users/Shared/HKEx/Funds.csv", CleanedFundNames)
    return
SaveFundsFile()


def ExtractFundFilingData(fundname, filingdata):
    funddata = []
    for line in filingdata:
        investorName = line[3]
        if re.search(fundname, investorName, re.I):
            funddata += [line]

    return(funddata)

def Fundname2FundFileName(fund):
    p = re.compile('\s+')
    fundnameStr = p.sub('', fund)
    p = re.compile('\.')
    fundnameStr = p.sub('', fundnameStr)
    return(fundnameStr)

def UpdateFundsfromTickerFile(funds, ticker, lazyUpdate=True):
    tickerfile = "/Users/Shared/HKEx/FilingsbyTicker/"+ticker+".csv"
    if os.path.isfile(tickerfile):
        tickerfiletime = os.path.getmtime(tickerfile)
        with open(tickerfile, encoding='utf-8') as f:
            tickerdata = [(ticker,) + tuple(line) for line in csv.reader(f)]
    else:
        return

    for fund in funds:
        #print(fund)
        fundnameStr = Fundname2FundFileName(fund)
        fundfile = "/Users/Shared/HKEx/FilingsByFund/"+fundnameStr+".csv"
        if os.path.isfile(fundfile):
            if lazyUpdate:
                fundfiletime = os.path.getmtime(tickerfile)
                if (fundfiletime > tickerfiletime):
                    continue
        fundData = ExtractFundFilingData(fund, tickerdata)
        FTM.SafeAddData(fundfile, fundData, sortKeys=[1])
    return

def UpdateOneFundfromData(fund, data):
    fundnameStr = Fundname2FundFileName(fund)
    fundfile = "/Users/Shared/HKEx/FilingsByFund/" + fundnameStr + ".csv"
    fundData = ExtractFundFilingData(fund, data)
    FTM.SafeAddData(fundfile, fundData, sortKeys=[1])
    return

def UpdateFundsfromData(funds, data):
    for fund in funds:
        UpdateOneFundfromData(fund, data)
    return

def InitializeFundFilings():
    fundfile = "/Users/Shared/HKEx/Funds.csv"
    if os.path.isfile(fundfile):
        try:
            with open(fundfile, encoding='utf-8') as f:
                funds = [tuple(line) for line in csv.reader(f)]
        except Exception as e:
            print(e)
            return
    else:
        return
    funds = [line[0] for line in funds]
    newfunds = []
    for fund in funds:
        if not os.path.isfile("/Users/Shared/HKEx/FilingsbyFund/"+ Fundname2FundFileName(fund) + ".csv"):
            newfunds += [fund]

    if len(newfunds)<1:
        return
    else:
        funds = newfunds

    tickerfiles = glob("/Users/Shared/HKEx/FilingsbyTicker/*.csv")
    tickers = [os.path.splitext(os.path.basename(tickerfile))[0] for tickerfile in tickerfiles]
    print(str(len(funds)) + " funds to initialize")
    print(str(len(tickers)) + " stocks with filings")
    for ticker in tickers:
        UpdateFundsfromTickerFile(funds, ticker, lazyUpdate=False)
    return

def DailyUpdateFunds(data):
    fundfile = "/Users/Shared/HKEx/Funds.csv"
    if os.path.isfile(fundfile):
        try:
            with open(fundfile, encoding='utf-8') as f:
                funds = [tuple(line) for line in csv.reader(f)]
        except Exception as e:
            print(e)
            return
    else:
        return

    funds = [line[0] for line in funds]
    UpdateFundsfromData(funds, data)
    return




def BackupImportantFiles():
    shutil.copy2("/Users/Shared/HKEx/FullScrapingList.csv", "/Users/Shared/HKEx/Backup/")
    shutil.copy2("/Users/Shared/HKEx/HistoricalList2Scrape.csv", "/Users/Shared/HKEx/Backup/")
    #shutil.copy2("/Users/Shared/HKEx/ScrapedIDs.csv", "/Users/Shared/HKEx/Backup/")


def LogErrorUrl(url):
    errorLogFile = "/Users/Shared/HKEx/ErrorLogs.csv"
    FTM.AddString2File(errorLogFile, url)
    return


def SaveFilingData (stockCode, eventDt, filingCode, investorName, eventInfo, aevtSummary, bevtSummary, filingurl, filingDt):
    tgtFile = "/Users/Shared/HKEx/FilingsByTicker/" + str(stockCode) + ".csv"
    newData = [( eventDt, filingCode, investorName, str(eventInfo[0]), eventInfo[3], str(eventInfo[4]), str(eventInfo[5]), str(eventInfo[6]), str(eventInfo[7]), str(aevtSummary[0]), str(aevtSummary[1]), str(aevtSummary[2]), str(aevtSummary[3]), str(bevtSummary[0]),str(bevtSummary[1]),str(bevtSummary[2]),str(bevtSummary[3]),filingurl, filingDt),]

    if os.path.isfile(tgtFile):
        try:
            with open(tgtFile, mode='r', encoding='utf-8') as infile:
                fileData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            print(e)
            return

        fileData += newData
        fileData = list(set(fileData))
        fileData = sorted(fileData, key=itemgetter(0, 2))
        FTM.SafeSaveData(tgtFile, fileData)
    else:
        FTM.SafeSaveData(tgtFile, newData)
    return


def RemovedScrapedFilings(filinglist):
    curFilingFile = "/Users/Shared/HKEx/FullScrapingList.csv"
    outputList = []

    if os.path.isfile(curFilingFile):
        try:
            with open(curFilingFile, encoding='utf-8') as f:
                tmpData = [tuple(line) for line in csv.reader(f)]
        except Exception as e:
            print(e)
            return(filinglist)

        curData = set(tmpData)
        for line in filinglist:
            if not (line in curData):
                outputList += [line]

        return(outputList)
    else:
        return(filinglist)


def CheckFilingListAgainstErrorLog(filinglist):
    if (filinglist is None) or (len(filinglist)<1):
        return None

    ScrapedFormIDFile = "/Users/Shared/HKEx/Errorlogs.csv"
    IDFileExists = os.path.isfile(ScrapedFormIDFile)
    if IDFileExists:
        ScrapedIDs = []
        try:
            with open(ScrapedFormIDFile, 'r+') as csvfile:
                IDlines = [tuple(line) for line in csv.reader(csvfile)]
            ScrapedIDs = sorted(list(set([line[0] for line in IDlines])))
            with open(ScrapedFormIDFile, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                for ticker in ScrapedIDs:
                    writer.writerow([ticker])

        except Exception as e:
            print(e)

        filinglistIDs = [line[1] for line in filinglist]
        unscrapedIDs = list(set(filinglistIDs) - set(ScrapedIDs))
        targetList = [line for line in filinglist if line[1] in unscrapedIDs ]

        if (targetList is None) or (len(targetList)<1):
            return None
        else:
            return(targetList)
    else:
        return(filinglist)


def RetrieveFilingListfromUrl(listurl, urlheader):
    filingheader = urlheader[:urlheader.index("NSAllForm")]
    #print(filingheader)
    try:
        filingpage = urlopen(listurl)
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return None
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
        return None
    except Exception as e:
        print('Unknown exception raised.')
        print(e)
        return None

    soup = BeautifulSoup(filingpage)
    lines = soup.findAll('td', attrs={'class' : "tbCell"})
    #print(lines)
    outputList = []
    for line in lines:
        #print(line)
        tmpHREF = line.find_all('a')
        if tmpHREF:
            for tmpRef in tmpHREF:
                formID = tmpRef.string
                formUrl = filingheader+tmpRef['href']
                if (len(formID)>13):
                    # To do:
                    # 1. Identify the position of the formID is formUrl, and truncate the the formUrl to get a consistent and concise URL for the form
                    # 2. Look for filing code, e.g. 103
                    #simpleUrl = formUrl[:formUrl.index(formID)]+formID
                    simpleUrl = formUrl[:formUrl.index("&")]
                    filingDt = line.next_sibling.string
                    tmpfilingCode = line.next_sibling.next_sibling.next_sibling.next_sibling.text
                    if tmpfilingCode.find("("):
                        filingCode = tmpfilingCode[:(tmpfilingCode.index("("))]
                        longshortCode = tmpfilingCode[(tmpfilingCode.index("(")+1):(tmpfilingCode.index("(")+2)]
                    else:
                        filingCode = tmpfilingCode
                        longshortCode = "N"
                    outputList += [(formID, simpleUrl, filingDt, filingCode, longshortCode),]

    outputList = list(set(outputList))
    return(outputList)


def RetrieveFilingsList(urlheader, startDt, endDt):
    #retrieve the filings list between to given dates
    #1. Use startDt and endDt to create filings searching url
    #2. Use the url to retrieve filings list
    #3. parse the filings list to create a table of filing IDs and urls
    startDtStr = startDt.strftime("%d/%m/%Y")
    endDtStr = endDt.strftime("%d/%m/%Y")
    listurl = urlheader+startDtStr+"&sced="+endDtStr+"&src=MAIN&lang=EN"
    print(listurl)
    FilingList = []
    pageCtr = 0
    pagelisturl = listurl
    while True:
        curFilingList = RetrieveFilingListfromUrl(pagelisturl, urlheader)
        if (curFilingList is None) or (len(curFilingList)<=0):
            break

        if (len(curFilingList)>0):
            FilingList += curFilingList

        pageCtr +=1
        pagelisturl = listurl + '&pg=' + str(pageCtr)
        print(pageCtr)

    FilingList = list(set(FilingList))
    return(FilingList)


def GetEventStr (strurl):
    retStr =''
    if strurl:
        retStr = FTM.CleanString(strurl.text)
    return(retStr)


def GetEventNumber(strurl, float=True, verbose=False):
    retNum =''
    if strurl:
        try:
            if float:
                retNum = locale.atof(strurl.string)
            else:
                retNum = locale.atoi(strurl.string)
        except Exception as e:
            if verbose:
                print(strurl)
                print(e)
    return(retNum)


def ScrapeEventDetail(eventUrl):
    if eventUrl is None:
        return None
    idAppendStr =''
    eventShare = eventUrl.find('span', attrs={'id': "lblDEvtShare"})
    if eventShare is None:
        eventShare = eventUrl.find('span', attrs={'id': "lblDEvtShare2"})
        if eventShare is None:
            return None
        else:
            idAppendStr ='2'

    eventShare = GetEventNumber(eventShare, float=False)
    if (eventShare ==''):
        return None

    idStr = "lblDEvtPosition" + idAppendStr
    eventPosition = eventUrl.find('span', attrs={'id':idStr})
    eventPositionStr = GetEventStr(eventPosition)

    #Event reason, retrieve only the code
    idStr = "lblDEvtReason" + idAppendStr
    eventReason = eventUrl.find('span', attrs={'id': idStr})
    if eventReason:
        tmpReasonCode = eventReason.find('td')
        if (tmpReasonCode is None):
            eventReasonCode = GetEventStr(eventReason)
        else:
            eventReasonCode = GetEventStr(tmpReasonCode)
    else:
        eventReasonCode = ''

    #Event currency:
    idStr = "lblDEvtCurrency" + idAppendStr
    eventCurrency = eventUrl.find('span', attrs={'id': idStr})
    eventCurrencyStr = GetEventStr(eventCurrency)

    # Event prices:
    idStr = "lblDEvtHPrice" +idAppendStr
    eventHPrice = eventUrl.find('span', attrs={'id': idStr})
    eventHPriceNum = GetEventNumber(eventHPrice)

    idStr = "lblDEvtAPrice" + idAppendStr
    eventAPrice = eventUrl.find('span', attrs={'id': "lblDEvtAPrice"})
    eventAPriceNum = GetEventNumber(eventAPrice)

    idStr = "lblDEvtAConsider"+idAppendStr
    eventAConsider = eventUrl.find('span', attrs={'id': idStr})
    eventAConsiderNum = GetEventNumber(eventAConsider)

    idStr =  "lblDEvtNatConsider"+idAppendStr
    eventNATConsider = eventUrl.find('span', attrs={'id': "lblDEvtNatConsider"})
    eventNATConsiderNum = GetEventNumber(eventNATConsider)

    #print(eventShare, eventPositionStr, eventReasonCode, eventCurrencyStr, eventHPriceNum, eventAPriceNum)
    return ([eventShare, eventPositionStr, eventReasonCode, eventCurrencyStr, eventHPriceNum, eventAPriceNum, eventAConsiderNum, eventNATConsiderNum])


def ScrapePositionSummary(summaryUrl):
    longShares = ''
    longPct =''
    shortShares = ''
    shortPct =''
    for line in summaryUrl.findAll('tr'):
        lineItems = line.findAll('td')
        if (len(lineItems) !=3):
            continue
        positionType = lineItems[0].string

        if re.search("long\s+position", positionType, re.I):
            positionType="Long"
            longShares = GetEventNumber(lineItems[1], float=False)  # numShares = lineItems[1].string
            longPct = GetEventNumber(lineItems[2])
        else:
            if re.search("short\s+position", positionType, re.I):
                positionType = "Short"
                shortShares = GetEventNumber(lineItems[1], float=False)  # numShares = lineItems[1].string
                shortPct = GetEventNumber(lineItems[2])
            else:
                continue
    #print(longShares, longPct, shortShares, shortPct)
    return([longShares, longPct, shortShares, shortPct])

def ScrapeInvestorInfo(formUrl):
    investorID = formUrl.find('span', attrs={'id': "lblDName"})
    if investorID is None:
        directorID = formUrl.find('span', attrs={'id': "lblEngName"})
        directorName =''
        upCtr = 0
        if directorID:
            while (directorName==''):
                surnameID = directorID.find('span', attrs={'id': "lblDSurname"})
                firstnameID = directorID.find('span', attrs={'id': "lblDFirstname"})
                midnameID = directorID.find('span', attrs={'id': "lblDMidname"})

                if surnameID:
                    directorName = surnameID.text
                else:
                    directorName =''
                if firstnameID:
                    directorName = directorName + ' ' + firstnameID.text
                if midnameID:
                    directorName = directorName + ' ' + midnameID.text

                directorName = FTM.CleanString(directorName)
                directorID = directorID.parent
                upCtr +=1
                if upCtr >3:
                    break
    else:
        directorName = GetEventStr(investorID)
    return(directorName)


def ScrapeOneHKExFiling(formID, filingurl, filingDtIn, filingCode, longshortCode):
    print(formID, filingurl)
    try:
        filingpage = urlopen(filingurl)
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return None
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
        return None
    except Exception as e:
        print('Unknown exception raised.')
        print(e)
        return None

    soup = BeautifulSoup(filingpage)
    #1. Look for stock code:
    lines = soup.findAll('span', attrs={'id': "lblDStockCode"})
    if lines:
        stockCode = GetEventStr(lines[0])
    else:
        LogErrorUrl(filingurl)
        return
    if stockCode == '':
        LogErrorUrl(filingurl)
        return

    #2. Look for event date:
    lines = soup.findAll('span', attrs={'id': "lblDEventDate"})
    if lines and lines[0]:
        eventDt = GetEventStr(lines[0])
    else:
        eventDt = ''
    if len(eventDt)>10:
        eventDt=eventDt[:10]
        eventDt = datetime.datetime.strptime(eventDt, "%d/%m/%Y").strftime("%Y/%m/%d")

    #3. Look for filing date:
    lines = soup.findAll('span', attrs={'id': "lblDSignDate"})
    if lines and lines[0]:
        filingDt = GetEventStr(lines[0])
    else:
        filingDt = filingDtIn.strip()
    if filingDt =='':
        filingDt = filingDtIn.strip()
    if len(filingDt)>10:
        filingDt = filingDt[:10]
        filingDt = datetime.datetime.strptime(filingDt,"%d/%m/%Y").strftime("%Y/%m/%d")

    #4. Look for name of director or substantial holder
    investorName = ScrapeInvestorInfo(soup)

    #6. Look for trading information
    tmpEevent = soup.find('tr', attrs={'id': "trRelevantEvent1"})
    event1 = ScrapeEventDetail(tmpEevent)
    tmpEevent = soup.find('tr', attrs={'id': "trRelevantEvent2"})
    event2 = ScrapeEventDetail(tmpEevent)
    if event2 is None:
        if event1 is None:
            #LogErrorUrl(filingurl)
            #UpdateScrapedIDs(formID)
            return([])
        else:
            eventInfo = event1
    else:
        if event1 is None:
            eventInfo = event2
        else:
            eventInfo = event1

    #7. Look for Before/After Position info
    preEventSummary = soup.find('table', attrs={'id': "grdSh_BEvt"})
    #print(preEventSummary)
    bevtSummary = ScrapePositionSummary(preEventSummary)
    postEventSummary = soup.find('table', attrs={'id': "grdSh_AEvt"})
    #print(postEventSummary)
    aevtSummary = ScrapePositionSummary(postEventSummary)

    SaveFilingData (stockCode, eventDt, filingCode, investorName, eventInfo, aevtSummary,  bevtSummary, filingurl.strip(), filingDt)
    return ([(stockCode, eventDt, filingCode, investorName, eventInfo, aevtSummary,  bevtSummary, filingurl.strip(), filingDt)])

def ScrapeHKExFilingsfromList(targetList):
    scrapeResults = []
    residualList = []
    #scrapedIDs = []
    for target in targetList:
        scrapedLine = []
        try:
            scrapedLine = ScrapeOneHKExFiling(target[0], target[1], target[2], target[3],target[4])
        except Exception as e:
            LogErrorUrl(target[1])
            print(e)

        if  (scrapedLine is not None) :
            if (len(scrapedLine) > 0):
                scrapeResults += scrapedLine
            #scrapedIDs += [(target[0],)]
        else:
            residualList += [target]
    return((scrapeResults, residualList))

def ScrapeHKExFilingsbyDates(startDt, endDt):
    #1. generate target list:
    urlheader = "http://sdinotice.hkex.com.hk/di/NSAllFormDateList.aspx?sa1=da&scsd="
    filinglist1 = RetrieveFilingsList(urlheader, startDt, endDt)
    urlheader = "http://sdinotice.hkex.com.hk/filing/di/NSAllFormDateList.aspx?sa1=da&scsd="
    filinglist2 = RetrieveFilingsList(urlheader, startDt, endDt)

    if (filinglist2 is None):
        filinglist = filinglist1
    else:
        if (filinglist1 is None):
            filinglist = filinglist2
        else:
            filinglist = filinglist1 + filinglist2
    if (filinglist is None) or (len(filinglist) <1):
        return

    #2. check filinglist and prune scraped filings
    print(str(len(filinglist)) + " filings between " + str(startDt) + " and " + str(endDt))
    filinglist = RemovedScrapedFilings(filinglist)
    if (filinglist is None) or (len(filinglist)<1):
         print('Nothing to scrape excluding scraped forms.')
         return None
    else:
         print(str(len(filinglist)) + " to scrape.")
    filinglist = CheckFilingListAgainstErrorLog(filinglist)
    if (filinglist is None) or (len(filinglist) < 1):
        print('Nothing to scrape excluding known errors. ')
        return None
    else:
        print(str(len(filinglist)) + " to scrape excluding known errors.")

    #3. Scrape filings
    (scrapedResults, residualList)  = ScrapeHKExFilingsfromList(filinglist)

    #4. Update FullScrapingLists.csv and HistoricalList2Scrape.csv
    FTM.SafeAddData("/Users/Shared/HKEx/FullScrapingList.csv", filinglist)

    if (residualList is not None) and (len(residualList)>0):
        FTM.SafeAddData("/Users/Shared/HKEx/HistoricalList2Scrape.csv", residualList)
        UpdateFilingList2Scrape()

    return(scrapedResults)

def SaveDailyFilings(outputData):
    if (outputData is None) or (len(outputData)<1):
        return

    outputfile = '/Users/shared/HKEx/DailyFilings/' + datetime.date.today().strftime('%Y%m%d') + '.csv'
    saveData = []
    for line in outputData:
        saveData +=[(line[0], line[1], line[2], line[3], line[4][0],  line[4][3], line[4][4], line[4][5], line[4][6], line[4][7], line[5][0], line[5][1], line[5][2], line[5][3], line[6][0], line[6][1], line[6][2], line[6][3],line[7],line[8], )]

    saveData = sorted(saveData, key=itemgetter(1,0), reverse=True)
    FTM.SafeAddData(outputfile, saveData, sortKeys=[1,0])
    return

def DailyScrapeHKExFilings():
    #1. Backup important files:
    BackupImportantFiles()

    #2. Scrape recent filings
    scrapingdts = pd.bdate_range(end=datetime.datetime.now(), periods=40).tolist()
    outputData = ScrapeHKExFilingsbyDates(scrapingdts[0], scrapingdts[(len(scrapingdts)-1)])

    #3. Create today's snapshots and clean up
    SaveDailyFilings(outputData)
    return(outputData)

def ScrapeHistoricalHKExFilings(targetNum=5000):
    historicalListFile = "/Users/Shared/HKEx/HistoricalList2Scrape.csv"
    if os.path.isfile(historicalListFile):
        try:
            with open(historicalListFile, mode='r', encoding='utf-8') as inhistfile:
                histList = [tuple(line) for line in csv.reader(inhistfile)]
        except Exception as e:
            print(e)
            return
        print(str(len(histList)) + " in list.")
    else:
        return

    histList = CheckFilingListAgainstErrorLog(histList)
    if (len(histList)>0):
        (tmpResults,  tmpresidualList) = ScrapeHKExFilingsfromList(histList[:min(targetNum, len(histList))])
        remainingList = histList[min(targetNum, len(histList)):]
        print( str(len(tmpresidualList)) + ' lines did not scrape.')
        print(str(len(remainingList)) + ' lines not processed this time.')
        if (tmpresidualList is not None) and (len(tmpresidualList) > 0):
            remainingList += tmpresidualList
        FTM.SafeSaveData("/Users/Shared/HKEx/HistoricalList2Scrape.csv", remainingList)
        UpdateFilingList2Scrape()

    return

def UpdateFilingList2Scrape():
    historicalListFile = "/Users/Shared/HKEx/HistoricalList2Scrape.csv"
    try:
        with open(historicalListFile, mode='r', encoding='utf-8') as inhistfile:
            histList = [tuple(line)+ ((datetime.datetime.strptime(line[2], "%d/%m/%Y")).strftime("%Y/%m/%d"),) for line in csv.reader(inhistfile)]
    except Exception as e:
        print(e)
        return
    print(len(histList))

    histList = sorted(histList,key=itemgetter(5), reverse=True)
    histList = [tuple(line[:(len(line)-1)]) for line in histList]
    FTM.SafeSaveData(historicalListFile,histList)
    return

def HKExFilingDailyBatch():
    tmpfile = '/Users/shared/HKEx/DailyFilings/' + datetime.date.today().strftime('%Y%m%d') + '.csv'
    if os.path.isfile(tmpfile):
        return

    try:
        HKBuyback.BuybackSnapshot(180)
    except Exception as e:
        print(e)
    dailySnapshot = DailyScrapeHKExFilings()


    try:
        with open(tmpfile, encoding='utf-8') as f:
            tmpdata = [tuple(line) for line in csv.reader(f)]
        DailyUpdateFunds(tmpdata)
    except Exception as e:
        print(e)
    return

HKExFilingDailyBatch()
InitializeFundFilings()



#tmpfile = '/Users/shared/HKEx/DailyFilings/' + datetime.date.today().strftime('%Y%m%d') + '.csv'
#with open(tmpfile, encoding='utf-8') as f:
#    tmpdata = [tuple(line) for line in csv.reader(f)]
#DailyUpdateFunds(tmpdata)

# ScrapeHistoricalHKExFilings(5000)
# ScrapeHKExFilingsbyDates(datetime.datetime.strptime("01/03/2018", "%d/%m/%Y"), datetime.datetime.strptime("24/04/2018", "%d/%m/%Y"))
#UpdateFundsfromTickerFile(HKFunds, "00000")
#DailyScrapeHKExFilings()
#UpdateFilingList2Scrape()
#ScrapeOneHKExFiling("00000", "http://sdinotice.hkex.com.hk/filing/di/NSForm2.aspx?fn=321838", "000000", "000000", "000000")
#ScrapeOneHKExFiling("00000", "http://sdinotice.hkex.com.hk/di/NSForm2.aspx?fn=CS20180328E00109", "000000", "000000", "000000")
# This really should be run just once to create the FullScrapingList when first started collecting data.
# The FullScrapingList is subsequently updated in daily runs
def ScrapeHistoricalFilingList():
    scrapingdts = pd.bdate_range(start=datetime.datetime.strptime("01/04/2003", "%d/%m/%Y"), end=datetime.datetime.now())
    tgtFile = "/Users/Shared/HKEx/FullScrapingList.csv"
    j = len(scrapingdts)-1
    i = max(j-10, 0)
    while (i!=0):
        print(scrapingdts[i], scrapingdts[j])
        scrapelist = RetrieveFilingsList("http://sdinotice.hkex.com.hk/filing/di/NSAllFormDateList.aspx?sa1=da&scsd=",scrapingdts[i], scrapingdts[j])
        scrapelist += RetrieveFilingsList("http://sdinotice.hkex.com.hk/di/NSAllFormDateList.aspx?sa1=da&scsd=",
                                         scrapingdts[i], scrapingdts[j])
        #Add scrapelist to tgtFile
        #FTM.SafeSaveData(tgtFile,scrapelist)
        j -= 10
        i = max(j-10, 0)
    return()











