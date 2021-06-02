#!/usr/local/bin/python3


## https://adesquared.wordpress.com/2013/06/16/using-python-beautifulsoup-to-scrape-a-wikipedia-table/ 


from bs4 import BeautifulSoup
import urllib.request
from urllib.request import urlopen
import MySQLdb as MSD
from glob import glob
import re
import datetime
import os
import io
import csv
import locale
from dateutil import parser
import multiprocessing
from datetime import date
from bs4 import UnicodeDammit
import xml.etree.ElementTree as ET
from pytz import timezone
import pytz
from bdateutil import relativedelta
import holidays
import time
from dateutil import parser


isdst_now_in = lambda zonename: bool(datetime.datetime.now(pytz.timezone(zonename)).dst())

locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )

#1. Find Form 4 links from crawler.idx
# 1.1 Extract filing type, date, and link from crawler.idx
# 1.2 If it is a form 4, keep date, link, set hasXML and isScraped to false
# 1.3 Batch update the Form4 links database use INSERT IGNORE 
#2. Follow Form 4 links to scrape xml filings
# 2.1 Find xml link
# 2.2 If xml link exists:
#   2.2.1 Scrape xml
#   2.2.2 Save data to database
#   2.2.3 mark the link as processed
# 2.3 If no xml link exists:
#   2.3.1 marke the link as processed

#create the Form 4 Links table
#
def CreateLinksTable():
    db = None
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        db = None

    if db:
        try:
            cursor = db.cursor()
            sqlstr = """CREATE TABLE IF NOT EXISTS Links (
                rprtdt date,
                htmllink varchar(512),
                hasXML bool,
                isScraped bool,
                hasNonDeriv bool,
                primary key (htmllink)
                )"""
            cursor.execute(sqlstr)
            db.commit()
            db.close()
        except Exception as e:
            print(e)

def CreateNDTransactsTable():
    db = None
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        db = None

    if db:
        try:
            cursor = db.cursor()
            sqlstr = """CREATE TABLE IF NOT EXISTS ndTransacts (
                htmllink varchar(512),
                issuerCIK varchar(10),
                issuerTicker varchar(10),
                ownerCIK varchar(10),
                ownerName varchar(256),
                isDir bool,
                isOfficer bool,
                isPct10 bool,
                isOther bool,
                OfficerTitle varchar(50),
                secType varchar(200),
                transactDt date,
                transactCode varchar(1),
                swapUsed bool,
                transactShares float,
                transactPx float,
                transactADcode varchar(1),
                posttransactAmt float,
                ownershipNature varchar(1),
                primary key (htmllink, issuerCIK, ownerCIK, secType, transactDt, transactCode, transactShares, transactPx, transactADcode, posttransactAmt)
                )"""
            cursor.execute(sqlstr)
            db.commit()
            db.close()
        except Exception as e:
            print(e)

#CreateLinksTable()
#CreateNDTransactsTable()

def AddLinks2SQL(linksdata):
    if not linksdata:
        return(None)

    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)

    cursor = db.cursor()
    for line in linksdata:
            rprtdt = line[0]
            htmllink = line[1]
            hasXML = line[2]
            isScraped = line[3]
            hasNonDeriv = line[4]

            try:
                cursor.execute("""INSERT IGNORE INTO Links (rprtdt, htmllink, hasXML, isScraped, hasNonDeriv) VALUES (%s, '%s', %s, %s, %s)""" %(rprtdt, htmllink, hasXML, isScraped, hasNonDeriv))
            except Exception as e:
                #print(e)
                pass
    db.commit()
    db.close()
    return(None)


def AddLinks2SQLParallel(linksdata,  NumOfThreads=16):
    if not linksdata:
        return None

    linksdata = list(set(linksdata))
    if (NumOfThreads < 1):
        NumOfThreads = 2
    if (NumOfThreads > 16):
        NumOfThreads = 16
    threads = [None] * NumOfThreads
    results = [0] * NumOfThreads
    out_q = multiprocessing.Queue()
    tickerlistLen = len(linksdata) // NumOfThreads

    for i in range(len(threads)):
        startIdx = (i - 1) * tickerlistLen
        if (i == (len(threads) - 1)):
            endIdx = len(linksdata)
        else:
            endIdx = i * tickerlistLen
        threads[i] = multiprocessing.Process(target=AddLinks2SQL,
                                             args=(linksdata[startIdx:endIdx],))
        threads[i].start()

    for i in range(len(threads)):
        threads[i].join()

    return (threads)


def Crawler2LinksTable(filename):
    i = 0
    allLinks = []
    with open(filename, errors="surrogateescape") as f:
        for line in f:
            formtype = line[62:65]
            formtype = formtype.strip()
            if ((formtype == '4') or (formtype == '4/A')):
                rprtdt = line[86:96]
                htmllink = line[97:]
                rprtdt = rprtdt.strip()
                rprtdt = re.sub("-", '', rprtdt)
                htmllink = htmllink.strip()
                allLinks += [(rprtdt, htmllink, True, False, True)]

    if allLinks:
        AddLinks2SQLParallel(allLinks)
                

#Crawler2LinksTable('/Users/Shared/SEC/Crawlers/2014QTR1.idx')
def AddAllLinks():
    startY = 1993
    endY = date.today().year+1
    qtrs =['QTR1', 'QTR2', 'QTR3', 'QTR4']
    for i in range(startY, endY):
        for curQ in qtrs:
            tgtFile = '/Users/Shared/SEC/Crawlers/' + str(i) + curQ + '.idx'
            print(tgtFile)
            Crawler2LinksTable(tgtFile)


def SetXML2False(url):
    if not url:
        return(None)

    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)

    cursor = db.cursor()
    sql = """UPDATE Links SET hasXML = False where htmllink = '%s'""" %(url)
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)

    db.commit()
    db.close()
    return

def SetNonDeriv2False(url):
    if not url:
        return(None)

    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)

    cursor = db.cursor()
    sql = """UPDATE Links SET hasNonDeriv = False where htmllink = '%s'""" %(url)
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)

    db.commit()
    db.close()
    return

def SetIsScraped2True(url):
    if not url:
        return(None)

    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)

    cursor = db.cursor()
    sql = """UPDATE Links SET isScraped = True where htmllink = '%s'""" %(url)
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)

    db.commit()
    db.close()
    return


#For a given URL for a Form 4 filing, check if
def CheckXML(url):
    if not url:
        return(None)

    try:
        filingpage = urlopen(url)
        soup = BeautifulSoup(filingpage)
        xmllink = None
        for a in soup.find_all('a'):
            if ('Archives' in a['href']) and ('xml' in a.getText()):
                xmllink = 'http://www.sec.gov' + a['href']
                nonDerivTransacts = CheckNonDeriv(xmllink)
                if (len(nonDerivTransacts) == 0) or (nonDerivTransacts is None) :
                    SetNonDeriv2False(url)
                return(xmllink)
        if not xmllink:
            SetXML2False(url)
            return(None)
    except Exception as e:
        print(e)
        return(None)

def CheckNonDeriv(xmlurl):
    if not xmlurl:
        return(None)

    try:
        opener = urllib.request.build_opener()
        tree = ET.parse(opener.open(xmlurl))
        ndTransacts = tree.findall('nonDerivativeTable/nonDerivativeTransaction')
        return(ndTransacts)
    except Exception as e:
        print(e)
        return(None)


def CheckXMLBatch(urls):
    for url in urls:
        CheckXML(url)

def CheckXMLParallel(urls, NumOfThreads=4):
    if not urls:
        return None

    urls = list(set(urls))
    if (NumOfThreads < 1):
        NumOfThreads = 2
    if (NumOfThreads > 32):
        NumOfThreads = 32
    threads = [None] * NumOfThreads
    results = [0] * NumOfThreads
    out_q = multiprocessing.Queue()
    tickerlistLen = len(urls) // NumOfThreads

    for i in range(len(threads)):
        startIdx = i * tickerlistLen
        if (i == (len(threads) - 1)):
            endIdx = len(urls)-1
        else:
            endIdx = (i+1) * tickerlistLen-1
        threads[i] = multiprocessing.Process(target=CheckXMLBatch,
                                             args=(urls[startIdx:(endIdx+1)],))
        threads[i].start()

    for i in range(len(threads)):
        threads[i].join()

    return (threads)


def CheckAllXML():
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)

    sql = """select htmllink from Links where hasXML is True and hasNonDeriv is True and isScraped is False """
    cursor = db.cursor()
    try:
        urllist = cursor.execute(sql)
    except Exception as e:
        print(e)

    urlist = [line[0] for line in cursor]
    print(len(urlist))
    CheckXMLParallel(urlist)


#1. Check information in the Links table, skip if scraped or has no xml or has no non-derivatives transactions
#2. Run a CheckXML and skip if has no xml or no non-derivatives transaction
#3. Scraping
#4. Fill data in the non-derivatives table
#5. Update isScraped information in Links

def RetrieveScrapeInfo(url):
    if not url:
        return(None)

    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)


    sql = """select hasXML, hasNonDeriv, isScraped from Links where htmllink = '%s' """ %(url)
    cursor = db.cursor()
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
        return(None)

    result = cursor.fetchone()
    cursor.close()
    db.close()

    if result:
        return (result)
    else:
        return(None)

def Need2Scrape(url):
    ScrapeInfo = RetrieveScrapeInfo(url)
    if ScrapeInfo is None:
        return(False)

    hasXML = ScrapeInfo[0]
    hasNonDeriv = ScrapeInfo[1]
    isScraped = ScrapeInfo[2]

    if (isScraped) or (not hasXML) or (not hasNonDeriv):
        return(False)
    else:
        CheckXML(url)
        ScrapeInfo = RetrieveScrapeInfo(url)
        hasXML = ScrapeInfo[0]
        hasNonDeriv = ScrapeInfo[1]
        isScraped = ScrapeInfo[2]

        if (isScraped) or (not hasXML) or (not hasNonDeriv):
            return(False)
        else:
            return(True)


def ScrapeOneForm4(url):
    if not Need2Scrape(url):
        return(None)

    xmllink = None
    try:
        filingpage = urlopen(url)
        soup = BeautifulSoup(filingpage)
        for a in soup.find_all('a'):
            if ('Archives' in a['href']) and ('xml' in a.getText()):
                xmllink = 'http://www.sec.gov' + a['href']
                break;
    except Exception as e:
        print(e)
        return(None)

    if not xmllink:
        return(None)

    ndTransacts = []
    try:
        opener = urllib.request.build_opener()
        tree = ET.parse(opener.open(xmllink))
        ndTransacts = tree.findall('nonDerivativeTable/nonDerivativeTransaction')
    except Exception as e:
        print(e)
        return (None)

    if (len(ndTransacts) <= 0 ):
        return(None)

    issuerCIK = tree.find('issuer/issuerCik')
    issuerTicker = tree.find('issuer/issuerTradingSymbol')
    ownerCIK = tree.find('reportingOwner/reportingOwnerId/rptOwnerCik')
    ownerName = tree.find('reportingOwner/reportingOwnerId/rptOwnerName')
    ownerIsDir = tree.find('reportingOwner/reportingOwnerRelationship/isDirector')
    ownerIsOff = tree.find('reportingOwner/reportingOwnerRelationship/isOfficer')
    ownerIsPct10 = tree.find('reportingOwner/reportingOwnerRelationship/isTenPercentOwner')
    ownerIsOther = tree.find('reportingOwner/reportingOwnerRelationship/isOther')
    ownerTitle = tree.find('reportingOwner/reportingOwnerRelationship/officerTitle')

    issuerTickertxt=''
    issuerCIKtxt =''
    ownerCIKtxt = ''
    ownerNametxt = ''
    ownerTitletxt =''
    ownerIsDirtxt='0'
    ownerIsOfftxt='0'
    ownerIsPct10txt='0'
    ownerIsOthertxt='0'

    try:
        issuerCIKtxt = issuerCIK.text
        issuerTickertxt = issuerTicker.text
        ownerCIKtxt = ownerCIK.text
        ownerNametxt  = ownerName.text
        ownerTitletxt  = ownerTitle.text
        ownerIsDirtxt = ownerIsDir.text
        ownerIsOfftxt = ownerIsOff.text
        ownerIsPct10txt = ownerIsPct10.text
        ownerIsOthertxt = ownerIsOther.text
    except Exception as e:
        pass

    if ownerNametxt:
        ownerNametxt = re.sub('[^0-9a-zA-Z]+', ' ', ownerNametxt)
        ownerNametxt = re.sub('[\s+]', ' ', ownerNametxt)
        ownerNametxt = ownerNametxt.strip()

    if ownerTitletxt:
        ownerTitletxt = re.sub('[^0-9a-zA-Z]+', ' ', ownerTitletxt)
        ownerTitletxt = re.sub('[\s+]', ' ', ownerTitletxt)
        ownerTitletxt = ownerTitletxt.strip()

    if issuerTickertxt:
        issuerTickertxt = re.sub('[^0-9a-zA-Z\-\.\:]+', ' ', issuerTickertxt)
        issuerTickertxt = re.sub('[\s+]', ' ', issuerTickertxt)
        issuerTickertxt = issuerTickertxt.strip()

    isDir = (ownerIsDirtxt=='1')
    isOff = (ownerIsOfftxt=='1')
    isPct10 = (ownerIsPct10txt=='1')
    isOther = (ownerIsOthertxt=='1')
#    rprtInfo = (url, issuerCIKtxt, issuerTickertxt ,ownerCIKtxt, ownerNametxt, ownerIsDirtxt=='1',ownerIsOfftxt=='1', ownerIsPct10txt=='1',  ownerIsOthertxt=='1', ownerTitletxt )

    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return (None)

    FailedTransact = False
    for Transact in ndTransacts:
        secType = ''
        TransactDt =''
        TransactCode = ''
        SwapUsed ='0'
        TransactShares ='0'
        TransactPx ='0'
        TransactADCode =''
        PostTransactAmount='0'
        OwnershipNature=''
        try:
            secType = Transact.find('securityTitle/value').text
            TransactDt = Transact.find('transactionDate/value').text
            TransactCode = Transact.find('transactionCoding/transactionCode').text
            SwapUsed = Transact.find('transactionCoding/equitySwapInvolved').text
            TransactShares = Transact.find('transactionAmounts/transactionShares/value').text
            TransactPx = Transact.find('transactionAmounts/transactionPricePerShare/value').text
            TransactADCode = Transact.find('transactionAmounts/transactionAcquiredDisposedCode/value').text
            PostTransactAmount= Transact.find('postTransactionAmounts/sharesOwnedFollowingTransaction/value').text
            OwnershipNature = Transact.find('ownershipNature/directOrIndirectOwnership/value').text
        except Exception as e:
            pass

        isSwap = (SwapUsed=='1')

        if secType:
            secType = re.sub('[^0-9a-zA-Z]+', ' ', secType)
            secType = re.sub('[\s+]', ' ', secType)
            secType = secType.strip()

        if OwnershipNature:
            OwnershipNature = re.sub('[^0-9a-zA-Z]+', ' ', OwnershipNature)
            OwnershipNature = re.sub('[\s+]', ' ', OwnershipNature)
            OwnershipNature = OwnershipNature.strip()

        #TransactDt = re.sub("-", '',TransactDt)
        if TransactDt:
            try:
                tmpdt = parser.parse(TransactDt)
                TransactDt = tmpdt.strftime('%Y%m%d')
            except Exception as e:
                print(e)

        TransactSharesNum = 0
        TransactPxNum = 0
        PostTransactAmountNum = 0
        try:
            TransactSharesNum = locale.atof(TransactShares)
            TransactPxNum = locale.atof(TransactPx)
            PostTransactAmountNum = locale.atof(PostTransactAmount)
        except Exception as e:
            pass
#        transactInfo = (secType,TransactDt, isSwap,  TransactCode, TransactSharesNum, TransactPxNum, TransactADCode, PostTransactAmountNum, OwnershipNature)
#        row = rprtInfo + transactInfo

        try:
            cursor = db.cursor()
            sql = """ REPLACE INTO ndTransacts(htmllink,issuerCIK, issuerTicker, ownerCIK,ownerName, isDir, isOfficer, isPct10, isOther, OfficerTitle, secType, transactDt, transactCode, swapUsed, transactShares, transactPx, transactADcode, postTransactAmt, ownershipNature) values('%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s', '%s', %s, '%s', %s, %s, %s, '%s', %s, '%s')""" %(url, issuerCIKtxt, issuerTickertxt, ownerCIKtxt, ownerNametxt, isDir,  isOff, isPct10, isOther, ownerTitletxt, secType, TransactDt, TransactCode,  isSwap,TransactSharesNum, TransactPxNum, TransactADCode, PostTransactAmountNum,  OwnershipNature)
            cursor.execute(sql)
            db.commit()
        except Exception as e:
            print(e)
            print(url)
            FailedTransact = True

    db.close()
    if not FailedTransact:
        SetIsScraped2True(url)


def ScrapeForm4s(urllist):
    for url in urllist:
        try:
            ScrapeOneForm4(url)
        except Exception as e:
            print(url)
            pass

def ScrapeF4Parallel(urls, NumOfThreads=4, waittime=-1):
    if not urls:
        return None

    urls = list(set(urls))
    if (NumOfThreads < 1):
        NumOfThreads = 2
    if (NumOfThreads > 32):
        NumOfThreads = 32
    threads = [None] * NumOfThreads
    results = [0] * NumOfThreads
    out_q = multiprocessing.Queue()
    tickerlistLen = len(urls) // NumOfThreads

    for i in range(len(threads)):
        startIdx = i * tickerlistLen
        if (i == (len(threads) - 1)):
            endIdx = len(urls)-1
        else:
            endIdx = (i+1) * tickerlistLen -1
        threads[i] = multiprocessing.Process(target=ScrapeForm4s,
                                             args=(urls[startIdx:(endIdx+1)],))
        threads[i].start()

    if (waittime>0):
        for i in range(len(threads)):
            threads[i].join(waittime)
    else:
        for i in range(len(threads)):
            threads[i].join()
        for i in range(len(threads)):
            if threads[i].is_alive():
                threads[i].terminate()

    return(threads)


def ScrapeAllForm4s(startdt='1993-01-01', enddt = '2100-12-31', numofProcesses=16, waittime=-1):
    print(startdt)
    print(enddt)
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='Form4')
    except Exception as e:
        print(e)
        return(None)

    sql = """select htmllink from Links where hasXML is True and hasNonDeriv is True and isScraped is False  and rprtDt >= '%s' and rprtDt <= '%s' """ %(startdt, enddt)
    cursor = db.cursor()
    try:
        urllist = cursor.execute(sql)
    except Exception as e:
        print(e)

    urlist = [line[0] for line in cursor]
    print(len(urlist))
    db.close()
    ScrapeF4Parallel(urlist, numofProcesses, waittime)


def DownloadCurrentCrawler():
    srcFile = 'https://www.sec.gov/Archives/edgar/full-index/crawler.idx'
    tgtFile = '/Users/Shared/SEC/Crawlers/Crawler.idx'
    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
        urllib.request.urlcleanup()
    except Exception as e:
        print(tgtFile)
        print(e)

    Crawler2LinksTable(tgtFile)

#DownloadCurrentCrawler()

#1. Download current crawler.idx
#2. Add crawler.idx to Links
#3. Scrape Form 4s of last 3 days;
#4. Scrape Form 4s of last 3 months
#5. Scrape all Forms 4s
def UpdateForm4s():
    DownloadCurrentCrawler()

    endDt = date.today()
    startDt = endDt + datetime.timedelta(days=-30)
    NumOfThreads=multiprocessing.cpu_count()
    if (NumOfThreads >=8):
        NumOfThreads -=4

    ScrapeAllForm4s(startDt.strftime("%Y-%m-%d"), endDt.strftime("%Y-%m-%d"), NumOfThreads, -1)
    #startDt = endDt + datetime.timedelta(days=-90)
    #ScrapeAllForm4s(startDt.strftime("%Y-%m-%d"), endDt.strftime("%Y-%m-%d"), 8, -1)
    ScrapeAllForm4s('1993-01-01', '2100-12-31', NumOfThreads, 10)
    return


# tmp = "http://www.sec.gov/Archives/edgar/data/36995/0001127602-07-003981-index.htm"
# tmp = "http://www.sec.gov/Archives/edgar/data/1005862/0001005862-96-000002-index.htm"
# tmpxml = 'http://www.sec.gov/Archives/edgar/data/1175505/000120919116124855/doc4.xml'  ## with nonderivatives transaction
#scrapeurl = "http://www.sec.gov/Archives/edgar/data/1057058/0001057058-16-000067-index.htm"
# CheckXML(tmp)
# SetXML2False(tmp)
#CheckNonDeriv(tmpxml)
#CheckAllXML()
#ScrapeOneFiling(scrapeurl)
#ScrapeAllForm4s('2000-01-01', '2016-12-31', 16, -1)




while True:
    backgroundProcesses = UpdateForm4s()
    break
