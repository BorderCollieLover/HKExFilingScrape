
##samples:
##http://stackoverflow.com/questions/26446363/python-mysql-create-and-name-new-table-using-variable-name
##https://dev.mysql.com/doc/connector-python/en/connector-python-example-ddl.html

import MySQLdb as MSD
from glob import glob
import re
import datetime
import os
import io
import gzip
import csv
import locale
import multiprocessing
from dateutil import parser
import time
from pytz import timezone
import pytz


isdst_now_in = lambda zonename: bool(datetime.datetime.now(pytz.timezone(zonename)).dst())

locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )

def CreateVOITable(db, fileticker):
    if db and fileticker:
        try:
            cursor = db.cursor()
            sqlstr = """CREATE TABLE IF NOT EXISTS %s (
                isCall bool,
                strike float,
                expiry date,
                tradedt date, 
                lastpr float, 
                bid float,
                ask float,
                prchg float, 
                pctchg float, 
                volume integer,
                openint integer,
                iv float,
                primary key (isCall, strike, expiry, tradedt)
                )""" %(fileticker)
            cursor.execute(sqlstr)
            db.commit()
        except Exception as e:
            pass

def CreateOHLCTable(db, fileticker):
    if db and fileticker:
        try:
            cursor = db.cursor()
            sqlstr = """CREATE TABLE IF NOT EXISTS %s (
                tradedt date,
                OpPr float,
                HiPr float,
                LoPr float,
                ClPr float,
                volume integer,
                AdPr float,
                primary key (tradedt)
                )""" %(fileticker)
            cursor.execute(sqlstr)
            db.commit()
        except Exception as e:
            pass


def PopulateOptionTable(db, fileticker, optData):
    if not (db and optData and fileticker):
        return

    cursor = db.cursor()
    newData = []
    for line in optData:
        strike=locale.atof(line[0])
        try:
            lastpr = locale.atof(line[2])
        except Exception as e:
            lastpr = "NULL"
        try:
            bid = locale.atof(line[3])
        except Exception as e:
            bid = "NULL"
        try:
            ask = locale.atof(line[4])
        except Exception as e:
            ask = "NULL"

        try:
            prchg = locale.atof(line[5])
        except Exception as e:
            prchg = "NULL"


        try:
            pctchg = line[6]
        except Exception as e:
            pctchg = "NULL"


        try:
            pctchg = locale.atof(pctchg[:len(pctchg)-1])
        except Exception as e:
            pctchg = "NULL"
        try:
            vol = locale.atoi(line[7])
        except Exception as e:
            vol = "NULL"
        try:
            openint = locale.atoi(line[8])
        except Exception as e:
            openint = "NULL"

        iv = line[9]
        try:
            iv = locale.atof(iv[:len(iv)-1])
        except Exception as e:
            iv = "NULL"

        tradedt = line[10]

        contractid = line[1]
        tmp = re.sub(r'^[a-zA-Z_]+','',contractid)
        expirydt = '20' + tmp[:6]
        if (tmp[6] == 'C'):
            isCall=True
        else:
            isCall=False

        newData += [(isCall, strike, expirydt, tradedt, lastpr, bid, ask, prchg, pctchg, vol, openint, iv)]
        #print(isCall, strike, expirydt, tradedt, lastpr, bid, ask, prchg, pctchg, vol, openint, iv)
        try:
            cursor.execute("""INSERT IGNORE INTO %s (isCall, strike, expiry, tradedt, lastpr, bid, ask, prchg, pctchg, volume, openint, iv) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" %(fileticker, isCall, strike, expirydt, tradedt, lastpr, bid, ask, prchg, pctchg, vol, openint, iv))
            #query = """INSERT IGNORE INTO %s (isCall, strike, expiry, tradedt, lastpr, bid, ask, prchg, pctchg, volume, openint, iv) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            #cursor.execute(query, (fileticker, isCall, strike, expirydt, tradedt, lastpr, bid, ask, prchg, pctchg, vol, openint, iv))
        except Exception as e:
            print(e)
            pass

    db.commit()
    return(newData)

def PopulateOHLCTable(db, fileticker, ohlcDat):
    if not (db and fileticker and ohlcDat):
        return

    cursor = db.cursor()
    for line in ohlcDat:
        tradedt = re.sub("-",'',line[0])
        OpPr = locale.atof(line[1])
        HiPr = locale.atof(line[2])
        LoPr = locale.atof(line[3])
        ClPr = locale.atof(line[4])
        #Volume = locale.atoi(line[5])
        Volume = line[5]
        AdPr = locale.atof(line[6])
        # print(tradedt)
        # print(OpPr)
        # print(HiPr)
        # print(LoPr)
        # print(ClPr)
        # print(Volume)
        # print(AdPr)

        try:
            cursor.execute("""REPLACE INTO %s (tradedt, OpPr, HiPr, LoPr, ClPr, Volume, AdPr) VALUES (%s, %s, %s, %s, %s, %s, %s)""" %(fileticker, tradedt, OpPr, HiPr, LoPr, ClPr, Volume, AdPr))
        except Exception as e:
            pass

    db.commit()
    return(ohlcDat)


def ProcessHistOptionFile(db, fileticker, inputFile):
    optData = []
    try:
        with io.TextIOWrapper(gzip.open(inputFile, 'r')) as csvfile:
            optData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)
    PopulateOptionTable(db, fileticker, optData)

#add trade date (filename) to the option data so that it conforms to data rows in HistOptionData    
def ProcessOptionFile(db, fileticker, inputFile):
    fileDt = os.path.splitext(os.path.basename(inputFile))[0]
    #print(fileDt)
    if ('csv' in  fileDt):
        fileDt = os.path.splitext(fileDt)[0]
    #print(fileDt)
    fileDtStr = fileDt
    fileDt = datetime.datetime.strptime(fileDt, '%Y%m%d').date()

    optData = []
    try:
        with io.TextIOWrapper(gzip.open(inputFile, 'r')) as csvfile:
            optData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)

    newData =[]
    for line in optData:
        newData += [(line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], fileDtStr,line[10])]

    return(PopulateOptionTable(db, fileticker, newData))

##db = MSD.connect(user='root', passwd='cdefgh12', db='OptionData')
##x = ProcessOptionFile(db, 'VNET', '/Users/Shared/OptionData/VNET/20160516.csv.gz')
##test='VNET'
##cursor = db.cursor()
##stmt = "SHOW TABLES LIKE '%s'"%test
##print(stmt)
##cursor.execute(stmt)
##result = cursor.fetchone()
##print(result)
##test = '20160516'
##test2 = 100
##isCall = True
##strike = 10.5
##expirydt="20160620"
##tradedt="20160516"
##lastpr = 1.1
##bid = 0.9
##ask = 1.4
##prchg = 0
##pctchg = 0
##volume = 50
##openint = 1000
##iv = 35.5
##cursor.execute("""INSERT IGNORE INTO VNET VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""%(isCall, strike, expirydt, tradedt, lastpr, bid, ask, prchg, pctchg, volume, openint, iv))
##db.commit()
##db.close()
#print(x)


def SaveOptionData2MySQL(ticker, updateOnly=True):
    db = None
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='OptionData')
    except Exception as e:
        print(e)
        db = None

    if not db:
        print('Failed to connect to SQL server')
        return

    fileticker = re.sub("\^", '', ticker)
    if not re.match('^[a-zA-Z]+$', fileticker):
        return

    stmt = "SHOW TABLES LIKE '%s'"%fileticker
    result = False
    try:
        cursor = db.cursor()
        cursor.execute(stmt)
        result = cursor.fetchone()
    except Exception as e:
        pass
    if (not result) or (not updateOnly):
        #print('Table does not exsit for ' + ticker + ' or full update')

        CreateVOITable(db, fileticker)

        try:
            cursor = db.cursor()
            cursor.execute(stmt)
            result = cursor.fetchone()
        except Exception as e:
            pass

        if result:
            HistOptionFiles = glob('/Users/Shared/HistOptionData/'+fileticker+'/*.csv.gz')
            for datafile in HistOptionFiles:
                ProcessHistOptionFile(db, fileticker, datafile)

            OptionFiles = glob('/Users/Shared/OptionData/'+fileticker+'/*.csv.gz')
            for datafile in OptionFiles:
                ProcessOptionFile(db, fileticker, datafile)
    else:
        #print('Table exists for ' + ticker + ' , update only')
        #it seems that update time from information_schema is null so we check for the last tradedt
        stmt = """SELECT MAX(tradedt) from %s""" %fileticker
        try:
            cursor.execute(stmt)
        except Exception as e:
            pass
        result = cursor.fetchone()
        checkdt = result[0]
        OptionFiles = glob('/Users/Shared/OptionData/'+fileticker+'/*.csv.gz')
        for datafile in OptionFiles:
            fileDt = os.path.splitext(os.path.basename(datafile))[0]
            # print(fileDt)
            if ('csv' in fileDt):
                fileDt = os.path.splitext(fileDt)[0]
            # print(fileDt)
            fileDt = datetime.datetime.strptime(fileDt, '%Y%m%d').date()
            if (checkdt < fileDt):
                #print('file to process ' + datafile)
                ProcessOptionFile(db, fileticker, datafile)


    db.close()

#SaveOptionData2MySQL('CONN')

def BatchSaveOptionData(tickerlist, updateOnly=True):
    for ticker in tickerlist:
        #print(ticker)
        try:
            SaveOptionData2MySQL(ticker, updateOnly)
        except Exception as e:
            print(ticker)
            print(e)


def BatchSaveOptionDataParallel(tickerlist,  NumOfThreads=2, updateOnly=True, waittime=-1 ):
    if not tickerlist:
        return None

    tickerlist = list(set(tickerlist))
    if (NumOfThreads < 1):
        NumOfThreads = 2
    if (NumOfThreads > 8):
        NumOfThreads = 8
    threads = [None] * NumOfThreads
    results = [0] * NumOfThreads
    out_q = multiprocessing.Queue()
    tickerlistLen = len(tickerlist) // NumOfThreads

    for i in range(len(threads)):
        startIdx = i  * tickerlistLen
        if (i == (len(threads) - 1)):
            endIdx = len(tickerlist)
        else:
            endIdx = (i+1) * tickerlistLen-1
        threads[i] = multiprocessing.Process(target=BatchSaveOptionData,
                                             args=(tickerlist[startIdx:(endIdx+1)], updateOnly))
        threads[i].start()

        # a positive wait time will allow the calling process to proceed after the specified time
        # a negative wait time (default) will block the process until all parallel prcesses finish
    if (waittime > 0):
        for i in range(len(threads)):
            threads[i].join(waittime)
    else:
        for i in range(len(threads)):
            threads[i].join()
        for i in range(len(threads)):
            if threads[i].is_alive():
                threads[i].terminate()

    return (threads)


#Unlike Option data, where there is no chance to modify, OHLC data always faces the problem of modification (of the Adjusted column) after split/div
#Might need to check a few Ad column and if not matching, then flush all Ad data and fill in again; there might then be the issue of partial update........
#The advantage of flush all Ad data and then refill is that we might somehow overcome the missing historical data issue, when patches of historical data is missing when re-downloading
#Needs more work
def SaveOHLC2MySQL(ticker):
    fileticker = re.sub("\^", '', ticker)
    if not re.match('^[a-zA-Z]+$', fileticker):
        return

    #load OHLC data
    ohlcfile = '/Users/Shared/Price Data/Daily/' + fileticker + '.csv'
    ohlcDat = None
    if os.path.isfile(ohlcfile):
        try:
            with open(ohlcfile, encoding='utf-8') as f:
                ohlcDat = [tuple(line) for line in csv.reader(f)]
                ohlcDat = ohlcDat[1:]
        except Exception as e:
            print('OHLC file does not exist for ' + ticker)
            return(None)
    else:
        print('OHLC file does not exist for ' + ticker)
        return(None)

    db = None
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='OHLC')
    except Exception as e:
        print(e)
        db = None

    if not db:
        print('Failed to connect to SQL server')
        return(None)


    try:
        stmt = "SHOW TABLES LIKE '%s'"%fileticker
        cursor = db.cursor()
        cursor.execute(stmt)
        result = cursor.fetchone()
    except Exception as e:
        pass
        result=False

    if not result:
        #print('OHLC table does not exsit for ' + ticker + ' or full update')
        CreateOHLCTable(db, fileticker)

        try:
            stmt = "SHOW TABLES LIKE '%s'" % fileticker
            cursor = db.cursor()
            cursor.execute(stmt)
            result = cursor.fetchone()
        except Exception as e:
            pass
            result = False

    else:
        #print('Table exists for ' + ticker + ' , update only')
        #it seems that update time from information_schema is null so we check for the last tradedt
        stmt = """SELECT MAX(tradedt) from %s""" %fileticker
        cursor.execute(stmt)
        result = cursor.fetchone()
        checkdt = result[0]
        # print(checkdt)
        newDat = [line for line in ohlcDat if parser.parse(line[0]).date() > checkdt]
        if (len(newDat) <= 0):
            return(None)
        oldDat = ohlcDat[:(len(ohlcDat)-len(newDat))]
        # print(oldDat[len(oldDat)-1])
        numCheckRows = 10
        stmt =  """SELECT * from %s ORDER BY tradedt DESC LIMIT %s """ %(fileticker, numCheckRows)
        cursor.execute(stmt)
        CheckRows = cursor.fetchall()
        # print(CheckRows)
        # print(newDat)
        CheckFailed = False
        tol = 1e-3
        for lineIdx, line in enumerate(CheckRows):
            oldDatLine = oldDat[len(oldDat)-1-lineIdx]
            oldDatDt = oldDatLine[0]
            oldDatAdPr = oldDatLine[6]
            if (line[6] is None):
                CheckFailed=True
                break;
            else:
                CheckFailed =  (line[0] != parser.parse(oldDatDt).date()) or (abs(line[6] - locale.atof(oldDatAdPr)) > tol)
                if CheckFailed:
                    break;
        if CheckFailed:
            #set the AdPr column to NaN
            stmt = """Update %s SET AdPr = NULL""" %(fileticker)
            try:
                cursor.execute(stmt)
            except Exception as e:
                pass
            db.commit()
        else:
            ohlcDat = newDat

        #check for 10 Adjusted Closes, if they match perfectly, then update with only new data
        #If not perfect, then flush all Adjusted and update with all OHLC data
        #If checkdt not in ohlcdata: flush Ad and update with full data
        #If checkdt in ohlcdata, find the 10-d before data and retrieve ohlc from SQL server from that day
        #If numbers of rows doesn't match, flush Ad and update with full data
        #If any Adjusted doesn't match, flush Ad and update with full data; else update with only new data
        #Will use insert and update on duplicate, nevertheless flush Adjusted column in case there are data patches mismatch and weird Adjusted causes confusion elsewhere

    #Populate the OHLC table
    if result:
        PopulateOHLCTable(db, fileticker, ohlcDat)
    db.close()

#SaveOHLC2MySQL('BLL')
def BatchSaveOHLC(tickerlist):
    for ticker in tickerlist:
        #print(ticker)
        try:
            SaveOHLC2MySQL(ticker)
        except Exception as e:
            print(ticker)
            print(e)


def BatchSaveOHLCParallel(tickerlist,  NumOfThreads=2, waittime=-1 ):
    if not tickerlist:
        return None

    tickerlist = list(set(tickerlist))
    if (NumOfThreads < 1):
        NumOfThreads = 2
    if (NumOfThreads > 8):
        NumOfThreads = 8
    threads = [None] * NumOfThreads
    results = [0] * NumOfThreads
    out_q = multiprocessing.Queue()
    tickerlistLen = len(tickerlist) // NumOfThreads

    for i in range(len(threads)):
        startIdx = i* tickerlistLen
        if (i == (len(threads) - 1)):
            endIdx = len(tickerlist)
        else:
            endIdx = (i+1) * tickerlistLen -1
        threads[i] = multiprocessing.Process(target=BatchSaveOHLC,
                                             args=(tickerlist[startIdx:(endIdx+1)],))
        threads[i].start()

        # a positive wait time will allow the calling process to proceed after the specified time
        # a negative wait time (default) will block the process until all parallel prcesses finish
    if (waittime > 0):
        for i in range(len(threads)):
            threads[i].join(waittime)
    else:
        for i in range(len(threads)):
            threads[i].join()
        for i in range(len(threads)):
            if threads[i].is_alive():
                threads[i].terminate()

    return (threads)

#SaveOHLC2MySQL("MOMO")

def Time2NextRun():
    currDt = datetime.datetime.now(timezone('EST'))
    RunHour = 23
    if isdst_now_in("America/New_York"):
        RunHour -=1
    if (currDt.hour >= RunHour)  :
        nextDt = currDt + datetime.timedelta(days=1)
    else:
        nextDt = currDt
    nextDt = nextDt.replace(hour=RunHour, minute=30)
    print('wait until')
    print(nextDt)
    print((nextDt - currDt).total_seconds())
    return((nextDt - currDt).total_seconds())



OptionMonitorList = [ 'YY', 'NOAH', '^VIX']
#tickerfiles = ['ChineseFundTickers', 'positions']
tickerfiles = [ 'positions']

tickers = []

optDir = '/Users/shared/OptionData/'
allOptTickers = os.listdir(optDir)
#print(allOptTickers)

def RetrieveAllDataBaseTickers(dbname):
    try:
        db = MSD.connect(user='root', passwd='cdefgh12')
    except Exception as e:
        print(e)
        return(None)

    sql = """select table_name from information_schema.tables where table_schema= '%s' """ %(dbname)

    cursor = db.cursor()
    try:
        urllist = cursor.execute(sql)
    except Exception as e:
        print(e)

    tickers =  [item[0] for item in cursor]
    db.close()
    print(len(tickers))
    return(tickers)

def CleanEmptyTables(dbname):
    tickers = RetrieveAllDataBaseTickers(dbname)

    if tickers:
        try:
            db = MSD.connect(user='root', passwd='cdefgh12', db=dbname)
        except Exception as e:
            print(e)
            return (None)

        for ticker in tickers:
            sql = """select count(*) from %s """ %(ticker)

            cursor=db.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                result = result[0]
                cursor.close()
            except Exception as e:
                print(e)
                pass

            #print(ticker)
            #print(result)
            if (result <=1):
                sql = """Drop Table %s """ %(ticker)
                cursor = db.cursor()
                cursor.execute(sql)
                print(ticker)
                print(result)
        db.commit()
        db.close()

def CleanStaleTables(dbname):
    tickers = RetrieveAllDataBaseTickers(dbname)

    if tickers:
        try:
            db = MSD.connect(user='root', passwd='cdefgh12', db=dbname)
        except Exception as e:
            print(e)
            return (None)

        for ticker in tickers:
            sql = """select max(tradedt) from %s """ %(ticker)

            cursor=db.cursor()
            try:
                cursor.execute(sql)
                result = cursor.fetchone()
                checkdt = result[0]
                cursor.close()
            except Exception as e:
                print(e)
                pass

            #print(ticker)
            #print(result)
            #print(ticker, checkdt, (datetime.date.today()-checkdt),  (datetime.date.today()-checkdt).days)
            if ((datetime.date.today()-checkdt).days > 20):
                sql = """Drop Table %s """ %(ticker)
                cursor = db.cursor()
                cursor.execute(sql)
                print(ticker)
                print(result)


        db.commit()
        db.close()




while True:
    for tickerfilename in tickerfiles:
        curTickerFile = '/Users/Shared/SC13Monitor/Lists/' + tickerfilename + '.csv'
        try:
            with open(curTickerFile, encoding='utf-8') as csvfile:
                curTickers = [tuple(line) for line in csv.reader(csvfile)]
            tickers = list(set(tickers + curTickers))
        except Exception as e:
            print(e)
    if tickers:
        tickers = [line[0] for line in tickers]

    OptionMonitorList += tickers
    OptionMonitorList = list(set(OptionMonitorList) - {"EEM", "IWM", "SPY", "IBB", "QQQ", "GLD", "IBB"})

    #NumOfThreads=multiprocessing.cpu_count()
    #if (NumOfThreads >=8):
    #    NumOfThreads -=4
    NumOfThreads = 2

    #print(OptionMonitorList)

    BatchSaveOptionDataParallel(OptionMonitorList, NumOfThreads)
    BatchSaveOHLCParallel(OptionMonitorList, NumOfThreads=NumOfThreads)
    CleanEmptyTables('OptionData')
    CleanEmptyTables('OHLC')
    CleanStaleTables('OptionData')
    CleanStaleTables('OHLC')
    break

