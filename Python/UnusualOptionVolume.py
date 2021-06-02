

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
import numpy as np


##Scan the option database for unusual volume
##It seems that MySQL server becomes unstable after over 1000 option tables so I will not proceed along this direction for now


#Get all the Option datatable from the database
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


def RetrieveLatestOptDt():
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='OptionData')
    except Exception as e:
        print(e)
        return(None)


    stmt = """SELECT MAX(tradedt) from VIX """
    cursor = db.cursor()
    try:
        cursor.execute(stmt)
    except Exception as e:
        pass
    result = cursor.fetchone()
    checkdt = result[0]
    db.close()
    return(checkdt)



def OptionVolumeZ(ticker, tradedt, isCall=True, winlen=50):
    #a minimum volumn theshold, if lower than this volume then ignore
    cutoffVolume = 500
    try:
        db = MSD.connect(user='root', passwd='cdefgh12', db='OptionData')
    except Exception as e:
        print(e)
        return(None)

    stmt = """SELECT MAX(tradedt) from %s  where isCall = %s""" %(ticker, isCall)
    cursor = db.cursor()
    checkdt = None
    try:
        cursor.execute(stmt)
        result = cursor.fetchone()
        checkdt = result[0]
    except Exception as e:
        db.close()
        return(None)

    #print(checkdt)

    if (not checkdt) or (checkdt != tradedt):
        return(None)

    stmt = """select tradedt, sum(volume) from %s where isCall = %s group by tradedt order by tradedt DESC limit %s """ % (ticker, isCall, winlen+1)

    try:
        cursor.execute(stmt)
    except Exception as e:
        db.close()
        return(None)

    #results = cursor.fetchall()
    voldata = [item[1] for item in cursor]
    #print(voldata)
    #print(len(voldata))
    if (len(voldata) <= winlen):
        return(None)

    if (voldata[0] < cutoffVolume):
        return(None)

    return((ticker, voldata[0], np.mean(voldata[1:]), np.std(voldata[1:]), (voldata[0]-np.mean(voldata[1:]))/np.std(voldata[1:])) )

def OptionVolumeZBatch(tickerlist, tradedt, isCall=True, winlen=50):
    outputs = []
    for ticker in tickerlist:
        tmp = OptionVolumeZ(ticker, lastDt, isCall, winlen)
        if tmp:
            outputs += [tmp]

    return(outputs)



tickers = CleanEmptyTables('OptionData')
lastDt = RetrieveLatestOptDt()
print(lastDt)
#lastDt = parser.parse(lastDt)

#tmp = OptionVolumeZ("AAPL", lastDt)
#print(tmp)
#tmp = OptionVolumeZBatch(tickers, lastDt)
#print(tmp)