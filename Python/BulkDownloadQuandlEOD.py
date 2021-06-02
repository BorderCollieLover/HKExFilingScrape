
import json
import datetime
import urllib.request
from urllib.request import urlopen
import os
import zipfile
import pytz
from glob import glob
import csv
import subprocess

def UpdateAllUSTickers(srcdir):
    newTickerfile = srcdir + "tickers.txt"
    tickerfileExists = os.path.isfile(newTickerfile)
    if tickerfileExists:
        curTickers = []
        try:
            with open(newTickerfile, 'r') as csvfile:
                tickerlines = [tuple(line) for line in csv.reader(csvfile)]
                curTickers = sorted(list(set([line[0] for line in tickerlines])))
        except Exception as e:
            print(e)

        #print(len(curTickers))

        allUSTickerfile = "/Users/Shared/Models/all.US.tickers.csv"
        if os.path.isfile(allUSTickerfile):
            tmpTickers =[]
            try:
                with open(allUSTickerfile, 'r') as csvfile:
                    tickerlines = [tuple(line) for line in csv.reader(csvfile)]
                    tmpTickers = sorted(list(set([line[0] for line in tickerlines])))
            except Exception as e:
                print(e)

            #print(len(tmpTickers))

            if tmpTickers:
                tickers = sorted(list(set(tmpTickers + curTickers)))
                #print(len(tickers))
                #print(len(tmpTickers))

                if (len(tickers) > len(tmpTickers)):
                    with open(allUSTickerfile, 'w', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        for ticker in tickers:
                            writer.writerow([ticker])

#UpdateAllUSTickers("/Users/Shared/Quandl/EOD/Tickers/EOD_20170826.txt")
#UpdateAllUSTickers("/Users/Shared/Quandl/WikiEOD/Tickers/full_data_WIKI_PRICES_2017-08-26T01h22m52.txt")

def UpdateTickerList(srcdir):
    tickerfile = srcdir + 'tickers.txt'
    tickerfileExists = os.path.isfile(tickerfile)
    if tickerfileExists:
        tickerfileTime = os.stat(tickerfile).st_mtime
    curTickers = []
    try:
        with open(tickerfile, 'r') as csvfile:
            tickerlines = [tuple(line) for line in csv.reader(csvfile)]
            curTickers = sorted(list(set([line[0] for line in tickerlines])))
    except Exception as e:
        print(e)

    tickers = curTickers
    #print(len(tickers))
    #retrieve tickers files from the Tickers directory
    allTickerFiles = []
    try:
        allTickerFiles = sorted(glob(srcdir+'Tickers/'+'*.txt'), key=os.path.getmtime, reverse=True)
    except Exception as e:
        print(e)

    if (len(allTickerFiles)>0):
        for curTickerfile in allTickerFiles:
            if tickerfileExists:
                if (os.stat(curTickerfile).st_mtime - tickerfileTime < 0):
                    break
            print(curTickerfile)
            tmpTickers = []
            try:
                with open(curTickerfile, 'r') as csvfile:
                    tickerlines = [tuple(line) for line in csv.reader(csvfile)]
                    tmpTickers = sorted(list(set([line[0] for line in tickerlines])))
            except Exception as e:
                print(e)

            if (len(tmpTickers)>0):
                tickers = list(set(tickers)|set(tmpTickers))
                print(len(tickers))

        #print(len(tickers))
        #print(len(curTickers))
        if (len(tickers)>len(curTickers)):
            #print('here')
            tickers = sorted(tickers)
            with open(tickerfile, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                for ticker in tickers:
                    writer.writerow([ticker])

    return(tickers)

#returns 0 if no new file is downloaded
#returns 1 if a new file is downloaded
def DownloadLink2File(src, tgt, overwrite=False):
    srcFile = src
    tgtFile = tgt
    if (not overwrite):
        if (not os.path.isfile(tgtFile)):
            try:
                urllib.request.urlretrieve(srcFile, tgtFile)
            except Exception as e:
                print(tgtFile)
                print(e)
                return(0)
        else:
            print(tgt + ' already exists!')
            return(0)
    else:
        try:
            urllib.request.urlretrieve(srcFile, tgtFile)
        except Exception as e:
            print(tgtFile)
            print(e)
            return(0)
    return(1)

def UnzipFile(tgtfile, tgtdir=''):
    with zipfile.ZipFile(tgtfile, "r") as zip_ref:
        zip_ref.extractall(tgtdir)
    return

#download the WikiEOD database in bulk and into a zip file
#Keep only the latest one unzipped (although maybe not necessary if I don't use that file)


def SetupWikiEOD():
    srcDir = "/Users/Shared/Quandl/WikiEOD/"
    wikipriceurl = "https://www.quandl.com/api/v3/datatables/WIKI/PRICES/delta.json?api_key=3ojxoNzaKndioRPW_hXc"
    wikipricelist = urlopen(wikipriceurl)
    #print(wikipricelist)
    wikipricelinks = json.loads(wikipricelist.read().decode(wikipricelist.info().get_param('charset') or 'utf-8'))
    wikipricelinks = wikipricelinks['data']

    latest_full_data = wikipricelinks['latest_full_data']
    #print(latest_full_data)
    if not isinstance(latest_full_data, dict):
        return

    datalink = latest_full_data['full_data']
    datatimestamp = latest_full_data['to']
    tgtfile = srcDir+"ZIPs/"+datatimestamp+".csv.zip"
    downloadStatus = DownloadLink2File(datalink, tgtfile)

    if (downloadStatus == 0):
        return

    # 1. create updated tickers.txt
    ExtractTickersfromBulk(srcDir, tgtfile, True)
    # 2. Update tickers.txt for this data source
    UpdateTickerList(srcDir)
    UpdateAllUSTickers(srcDir)

    # 3. extract removed tickers' data if any
    SetupRemovedTickers('/Users/Shared/Quandl/WikiEOD/', header=True)
    # 4. re-unzip the latest data as it must have been removed
    #UnzipFile(tgtfile, srcDir + 'tmp/')
    #allbulkfiles = sorted(glob(srcDir + 'tmp/' + '*.csv'), key=os.path.getmtime, reverse=True)
    #if (len(allbulkfiles) > 1):
    #    for tmpfile in allbulkfiles[1:]:
    #        os.remove(tmpfile)

    # 5. remove old zip files to free up disk space
    allZipfiles = sorted(glob(srcDir + 'ZIPs/' + '*.zip'), key=os.path.getmtime, reverse=True)
    if (len(allZipfiles) > 1):
        for tmpfile in allZipfiles[1:]:
            os.remove(tmpfile)
    return

#download the QM EOD database in bulk (which I've subscribed to)
def SetupEOD():
    srcDir = "/Users/Shared/Quandl/EOD/"
    eodurl = "https://www.quandl.com/api/v3/databases/EOD/data?api_key=3ojxoNzaKndioRPW_hXc"
    tgtfile = srcDir + "ZIPs/" + str(datetime.datetime.now(pytz.timezone('US/Eastern')).date()) +".zip"
    print(tgtfile)
    downloadStatus = DownloadLink2File(eodurl, tgtfile)
    if (downloadStatus == 0):
        return

    # 1. create updated tickers.txt
    ExtractTickersfromBulk(srcDir,tgtfile, False)
    # 2. Update tickers.txt for this data source
    UpdateTickerList(srcDir)
    UpdateAllUSTickers(srcDir)

    # 3. Identify removed tickers by comparing the latest two ticker files under Tickers/
    # And extract the data if necessary
    SetupRemovedTickers('/Users/Shared/Quandl/EOD/', header=False)

    # 5. remove old zip files to free up disk space
    allZipfiles = sorted(glob(srcDir + 'ZIPs/' + '*.zip'), key=os.path.getmtime, reverse=True)
    if (len(allZipfiles) > 1):
        for tmpfile in allZipfiles[1:]:
            os.remove(tmpfile)


    return

def SetupRemovedTickers(srcdir, header=True):
    #1. Compare the latest two files in Tickers/
    #2. If there are tickers removed, then:
    # 2.1 Identify the bulk file that corresponds to the ticker file
    # 2.2 Extract OHLC from Bulk
    allTickerFiles = []
    try:
        allTickerFiles = sorted(glob(srcdir+'Tickers/'+'*.txt'), key=os.path.getmtime, reverse=True)
    except Exception as e:
        print(e)

    if (len(allTickerFiles) > 1):
        latestTickerFile = allTickerFiles[0]
        previousTickerFile = allTickerFiles[1]
        print(latestTickerFile)
        print(previousTickerFile)
        latestTickers = []
        try:
            with open(latestTickerFile, 'r') as csvfile:
                tickerlines = [tuple(line) for line in csv.reader(csvfile)]
                latestTickers = sorted(list(set([line[0] for line in tickerlines])))
        except Exception as e:
            print(e)

        previousTickers = []
        try:
            with open(previousTickerFile, 'r') as csvfile:
                tickerlines = [tuple(line) for line in csv.reader(csvfile)]
                previousTickers = sorted(list(set([line[0] for line in tickerlines])))
        except Exception as e:
            print(e)

        tickerfileName = os.path.splitext(os.path.basename(previousTickerFile))[0]
        allZipfiles = sorted(glob(srcdir + 'ZIPs/' + '*.zip'), key=os.path.getmtime, reverse=True)
        tgtBulkfile = None
        for curZipfile in allZipfiles:
            datafilenames = zipfile.ZipFile(curZipfile).namelist()
            datafilename = datafilenames[0]
            print(datafilename)
            if (os.path.splitext(datafilename)[0] == tickerfileName):
                tgtBulkfile = curZipfile
                break
        # if no matching zip file, remove the ticker file for data consistency
        if tgtBulkfile is None:
            os.remove(previousTickerFile)
            return

        removedTickers = list(set(previousTickers) - set(latestTickers))
        if ((removedTickers is not None) and(len(removedTickers)>0)):
            ExtractOHLCfromBulk(srcdir, tgtBulkfile, removedTickers, header)
        #else:
        #    #no removed tickers, no need to keep the previous zip file and its associated tickers file
        #    os.remove(previousTickerFile)
        #    os.remove(tgtBulkfile)

    return



def ExtractOHLCfromBulk(srcdir, bulkfile, tickers, header=True):
    datafilenames = zipfile.ZipFile(bulkfile).namelist()
    datafilename = datafilenames[0]
    ohlcfile = srcdir + 'tmp/' + datafilename
    unzippedFile = False
    if not os.path.isfile(ohlcfile):
        unzippedFile = True
        UnzipFile(bulkfile, srcdir + 'tmp/')

    with open(ohlcfile, 'r') as csvfile:
        ohlclines = [tuple(line) for line in csv.reader(csvfile)]
        if header == True:
            ohlclines = ohlclines[1:]

        for ticker in tickers:
            ohlcdata = [tuple(line[1:]) for line in ohlclines if line[0] == ticker]
            headerline = [("Open", "High", "Low", "Close", "Volume", "Dividend", "Split", "Adj_Open", "Adj_High", "Adj_Low",
                           "Adj_Close", "Adj_Volume")]
            ohlcdata = headerline + ohlcdata

            with open(srcdir + 'Daily/' + ticker + '.csv', 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(ohlcdata)

    if unzippedFile:
        os.remove(ohlcfile)
    return()

#This function extracts the ticker list from the specified bulk(zip) file and create a txt file under the Tickers subdirectory, using the name of the bulk file
def ExtractTickersfromBulk(srcdir, bulkfile, header=True):
    datafilenames = zipfile.ZipFile(bulkfile).namelist()
    datafilename = datafilenames[0]
    #print(os.path.splitext(datafilename)[0])
    #return
    tickerfile = srcdir + 'Tickers/' + os.path.splitext(datafilename)[0] + '.txt'
    #print(datafilenames[0])
    #print(tickerfile)
    if not os.path.isfile(tickerfile):
        ohlcfile = srcdir + 'tmp/' + datafilename
        if not os.path.isfile(ohlcfile):
            UnzipFile(bulkfile, srcdir + 'tmp/')

        with open(ohlcfile, 'r') as csvfile:
            ohlclines = [tuple(line) for line in csv.reader(csvfile)]
            if header==True:
                ohlclines = ohlclines[1:]
            tickers = sorted(list(set([line[0] for line in ohlclines])))

        with open(tickerfile, 'w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            for ticker in tickers:
                writer.writerow([ticker])

        #ExtractSummaryfromOHLC(srcdir, ohlcfile, tickerfile, header=header)

        os.remove(ohlcfile)

    return

def ExtractSummaryfromOHLC(srcDir, ohlcfile, tickerfile, header=True):
    if not os.path.isfile(ohlcfile):
        print(ohlcfile + ' does not exist!')
        return

    if not os.path.isfile(tickerfile):
        print(tickerfile + ' does not exist!')
        return

    summaryfile = srcDir + 'Summary/' + os.path.splitext(os.path.basename(tickerfile))[0]+'.csv'

    if os.path.isfile(summaryfile):
        print(summaryfile + ' alreadys exists!')
        return

    with open(tickerfile, 'r', encoding='utf-8') as csvfile:
        tmpTickers = [tuple(line) for line in csv.reader(csvfile)]
        tickers = [line[0] for line in tmpTickers]



    summaryOutput = []
    with open(ohlcfile, 'r') as csvfile:
        ohlclines = [tuple(line) for line in csv.reader(csvfile)]
        if header == True:
            ohlclines = ohlclines[1:]
        for ticker in tickers:
            print(ticker)
            ohlcdata = [tuple(line[1:]) for line in ohlclines if line[0] == ticker]
            beginDt = min([line[0] for line in ohlcdata])
            endDt = max([line[0] for line in ohlcdata])
            summaryOutput += [(ticker, beginDt, endDt)]
            print(ticker, beginDt, endDt)

    with open(summaryfile, 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(summaryOutput)

    return


def ExtractAllTickers(srcDir, header=True):
    allbulkfiles = sorted(glob(srcDir + 'Zips/' + '*.zip'), key=os.path.getmtime)
    if (len(allbulkfiles)>0):
        for tmpfile in allbulkfiles:
            ExtractTickersfromBulk(srcDir, tmpfile, header)
    return

SetupEOD()
SetupWikiEOD()

#ExtractSummaryfromOHLC("/Users/Shared/Quandl/EOD/", '/Users/Shared/Quandl/EOD/tmp/EOD_20170610.csv', '/Users/Shared/Quandl/EOD/Tickers/EOD_20170610.txt', False)
#ExtractAllTickers("/Users/Shared/Quandl/EOD/", False)
#ExtractAllTickers("/Users/Shared/Quandl/WikiEOD/", True)

#ExtractTickersfromBulk("/Users/Shared/Quandl/EOD/", '/Users/Shared/Quandl/EOD/Zips/2017-06-02.zip', False)

#UpdateTickerList("/Users/Shared/Quandl/WikiEOD/"+'full_data_WIKI_PRICES_2017-06-01t01h22m56.csv', "/Users/Shared/Quandl/WikiEOD/"+'foo1.txt', True)
#UpdateTickerList("/Users/Shared/Quandl/EOD/"+'EOD_20170601.csv', "/Users/Shared/Quandl/EOD/"+'foo1.txt', False)

#SetupRemovedTickersforOne('/Users/Shared/Quandl/EOD/EOD_20170527.csv', "/Users/Shared/Quandl/EOD/tickers.txt", "/Users/Shared/Quandl/EOD/", header=False)
#SetupRemovedTickers('/Users/Shared/Quandl/EOD/', header=False)
#ExtractOHLCfromBulk('/Users/Shared/Quandl/EOD/', '/Users/Shared/Quandl/EOD/ZIPs/2017-06-03.zip', ['SPP','WILN'], False)