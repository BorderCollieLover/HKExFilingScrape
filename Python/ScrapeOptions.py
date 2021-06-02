#!/usr/local/bin/python3.4

from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import datetime
import re
import html
import csv
import os
from pandas.tseries.offsets import BDay
import gzip
import multiprocessing
from urllib.error import URLError, HTTPError
from pytz import timezone
import pytz
from bdateutil import isbday
import holidays
import time
from bdateutil import relativedelta
from socket import *
import subprocess
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import sys
import locale
from operator import itemgetter
import socket
import io
import signal
from subprocess import check_output


def get_pid(name):
    return check_output(["/usr/local/bin/pidof",name])




## Generate the output file name based on ticker and the date of scraping
## Keep only as of close data so if market is closed but scraping time is of the same day
## then file name is today (if it is a business day) or the previous business day
## if market is closed but scraping time is a day later, then find the previous business day
## Check if the option data is already scraped
## only US stock options data is available through Yahoo Finance
##

isdst_now_in = lambda zonename: bool(datetime.datetime.now(pytz.timezone(zonename)).dst())

outputpath = '/Users/Shared/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )


#for sending a data request posing as a Safari
headervalues = {'name': 'Michael Foord',
          'location': 'Northampton',
          'language': 'Python'}
headerdata = urlencode(headervalues)
headerdata = headerdata.encode('utf-8')

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'
headers = { 'User-Agent' : user_agent }

seleniumopts = Options()
seleniumopts.add_argument("user-agent=" + user_agent)

dcap = dict(DesiredCapabilities.PHANTOMJS)
dcap["phantomjs.page.settings.userAgent"] = (
    user_agent
)


globalProcessArray = []

def CreateDirectory(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)

def IsMarketOpen():
    currDt = datetime.datetime.now(timezone('EST'))
    myUSHolidays = holidays.HolidayBase()
    for date, name in sorted(holidays.US(years=[currDt.year, currDt.year+1]).items()):
        if ((name != "Columbus Day") and (name != "Veterans Day") and (name !="Thanksgiving")):
            myUSHolidays.append({date:name})

    if isbday(currDt, holidays=myUSHolidays):
        if isdst_now_in("America/New_York"):
            dstoffset = -1
        else:
            dstoffset = 0

        mktOpenHour = 9
        mktOpenMin = 30
        mktCloseHour = 16
        #mktCloseMin = 0
        print(currDt)
        #print(currDt.hour)

        if (((currDt.hour > mktOpenHour + dstoffset) or ((currDt.hour == mktOpenHour + dstoffset) and (currDt.minute >= mktOpenMin))) and (currDt.hour < mktCloseHour + dstoffset)):
            return(True)
        else:
            return(False)
    else:
        return(False)



def CreateOutputFileName(ticker, scrapedt):
    fileticker = re.sub("\^", '', ticker)

    outputdir = outputpath + 'OptionData/'+fileticker
## Nov 27th, 2014
## If not on a business day, return last business day
## If on a business day then
##   a) return previous business day if before market opens
##   b) return current day if after marekt closes
##   c) None if in trading hour
## Subsequently, the script will suspend for two hours if a None is returned
    #print((isbday(scrapedt, holidays=holidays.US())))
    #print((holidays.US().get_list(scrapedt)))
    #print(len(holidays.US().get_list(scrapedt)) == 0 )

    if (isbday(scrapedt, holidays=holidays.US())  or ((len(holidays.US().get_list(scrapedt)) > 0 ) and ((holidays.US().get_list(scrapedt)[0]=='Columbus Day') or (holidays.US().get_list(scrapedt)[0]=='Veterans Day')  or (holidays.US().get_list(scrapedt)[0]=='Thanksgiving')))):
        if isdst_now_in("America/New_York"):
            dstoffset = -1
        else:
            dstoffset = 0
        if ((scrapedt.hour <9 + dstoffset) or ((scrapedt.hour == 9 + dstoffset) and (scrapedt.minute < 30))):
            filedt = scrapedt + relativedelta(bdays=-1, holidays=holidays.US())
        else:
            if ((scrapedt.hour > 16 + dstoffset) or ((scrapedt.hour  == 16 + dstoffset) and (scrapedt.minute >= 20))) :
                filedt = scrapedt
            else:
                return None
    else:
        #print('here2')
        filedt = scrapedt + relativedelta(bdays=-1, holidays=holidays.US())

    return(outputdir+'/'+filedt.strftime('%Y%m%d')+'.csv.gz')

##Added: May 22, 2015
##Calculate time to the next US market close, measured in seconds
def Time2NextUSMarketClose():
    currDt = datetime.datetime.now(timezone('EST'))
    myUSHolidays = holidays.HolidayBase()
    for date, name in sorted(holidays.US(years=[currDt.year, currDt.year+1]).items()):
        if ((name != "Columbus Day") and (name != "Veterans Day") and (name !="Thanksgiving")):
            myUSHolidays.append({date:name})


    MktCloseHour = 16
    if isdst_now_in("America/New_York"):
        MktCloseHour -=1
    if (currDt.hour > MktCloseHour):
        nextDt = currDt + relativedelta(bdays=1,holidays=myUSHolidays)
    else:
        nextDt = currDt

    ##Use 20 minutes to account for delay in Yahoo Finance website
    nextDt = nextDt.replace(hour=MktCloseHour, minute=20)
    print(nextDt)
    return((nextDt - currDt).total_seconds())


## save scraped option data to a file
## July 20, 2016: modified slightly to take in as the third parameter the filename rather than the scraping date
##  this way avoids repeating CreateOutputFilename to improve efficiency.
## works for ScrapeOption2, for other, earlier calls need to change the input parameter slightly
def SaveOptionData(ticker, optiondump, outputfilename):
    if (len(optiondump)>0):
        #outputfilename = CreateOutputFileName(ticker, scrapingdt)
        outputpath = os.path.dirname(os.path.realpath(outputfilename))
        CreateDirectory(outputpath)

        with gzip.open(outputfilename, 'wt') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(optiondump)

def CleanupScrapedData(data):
    outputData = []
    if (data is not None) and (len(data) > 0):
        data = sorted(data, key=itemgetter(1, 10), reverse=False)
        outputData = []
        # print('here2')

        for lnIdx, line in enumerate(data):
            if lnIdx == 0:
                outputData += [line]
            else:
                # if same option symbol, it must have been scraped twice, keep the first scraping result
                if not (line[1] == data[lnIdx - 1][1]):
                    outputData += [line]

    return(outputData)


# save scraped option data to a file, but check if the file already exists
# if yes, then combine the data and keep the entry scraped earlier
# This is helpful because in the latest multi-processing scheme, occasionally multiple threads can be sraping for the same underlying
# After switching to phantomjs, it is also notable that scraping results is somewhat unstable with phantomjs but this might be related to network issues
# Nov 2, 2016
def SaveOptionData2(ticker, optiondump, outputfilename):
    if (len(optiondump)>0):

        if os.path.isfile(outputfilename):
            #print(ticker)
            #print(outputfilename)
            try:
                with io.TextIOWrapper(gzip.open(outputfilename, 'r')) as f:
                    tmpData = [tuple(line) for line in csv.reader(f)]
            except Exception as e:
                print(e)
                tmpData = None

            if (tmpData is not None) and (len(tmpData) >0):
                #print(len(tmpData))
                tmpData += optiondump
                optiondump = CleanupScrapedData(tmpData)
            else:
                optiondump = CleanupScrapedData(optiondump)
            #print(len(optiondump))
        else:
            outputpath = os.path.dirname(os.path.realpath(outputfilename))
            CreateDirectory(outputpath)


        with gzip.open(outputfilename, 'wt') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(optiondump)

        print(len(optiondump), ticker)



##Scrape the option data for one maturity
##Return the data in a list of tuples
##Note that the scraping time is passed along and stored the in the data
def ScrapeOneExpiry(ExpiryLink, ScrapeDt):
    optionlist = []

    try:
        optionsPage = urlopen(ExpiryLink, timeout=10)
        soup = BeautifulSoup(optionsPage)
        alltables = soup.findAll('table')

        for tblidx in range(1,len(alltables)):
            opttable=alltables[tblidx]
            for row in opttable.findAll("tr"):
                cells=row.findAll("td")

                ## option data row has 10 data points
                if (len(cells)==10) :
                    thisoption = ()
                    for j in range(0, len(cells)):
                        ## The first two data elements, namely strike and option ticker, have hyper-links
                        if (j < 2):
                            tmpstr = re.findall('\>(.+?)\<', str(cells[j]))
                            thisoption = thisoption + (re.search('\>(.+?)$', tmpstr[0]).group(1),)
                        else:
                            thisoption= thisoption+(re.search('\>(.+?)\<', str(cells[j])).group(1),)
                    thisoption = thisoption + (ScrapeDt.strftime('%Y-%m-%d %H:%M'),)
                    optionlist = optionlist+ [thisoption]
    except HTTPError as e3:
        print('Retrieving options for '+ExpiryLink+': The server couldn\'t fulfill the request.')
        print('Error code: ', e3.code)
    except URLError as e3:
        print('Retrieving options for '+ExpiryLink+': We failed to reach a server.')
        print('Reason: ', e3.reason)
    except Exception as e3:
        print('Unknown error retrieving options for '+ExpiryLink)
        print(e3)

    return(optionlist)



#July 19, 2016
#Managed to get Yahoo! Finance to work again using Selenium and PhantomJS
#But so far it seems very slow and resources-intensive
#Mar 14, 2017
# Explicitly use webdriverwaittime and webdrivertimeout
# In earlier versions I used a webdriverwaittime of 1 (second) and time.sleep
# This results in a lot of hang phantomJS instances and eventually slows down the computer
# To solve this problem, I used driver.implicitly_wait and driver.set_page_load_timeout
#   as pointed out online, PhantomJS's own desired capability is not yet supported in Selenium so this is the viable workout
#   http://stackoverflow.com/questions/21739450/setting-timeout-on-selenium-webdriver-phantomjs
# However, what I found is that:
#   1) To set webdrivertimeout to a small number, e.g. 10, the scraping process times out easily (at least on the main Macbook Pro)
#       even though in the past I used time.sleep to wait for 1; IT SEEMS SET PAGE LOAD TIMEOUT is MUCH MORE SENSITIVE
#   2) To try to test for the presenence of tag, e.g. "select" or "tr", doesn't work. Maybe I haven't tried all possibilities but it didn't work for the experiements I did
# So the way to work around this issue, seems to be using a small webdriverwaittime and a much larger webdrivertimeout
#  and to keep trying to get elements until the time out
# Experiments seem to confirm to my suspicion that set_page_load_timeout is very sensitive
#  For example, if I set webdriverwaittime to 0.5, most webpage would load within this time limit and scraping can proceed
#   but if I set a timeout to 1 , almost all tickers will throw a time-out exception
# So essentially I write a wrapper of Expected Condition based on what the scraping script is looking for
# Finally, I switched back to time.sleep instead of implicitly_wait because the documentation of Selenium is not clear enough whether I can use a floating number for wait time
# but time.sleep is explicit in accepting floating numbers

def OptionScrape2(ticker, ignoreExisting=False):
    ScrapingDt = datetime.datetime.now(timezone('EST'))
    OutputFileName = CreateOutputFileName(ticker, ScrapingDt)
    webdriverwaittime = 0.1
    webdrivertimeout = 30
    # failCtrThreshold is used to control re-try of finding elements in PhantomJS returns
    # use +1 to so that
    if ignoreExisting:
        #recheck if the ticker has already been scraped/rescraped so as to save computation
        recheckResult = CheckOptionCounts([ticker], ignoreExisting)
        if (recheckResult is None) or (len(recheckResult)<1):
            return(0)
        webdrivertimeout = 2*webdrivertimeout
    failCtrThreshold = int((webdrivertimeout / webdriverwaittime))+1


    if (OutputFileName is None):
        return(0)


    scrapingdtstring = ScrapingDt.strftime('%Y-%m-%d %H:%M')

    if (not os.path.isfile(OutputFileName)) or ignoreExisting:
        if ignoreExisting:
            print(ticker, '.........Rescraping.......')
        #optionsUrl = "http://finance.yahoo.com/quote/" + ticker + "/options"
        optionsUrl = "http://finance.yahoo.com/quote/" + ticker + "/options?p=" + ticker

        driver = webdriver.PhantomJS(executable_path='/usr/local/Cellar/phantomjs/2.1.1/bin/phantomjs',
                                     desired_capabilities=dcap,
                                     service_args=['--ignore-ssl-errors=true', '--ssl-protocol=any'])

        tmps = None
        try:
            driver.set_page_load_timeout(webdrivertimeout)
            driver.get(optionsUrl)
            #driver.implicitly_wait(webdriverwaittime)
            #time.sleep(webdriverwaittime)
            #print(len(tmps))
            #WebDriverWait(driver, webdriverwaittime).until(EC.presence_of_element_located(By.TAG_NAME, "select"))
            #WebDriverWait(driver, webdriverwaittime).until(EC.visibility_of_element_located(By.TAG_NAME, "Select"))
            #WebDriverWait(driver, webdriverwaittime)
            failCtr = 1
            tmp = None
            while (failCtr < failCtrThreshold):
                time.sleep(webdriverwaittime)
                try:
                    tmp = driver.find_element_by_tag_name("select")
                except Exception as e:
                    failCtr += 1
                    continue
                    #driver.implicitly_wait(webdriverwaittime)

                if tmp is None:
                    failCtr +=1
                    #print ('wait more', failCtr)
                    #driver.implicitly_wait(webdriverwaittime)
                else:
                    tmps = tmp.find_elements_by_tag_name("option")
                    if (tmps is None) or (tmps ==[]) or (len(tmps) <=0):
                        failCtr += 1
                    else:
                        #print('returned, ', failCtr)
                        break;

        except Exception as e:
            #print(e)
            print(-1, ticker)
            driver.service.process.send_signal(signal.SIGTERM)
            driver.quit()
            return(-1)

        #print("here 2 ")
        if (tmps is None) or (tmps ==[]) or (len(tmps) <=0):
            print(-1, ticker)
            driver.service.process.send_signal(signal.SIGTERM)
            driver.quit()
            return(-1)

        expiries = []
        outputData = []

        for x in tmps:
            expiries += [x.get_attribute('value')]

        #for efficicency and data complete-ness, move scraping of each expiry in here
        failedExpiries = []
        for expiry in expiries:
            expiryUrl = "http://finance.yahoo.com/quote/" + ticker + "/options?date=" + expiry
            failCtr = 0
            expiryData = []
            try:
                driver.get(expiryUrl)
                while (failCtr < failCtrThreshold):
                    time.sleep(webdriverwaittime)
                    failCtr += 1
                    try:
                        #time.sleep(webdriverwaittime*(failCtr+1))
                        #driver.implicitly_wait(webdriverwaittime)
                        #WebDriverWait(driver, webdriverwaittime).until(EC.presence_of_element_located(By.TAG_NAME, "tr"))
                        tmps = driver.find_elements_by_tag_name("tr")
                    except Exception as e:
                        #failCtr += 1
                        print(ticker, e)
                        print(ticker, expiryUrl, failCtr)
                        continue
                    if tmps and (len(tmps) >0):
                        #print(len(tmps))
                        for x in tmps:
                            optionItems = []
                            try:
                                optionItems = x.find_elements_by_tag_name("td")
                            except Exception as e:
                                #failCtr +=1
                                continue
                            if optionItems:
                                if len(optionItems) == 10:
                                    lineResult = ()
                                    try:
                                        for optionItem in optionItems:
                                            lineResult = lineResult + (optionItem.text,)
                                    except Exception as e:
                                        print(e)
                                    if (len(lineResult) == 10):
                                        lineResult = lineResult + (ScrapingDt.strftime('%Y-%m-%d %H:%M'),)
                                        expiryData += [lineResult]

                        if len(expiryData)>0:
                            break;
            except Exception as e:
                #print(e)
                failedExpiries += [expiry]
                print(ticker, expiryUrl, failCtr )

                #else:
                    #failCtr += 1
                #    continue
                #if len(expiryData)>0:
                    #print('expiry returned ' ,failCtr)
                #    break
                #else:
                #    failCtr += 1

            if expiryData:
                outputData = outputData + expiryData
                #print(ticker, len(outputData), failCtr)

        if (len(failedExpiries) > 0):
            webdrivertimeout = 2*webdrivertimeout
            failCtrThreshold = int((webdrivertimeout / webdriverwaittime)) + 1
            driver.set_page_load_timeout(webdrivertimeout)
            expiries = failedExpiries

        # try failed expiries with longer time out value
            for expiry in expiries:
                expiryUrl = "http://finance.yahoo.com/quote/" + ticker + "/options?date=" + expiry
                failCtr = 0
                expiryData = []
                try:
                    driver.get(expiryUrl)
                    while (failCtr < failCtrThreshold):
                        time.sleep(webdriverwaittime)
                        failCtr += 1
                        try:
                            #time.sleep(webdriverwaittime*(failCtr+1))
                            #driver.implicitly_wait(webdriverwaittime)
                            #WebDriverWait(driver, webdriverwaittime).until(EC.presence_of_element_located(By.TAG_NAME, "tr"))
                            tmps = driver.find_elements_by_tag_name("tr")
                        except Exception as e:
                            #failCtr += 1
                            print(ticker, e)
                            print(ticker, expiryUrl, failCtr)
                            continue
                        if tmps and (len(tmps) >0):
                            #print(len(tmps))
                            for x in tmps:
                                optionItems = []
                                try:
                                    optionItems = x.find_elements_by_tag_name("td")
                                except Exception as e:
                                    #failCtr +=1
                                    continue
                                if optionItems:
                                    if len(optionItems) == 10:
                                        lineResult = ()
                                        try:
                                            for optionItem in optionItems:
                                                lineResult = lineResult + (optionItem.text,)
                                        except Exception as e:
                                            print(e)
                                        if (len(lineResult) == 10):
                                            lineResult = lineResult + (ScrapingDt.strftime('%Y-%m-%d %H:%M'),)
                                            expiryData += [lineResult]

                            if len(expiryData)>0:
                                break;
                except Exception as e:
                    #print(e)
                    #failedExpiries += [expiry]
                    print(ticker, expiryUrl, failCtr )
                if expiryData:
                    outputData = outputData + expiryData

        driver.service.process.send_signal(signal.SIGTERM)
        driver.quit()


        #SaveOptionData(ticker, outputData, OutputFileName)
        #print(len(outputData),  ticker)
        try:
            SaveOptionData2(ticker, outputData, OutputFileName)
        except Exception as e:
            print(ticker)
            print(e)

        if (outputData is not None):
            return (len(outputData))
        else:
            return (-1)
    else:
        return(0)




#scrape options of the given ticker list
#Put successfully scraped tickers in tickers_q
#Put the number of options scraped in out_q, although the number is not really used, it is just a token to signal a sub-process has finished
#Check signal_q for signal to exit. If the majority of parallel processes are done (by checking tokens in out_q),
# the wrapper will collect scraped tickes (tickers_q) and signal other parallel processes to quit (via signal_q);
# the wrapper will start a new set of parallel process to go through the residual tickers
def OptionScrapeBatch(tickers, out_q, tickers_q, signal_q, ignoreExisting=False):
    tmpCount = 0
    i = 0
    signal_check_i = len(tickers)//10
    for ticker in tickers:
        try:
            tickers_q.put(ticker)
            x = OptionScrape2(ticker, ignoreExisting=ignoreExisting)
            #print(ticker, x)
            if (x>=0):
                #tickers_q.put(ticker)
                tmpCount += x
        except Exception as e:
            print('here')
            print(e)
        i += 1
        if (i > signal_check_i):
            try:
                if signal_q.empty():
                    continue
                else:
                    signal_q.get()
                    print('closing current process after ......', i)
                    break
            except Exception as e:
                    print('signal queue closed', i)

    try:
        out_q.put(tmpCount)
    except Exception as e:
        print(e)
    print(len(tickers), ' tickers finished in one batch')
    return(None)


ScrapeTickers = ['GLD', 'SLV', 'USO', 'SPY', 'VXX', 'VXZ', '^VIX']
ScrapeTickers = list(set(ScrapeTickers))

SecondaryTickers=[ "^RUT", "EWJ", "IBB", "FXI", "XRT", "XLE", "XOP", "OIH", "AMLP", "XLF", "XLV", "XLK", "XLY", "XLI", "XLU", "XLP", "GDX"]

#PriorityTickers includes the tickers that we monitor as listed in the related files below, including the two lists above which are mainly indices and sector etfs
PriorityTickers = list(set(ScrapeTickers + SecondaryTickers))
tickerfiles = ['ChineseFundTickers', 'chinese.adr', 'watchlist', 'positions', 'Point72', "indices"]
for tickerfilename in tickerfiles:
    curTickerFile = '/Users/Shared/SC13Monitor/Lists/' + tickerfilename + '.csv'
    try:
        with open(curTickerFile, encoding='utf-8') as csvfile:
            curTickers = [line[0] for line in csv.reader(csvfile)]
        PriorityTickers = list(set(PriorityTickers + curTickers))
    except Exception as e:
        print(e)

with open(outputpath+'Models/liquid option stocks.csv', 'r') as csvfile:
    tickerreader = csv.reader(csvfile)
    for row in tickerreader:
        SecondaryTickers +=row
SecondaryTickers = list(set(SecondaryTickers) - set(ScrapeTickers))
ScrapeTickers += SecondaryTickers


StaleTickers = [ 'XRS','VA', 'RIGP', 'CAM', 'UA-C', 'MWW', 'CKEC', 'DTSI', 'NCT', 'AHS', 'LMCA', 'LMCB', 'LMCK', 'MFRM', 'IMS', 'BRCM', 'BRCD', 'SSS', 'AEGR', 'QLTI', 'ACW', 'CVT', 'AOI', 'MEG', 'DRII', 'EMC', 'CJES', 'ISIS', 'MBLX','PSG','SCTY','TCK','TAL', 'KUTV', 'MY', 'TAOM', 'EJ', 'NPD', 'DSKY', 'IM', 'LXK', 'DANG', 'TSL', 'ACTS', 'SYUT', 'KZ', 'OVTI', 'WX', 'VIMC', 'BONA','MR', 'HMIN', 'YOKU','CCSC','MCOX', 'DATE','LONG', 'XUE', 'QIHU', 'SWHC','IM', 'LXK','DANG', 'EJ', 'CSUN','JST','ACI','ADT','BTU','PBY', 'KING', 'SUNE','SWSH', "HMIN", "DATE", 'BONA', 'YOKU', 'KUTV', 'LDRH', 'MY', 'TAOM', 'GB', 'OVTI', 'VIMC', 'MR', 'CCSC', 'MCOX', 'LONG', 'XUE', 'QIHU', 'VIX', 'RUT', 'GSPC', 'SP500TR', 'IXIC', '.DS_Store', '@OptionCount.csv', '_Archived Items', 'RSE', 'TPUB', '@log']
StaleTickers = list(set(StaleTickers))
StaleTickersFile = outputpath+'Models/option skip list.csv'
if (os.path.isfile(StaleTickersFile)):
    with open(StaleTickersFile, 'r') as csvfile:
        tickerreader = csv.reader(csvfile)
        for row in tickerreader:
            StaleTickers +=row

ScrapeTickers = list(set(ScrapeTickers) - set(StaleTickers))

R1000Tickers=[]
with open(outputpath+'Models/R1000.csv', 'r') as csvfile:
    tickerreader = csv.reader(csvfile)
    for row in tickerreader:
        R1000Tickers +=row

R2000Tickers=[]
with open(outputpath+'Models/R2000.csv', 'r') as csvfile:
    tickerreader = csv.reader(csvfile)
    for row in tickerreader:
        R2000Tickers +=row


#ScrapeTickers = ['AMCN']
##for ticker in ScrapeTickers:
##        OptionScrape(ticker)

##OptionScrape('DQ')
##OptionScrape('KE')
##OptionScrape('CAF')
##OptionScrape('^VIX')

#The wrapper to run scraping simultaneously on NumOfThreads processes
#To avoid the scenario when the whole program hang on one or two particularly slow processes, the wrapper will poll the out_q
# wehereby a token will be put in when a sub-process finishes
#When a number of processes finish -- determined by threadCutOff -- the wrapper will either a
# a) exit, if exitOnZero is True, and leave the residual processes to finish the in backgroud or,
# b) signal the remaining processes to stop (via signal_q); collect the tickers that are successfully scraped and call ScrapeOptionsParallel3 recursively with the residual tickers
#When called recursively, set exitOnZero to True, since the residual tickers are just a few, it is more efficient to just let the processes run through rather than recursively keep spawning new processes,
# Otherwise a few tickers that might take a long time to scrape will be unnecessarily scraped repeatedly in spawn processes
def ScrapeOptionsParallel3(tickers, NumOfThreads, exitOnZero=False, recursiveCtr=1, ignoreExisting=False):
    if not tickers:
        return

    if len(tickers) <1 :
        return

    print(len(tickers), ' tickers to scrpae ')
    print(NumOfThreads, ' usable processes')
    if ((len(tickers)==1) or (NumOfThreads==1)):
        out_q = multiprocessing.Queue()
        tickers_q = multiprocessing.Queue()
        signal_q = multiprocessing.Queue()
        #April 19, 2017
        #When there is only ticker, or one thread, run it as an indepdent thread so as not to hold up follow-up processing
        #OptionScrapeBatch(tickers, out_q, tickers_q, signal_q, ignoreExisting=ignoreExisting)
        runThread = multiprocessing.Process(target=OptionScrapeBatch, args=(tickers, out_q, tickers_q, signal_q, ignoreExisting))
        runThread.start()
        print('here')
        #end of April 19, 2017
        out_q.close()
        tickers_q.close()
        return

    tickers = list(set(tickers))
    if (NumOfThreads <=0):
        NumOfThreads = 2
    # if threadCutOff processes have terminated, via checking out_q, then terminated the remaining processes and scrape in parallel again
    # to avoid a few residual tickers holding up the whole computation for too long
    if (NumOfThreads >= len(tickers)):
        NumOfThreads = len(tickers)

    #threadCutOff = NumOfThreads // 2
    threadCutOff = 1
    #Add this so that when the residual list is still long, keep working on it before moving on to lower priority tickers too early
    residuallenCutOff = 200*recursiveCtr # if there are more than residuallenCutOff residual tickers, the recursive call will continue with exitOnZero = False

    signal_count = NumOfThreads - threadCutOff
    threads = [None] * NumOfThreads
    out_q = multiprocessing.Queue()
    tickers_q = multiprocessing.Queue()
    signal_q = multiprocessing.Queue()
    tickerListLen = len(tickers) // NumOfThreads

    for i in range(NumOfThreads):
        startIdx = i * tickerListLen
        if (i == (len(threads) - 1)):
            endIdx = len(tickers) - 1
        else:
            endIdx = (i + 1) * tickerListLen - 1
        threads[i] = multiprocessing.Process(target=OptionScrapeBatch,
                                             args=(tickers[startIdx:(endIdx + 1)], out_q, tickers_q, signal_q, ignoreExisting))
        threads[i].start()

    #globalProcessArray += threads


    total_options_scraped = 0
    scraped_tickers = []
    qgetCtr = 0
    while (qgetCtr < threadCutOff):
        out_q.get()
        qgetCtr += 1

    if (not exitOnZero):
        for i in range(signal_count):
            signal_q.put(1)
    #this break seems necessary for pipes to flush, so that all scraped tickers are properly sent to tickers_q
    time.sleep(10)

    while (not out_q.empty()):
        out_q.get()

    while (not tickers_q.empty()):
        scraped_tickers += [tickers_q.get()]

    if exitOnZero:
        while (not out_q.empty()):
            out_q.get()
        out_q.close()
        tickers_q.close()
        signal_q.close()
        return

    while (not tickers_q.empty()):
        scraped_tickers += [tickers_q.get()]

    residual_tickers = list(set(tickers) - set(scraped_tickers))
    print(residual_tickers)
    if (residual_tickers):
        #if calling recursively, set exitOnZero to true which can be helpful when there is a big list of tickers with no option data
        # as with most of the tickers in the ScrapeTickers list in the main program.
        ScrapeOptionsParallel3(residual_tickers, NumOfThreads, (len(residual_tickers)<residuallenCutOff), recursiveCtr*2, ignoreExisting=ignoreExisting)

    out_q.close()
    tickers_q.close()
    signal_q.close()



#a sequence of (hostname, loadfactor), the loadfactor is roughly the number of cores to use
#need to get the hostname from socket.gethostname and copy it over, somehow typing in doesn't work, might be some issues with hyphen
DistributedScrapingLoad = [("Mins-MacBook-Pro.local", 16),("users-MacBook-Pro.local", 14)]
DistributedScrapingLoad = sorted(DistributedScrapingLoad, key=lambda x:x[0])
print(DistributedScrapingLoad)

def GetMyTickers(tickers):
    tickers = sorted(list(set(tickers)))

    myHostname = socket.gethostname()
    startIdx = 0
    myLoad = 0
    totalLoad = 0
    for line in DistributedScrapingLoad:
        if (line[0] == myHostname):
            myLoad = line[1]
        else:
            if myLoad == 0:
                startIdx += line[1]

    for line in DistributedScrapingLoad:
        totalLoad += line[1]

    print('this computer is ', myHostname)
    print(startIdx, myLoad)

    if startIdx ==0:
        startTickerIdx = 0
    else:
        startTickerIdx = len(tickers)*startIdx//totalLoad

    if ((startIdx + myLoad) == totalLoad):
        endTickerIdx = len(tickers)+1
    else:
        endTickerIdx = len(tickers)*(startIdx + myLoad)//totalLoad

    myTickers = tickers[startTickerIdx:endTickerIdx]

    return(myTickers)


#This Distributed Computing wrapper takes into the structure above as input, so can dynamically scale to more computers
#The wrapper, distribute tickers to different computers in the order of the computer name, and proportional to the load factor
#When runResiduals is True, it scrapes all the tickers that SHOULD have been scraped by other computer(s) as a fail-safe measure if other computers are down
#In the main program, however, we execute the runResiduals mode after scraping tickers of lower priority.
#In normal scenarios, the residual tickers should have been scraped by other computer(s) and we don't waste computing power on multiple computers for one subsets of time-consuming tickers
#In the case when we notice one computer is down and reboot it, say in the morning, this scheme might still allow the computer to catch up to ensure the efficiency of distributed computing
def CrudeDistributedWrapper2(tickers, NumOfThreads, exitOnZero=False, runResiduals=False, ignoreExisting=False):
    tickers = sorted(list(set(tickers)))

    myHostname = socket.gethostname()
    startIdx = 0
    myLoad = 0
    totalLoad = 0
    for line in DistributedScrapingLoad:
        if (line[0] == myHostname):
            myLoad = line[1]
        else:
            if myLoad == 0:
                startIdx += line[1]

    for line in DistributedScrapingLoad:
        totalLoad += line[1]

    print('this computer is ', myHostname)
    print(startIdx, myLoad)

    if startIdx ==0:
        startTickerIdx = 0
    else:
        startTickerIdx = len(tickers)*startIdx//totalLoad

    if ((startIdx + myLoad) == totalLoad):
        endTickerIdx = len(tickers)+1
    else:
        endTickerIdx = len(tickers)*(startIdx + myLoad)//totalLoad

    myTickers = tickers[startTickerIdx:endTickerIdx]
    if runResiduals:
        myTickers = list(set(tickers) - set(myTickers))

    print('scraping ', len(myTickers), 'out of ', len(tickers), ' tickers')
    ScrapeOptionsParallel3(myTickers, NumOfThreads, exitOnZero, ignoreExisting=ignoreExisting)


def KillAllScrapingProcesses():
    global globalProcessArray
    print('Killer process is sleeping')
    time.sleep(100)

    print('Killer process is starting')
    print(len(globalProcessArray))
    for p in globalProcessArray:
        try:
            if p.is_alive():
                p.terminate()
                print('terminating a process......')
        except Exception as e:
            print(e)

#Dec 2, 2016
#This function checks the option counts of scraped data, and compare it against statistics stored in @OptionCount
#Return a list of tickers where the scraped data is significantly less than current statistics
#This list can be used for forced rescraping.
#To have a function to separately generate a list rescraping targets so can re-use existing parallel scraping codes
def CheckOptionCounts(tickerlist, ignoreExisting=True):
    #1. Read @OptionCounts data
    #2. Strip down list to tickers in @OptionCounts
    #3. Generate a common filename
    #4. For each ticker:
    #  4.1 If target file doesn't exist, add to output list
    #  4.2 If exists, compare the option count with stored statistic, add to output list if smaller than a threshold
    #5. Return the ooutput list

    if (tickerlist is None) or (len(tickerlist) < 1):
        return None

    try:
        with open('/Users/Shared/OptionData/@OptionCount.csv', 'r') as csvfile:
            optCount = dict(csv.reader(csvfile))
    except Exception as e:
        print(e)
        return([])

    optCountTickers = optCount.keys()
    tickerlist = list(set(tickerlist) & set(optCountTickers))
    print(len(optCount))
    if (tickerlist is None) or (len(tickerlist) < 1):
        return ([])

    ScrapingDt = datetime.datetime.now(timezone('EST'))
    #OutputFileName = os.path.splitext(os.path.splitext(os.path.basename(CreateOutputFileName('SPY', ScrapingDt)))[0])[0]
    OutputFileName = os.path.basename(CreateOutputFileName('SPY', ScrapingDt))
    print(OutputFileName)

    outputlist = []
    rescrapeThreshold = 0.65
    staticRowThreshold = 400  # the recorded option count target should be sufficiently large to warrant a rescraping

    for ticker in tickerlist:
        targetFile = '/Users/Shared/OptionData/' + ticker + '/'+ OutputFileName
        if (not os.path.isfile(targetFile)):
            outputlist += [ticker]
            continue
        else:
            if (not ignoreExisting):
                continue

        try:
            with io.TextIOWrapper(gzip.open(targetFile, 'r')) as f:
                tmpData = [tuple(line) for line in csv.reader(f)]
        except Exception as e:
            print(e)
            tmpData = None

        if (tmpData is  None) or (len(tmpData) <1):
            outputlist += [ticker]
            continue

        scrapedRows = len(tmpData)
        staticRows = locale.atoi(optCount[ticker])
        #print(ticker, scrapedRows, staticRows, scrapedRows < staticRows*rescrapeThreshold)
        if ((staticRows >= staticRowThreshold) and (scrapedRows < staticRows*rescrapeThreshold)):
            print(ticker, scrapedRows, staticRows, scrapedRows < staticRows * rescrapeThreshold)
            outputlist +=[ticker]

    return outputlist

#CheckOptionCounts(['SPY','QQQ'])
def ReScrapePartiallyCompleted(tickers, ignoreExisting=True):
    RecursiveCutOff = 30
    if IsMarketOpen():
        return
    redolist = list(set(tickers))
    redolist = CheckOptionCounts(redolist, ignoreExisting)
    if (redolist is None) or (len(redolist) < 1 ):
        return
    tgtTickers = GetMyTickers(redolist)
    with open(logfile, 'a') as f:
        f.write('Rescraping ' + str(
            len(tgtTickers)) + ' partially completed tickers\r\n')
    #CrudeDistributedWrapper2(redolist, NumOfThreads, exitOnZero=False, runResiduals=False, ignoreExisting=ignoreExisting)
    ScrapeOptionsParallel3(tgtTickers, NumOfThreads, exitOnZero=False, ignoreExisting=ignoreExisting)
    redolist = list(set(redolist) - set(tgtTickers))
    if (redolist is None) or (len(redolist)<1):
        return
    if IsMarketOpen():
        return
    #redolist = CheckOptionCounts(redolist, ignoreExisting)
    if (len(redolist) <=RecursiveCutOff ) :
        redolist = CheckOptionCounts(redolist, ignoreExisting)
        if (redolist is None) or (len(redolist)<1):
            return
        with open(logfile, 'a') as f:
            f.write('Rescraping ' + str(
                len(redolist)) + ' partially completed tickers\r\n')
        ScrapeOptionsParallel3(redolist, NumOfThreads, exitOnZero=False, ignoreExisting=ignoreExisting)
    else:
        ReScrapePartiallyCompleted(redolist, ignoreExisting)

while True:
    ScrapeOptionInstanceExists = (len(get_pid("phantomjs"))>0)
    if ScrapeOptionInstanceExists:
        print("Scrape Option is already running")

    #read the ExtraTickers files
    logfile = '/Users/Shared/OptionData/@log/' + socket.gethostname() + 'ScrapeOptionLog.txt'

    #with open(logfile, 'a') as f:
    #    f.write('Before loading tickers'+'\r\n')
    ExtraTickers = []
    with open(outputpath+'Models/ExtraUSTickers.csv', 'r') as csvfile:
        tickerreader = csv.reader(csvfile)
        for row in tickerreader:
            ExtraTickers +=row
    ExtraTickers = sorted(list(set(ExtraTickers)))

    #with open(logfile, 'a') as f:
    #    f.write('Before loading Extra 3 tickers'+'\r\n')
    ExtraTickers3 = []
    with open(outputpath+'Models/ExtraUSTickers3.csv', 'r') as csvfile:
        tickerreader = csv.reader(csvfile)
        for row in tickerreader:
            ExtraTickers3 +=row
    ExtraTickers3 = sorted(list(set(ExtraTickers3)))

    #with open(logfile, 'a') as f:
    #    f.write('Before loading Extra 2 tickers'+'\r\n')
    ExtraTickers2 = []
    try:
        with open(outputpath+'Models/ExtraUSTickers2.csv', 'r') as csvfile:
            tickerreader = csv.reader(csvfile)
            for row in tickerreader:
                ExtraTickers2 +=row
    except Exception as e:
        print(e)
    ExtraTickers2 = sorted(list(set(ExtraTickers2)))

    #with open(logfile, 'a') as f:
    #    f.write('Before listing OptionData '+'\r\n')
    ExtraTickers = sorted(list(set(ExtraTickers + ExtraTickers2 + ExtraTickers3)))
    ExistingTickers = list(set(os.listdir('/Users/Shared/OptionData/')))
    ScrapeTickers = list(set(ScrapeTickers + ExtraTickers + ExistingTickers + R1000Tickers + R2000Tickers + PriorityTickers))
    DelistedTickers = []
    try:
        DelistedTickers =  list(set(os.listdir('/Users/Shared/DelistedTickers/')))
    except Exception as e:
        print(e)
    StaleTickers = list(set(StaleTickers + DelistedTickers))

    #Load the OptionCount file and prioritize the top 3000 tickers, or 3500
    #since the scraping process now takes longer and won't be able to scrape all tickers after market close and before marekt opens again
    #make sure the tickers with meaningful traded options are consistently scraped
    #with open(logfile, 'a') as f:
    #    f.write('Before loading OptionCount'+'\r\n')
    top3000=[]
    try:
        with open('/Users/Shared/OptionData/@OptionCount.csv', 'r') as csvfile:
            tmp = [tuple(line) for line in csv.reader(csvfile)]
        optCount = [(line[0], locale.atoi(line[1])) for line in tmp]
        optCount = sorted(optCount, key=itemgetter(1), reverse=True)
        top3000 = [line[0] for line in optCount]
        top3000 = top3000[:900]
    except Exception as e:
        print(e)

    #Seperate into three non-overlapping lists to improve efficiency
    LowPriorityTickers =[]
    ScrapeTickers = list(set(ScrapeTickers) - set(StaleTickers))
    PriorityTickers = list(set(PriorityTickers) - set(StaleTickers))
    if top3000:
        PriorityTickers = list(set(PriorityTickers) | set(top3000[:300]))
    ExistingTickers = list(set(ExistingTickers) - set(StaleTickers))

    PriorityTickers = list(set(PriorityTickers) & set(ExistingTickers)) +["^VIX", "^RUT", "GLD", "SLV", "EWJ", "IBB", "FXI", "XRT", "XLE", "XOP", "OIH", "AMLP", "XLF", "XLV", "XLK", "XLY", "XLI", "XLU", "XLP", "XLRE","XLB", "GDX", "EWZ", "ASHR"]
    PriorityTickers = list(set(PriorityTickers))
    ExistingTickers = list(set(ExistingTickers) - set(PriorityTickers))
    if top3000:
        LowPriorityTickers =  list(set(ExistingTickers) - set(top3000))
        ExistingTickers = list(set(top3000) & set(ExistingTickers))
    ScrapeTickers = list (set(ScrapeTickers) - set(PriorityTickers))
    ScrapeTickers = list(set(ScrapeTickers) - set(ExistingTickers))
    ExistingTickers = list(set(ExistingTickers))

    print(len(PriorityTickers))
    print(len(ExistingTickers))
    if top3000:
        ScrapeTickers = list(set(ScrapeTickers) - set(LowPriorityTickers))
        print(len(LowPriorityTickers))
    print(len(ScrapeTickers))

    if IsMarketOpen():
        break

    NumOfThreads=multiprocessing.cpu_count()
    if (NumOfThreads >=8):
        NumOfThreads -=3
    else:
        if (NumOfThreads == 4):
            NumOfThreads -=1

    #If there are existing ScrapeOptions running, lower the number of processes further to avoid cogestion
    #But nevertheless it is better to re-scan tickers rather than starting multiple instances as it significantly lowers performance
    if ScrapeOptionInstanceExists:
        if NumOfThreads > 4:
            NumOfThreads = 3
        else:
            NumOfThreads = 2

    start_time = time.time()

    with open(logfile, 'a') as f:
        f.write('Starting at ' + str(datetime.datetime.now())+'\r\n')


    CrudeDistributedWrapper2(PriorityTickers, NumOfThreads)
    with open(logfile, 'a') as f:
        f.write('Batch 1 finishing at ' + str(datetime.datetime.now()) + ', using ' + str((time.time()-start_time)/60) + ' minutes' + '\r\n')
    start_time = time.time()
    CrudeDistributedWrapper2(ExistingTickers, NumOfThreads)
    with open(logfile, 'a') as f:
        f.write('Batch 2 finishing at ' + str(datetime.datetime.now()) + ', using ' + str((time.time()-start_time)/60) + ' minutes' + '\r\n')
    start_time = time.time()
    #CrudeDistributedWrapper2(PriorityTickers , NumOfThreads, False, True)

    #Scrape main tickers for all workstations
    #ScrapeOptionsParallel3(PriorityTickers, NumOfThreads)
    #ScrapeOptionsParallel3(ExistingTickers, NumOfThreads)
    ReScrapePartiallyCompleted(list(set(PriorityTickers)) + list(set(ExistingTickers)), ignoreExisting=False)
    ReScrapePartiallyCompleted(list(set(PriorityTickers)) + list(set(ExistingTickers)))

    if top3000:
        CrudeDistributedWrapper2(LowPriorityTickers, NumOfThreads)
        with open(logfile, 'a') as f:
            f.write('Batch 3 finishing at ' + str(datetime.datetime.now()) + ', using ' + str(
                (time.time() - start_time) / 60) + ' minutes' + '\r\n')

    if top3000:
        CrudeDistributedWrapper2(LowPriorityTickers, NumOfThreads, False, True)

    if ScrapeOptionInstanceExists:
        break

    #ReScrapePartiallyCompleted(list(set(PriorityTickers)) + list(set(ExistingTickers)))
    if IsMarketOpen():
        break
    CrudeDistributedWrapper2(PriorityTickers+ExistingTickers+LowPriorityTickers, NumOfThreads)
    CrudeDistributedWrapper2(PriorityTickers+ExistingTickers+LowPriorityTickers, NumOfThreads, False, True)

    if IsMarketOpen():
        break
    CrudeDistributedWrapper2(ScrapeTickers, NumOfThreads)
    CrudeDistributedWrapper2(ScrapeTickers, NumOfThreads, False, True)
    break






