#!/usr/local/bin/python3

from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import re
import csv
import os
import pandas as pd
from urllib.error import URLError, HTTPError
from operator import itemgetter


outputpath = '/Users/Shared/'

################################################################
##Known issues:
##  1. When removing non-ascii code, all Chinese names are also removed
##  2. The output csv file is encoded in utf-8, which may not be necessary now that non-ascii codes are removed
##  3. I assume that holding information, if exists, exists for both previous and present columns. Occassionally this is not the case
##     A warning message is displayed stating that the line is a bit strange. The particular line item is thrown out
##  4. For "Reasons for Disclosure" and "Number of shares involved" columns, I assume there is only one entry. Occasionally
##     there are two or more entries (long/short/pool etc). Only the first one is currently recorded. The entities report multiple
##     simutaneous buying/selling of one underlying are usually brokers
##  5. I screen and scrape the daily summary pages. HKEx seems to keep only 5-day history of summaries


##Daily Summary of Disclosure of Interests from HKEx
## Individual and corporate substantial shareholders: 
##http://sdinotice.hkex.com.hk/di/summary/DSM20141110C1.htm

## Directors interests in shares of the listed corporation
##http://sdinotice.hkex.com.hk/di/summary/DSM20141110C2.htm

#print(scrapingdts)

##This function sorts the scraped table by reporting code and add
## entries to the respective files organized by reporting codes 
def SortbyReportingCode(filename):
    subdirname = 'Investors' if ('Investors' in filename) else 'Insiders'
    #get all reporting codes:

    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            rptcodes = []
            for row in reader:
                rptcodes = rptcodes + [row[3]]
            rptcodes = list(set(rptcodes))

        for rptcode in rptcodes:
            outputfile = outputpath + 'HKEx/' + subdirname + '/' + rptcode +'.csv'
            with open(outputfile, 'a', encoding='utf-8') as fw:
                writer = csv.writer(fw)
                with open(filename, 'r') as fr:
                    reader = csv.reader(fr)
                    for row in reader:
                        if (row[3]==rptcode):
                            writer.writerow(row)

            with open(outputfile, 'r', encoding='utf-8') as fw:
                tmpData = [tuple(line) for line in csv.reader(fw)]
                tmpData = list(set(tmpData))
                tmpData = sorted(tmpData, key=itemgetter(2,0))
            with open(outputfile, 'w', encoding='utf-8') as fw:
                writer = csv.writer(fw)
                writer.writerows(tmpData)

    
## 2017.07.15: HKEx changed its summary format from 2017.07.01 and transaction code
## Switch to a different function for saving the output.
## This function is no longer used.
def DumpTable(onetable,filename):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        newtable=[]
        for thisline in onetable:
            if (len(thisline)==9):
                newline=thisline[0:7]+('','','','')+thisline[7:9]+('','','','')
            elif (len(thisline)==13):
                newline=thisline[0:9]+('','')+thisline[9:13]+('','')
            elif (len(thisline)==17):
                newline=thisline
            else:
                print(str(thisline)+' is a bit strange.')
                newline=None
            #print(newline)
            if (not newline is None):
                newtable=newtable+[newline]
        if (len(newtable)>0):
            with open(filename, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(newtable)

## 2017.07.15: HKEx changed the format of summary from 2017.07.01 onward
## Use DumpTable2 to save the output
def DumpTable2(onetable, filename):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        with open(filename, 'w', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(onetable)



def ProcessSummaryLine(entries):
    thisline = ()

    for i in range(0, len(entries)):
        ## 2017.07.13
        ## skip the 1st entry: link to the full filing (i = 0)
        ## skip the 2nd entry (company name)  (i = 1)
        ## skip the 4th entry (security type) (i = 3)
        thisitem = None
        #print(i, len(entries))
        #print(entries[i])
        #print(len(entries[i]))
        #print(entries[i].content)
        if (i != 0) & (i != 1) & (i != 3) & (len(entries[i]) > 0 ):
            if (entries[i].contents[0].name == 'table'):
                #print('do nothing')
                continue
            elif (entries[i].contents is not None):
                filingdata = (entries[i].findAll(text=True, recursive=True))
                for onedataitem in filingdata:
                    thisline += (onedataitem,)

        if (i == 5):
            thisline += ('DisclosureCode',)
        elif (i == 6):
            thisline += ('NumofShares',)
        elif (i==7) :
            thisline += ('AvgPrice',)
        elif (i==8):
            thisline += ('PrePostShares',)
        #print(thisline)


    return(thisline)


def ScrapeOneSummary(tabID, scrapingdt):
    dsmurl = 'http://sdinotice.hkex.com.hk/di/summary/DSM'
    summaryURL = dsmurl+scrapingdt.strftime('%Y%m%d')+'C'+str(tabID)+'.htm'
    outputfile = outputpath + 'HKEx/' +('Investors' if (tabID==1) else 'Insiders') +'/' + scrapingdt.strftime('%Y%m%d') + '.csv'
    print(summaryURL)
    print(outputfile)

    if os.path.isfile(outputfile):
        print(outputfile + ' already exists!')
    else:
        try:
            summaryPage = urlopen(summaryURL, timeout=10)
            soup = BeautifulSoup(summaryPage)
            #summarytable = soup.find('table', id= "Table3")
            #datarows = summarytable.findAll('tr', attrs={'bgcolor':re.compile(r'#ffffff|#cccccc')})
            datarows = soup.findAll('tr', attrs={'bgcolor': re.compile(r'#ffffff|#cccccc')})
            #print(len(datarows))
            thistable = []
            for onerow in datarows:
                entries=onerow.findAll('td', {'class' : 'txt'})
                thisline = ProcessSummaryLine(entries)
                thistable=thistable+[thisline]
                #print(len(thisline))
            print(thistable)
            DumpTable2(thistable, outputfile)
            #SortbyReportingCode(outputfile)
        except HTTPError as e:
            print('The server couldn\'t fulfill the request.')
            print('Error code: ', e.code)
        except URLError as e:
            print('We failed to reach a server.')
            print('Reason: ', e.reason)
        except Exception as e:
            print('Unknown exception raised.')
            print(e)

i = 15;
scrapingdts = pd.bdate_range(end=datetime.datetime.now(), periods=i).tolist()
#print(scrapingdts)
for tabID in [1,2]:
    for scrapingdt in scrapingdts:
        ScrapeOneSummary(tabID, scrapingdt)
#        break

#testdt = pd.Timestamp('2017-07-13 00:00:00')
#ScrapeOneSummary(1, testdt)
            
        
    
