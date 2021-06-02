#!/usr/local/bin/python3.4

##Sample: 
##https://adesquared.wordpress.com/2013/06/16/using-python-beautifulsoup-to-scrape-a-wikipedia-table/

from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import re
from dateutil import parser
import csv
import os
from urllib.error import URLError, HTTPError
from pytz import timezone
import pytz
import locale
import time
import subprocess
from glob import glob
#from bdateutil import relativedelta
#import holidays
import multiprocessing
from collections import Counter
from dateutil.relativedelta import relativedelta



isdst_now_in = lambda zonename: bool(datetime.datetime.now(pytz.timezone(zonename)).dst())
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )

#This is to create the 3-line header that preceeds a 13F filing that will be copied over when scraping text filings
Header3Line=[]
inputfile = '/Users/Shared/Fund Clone/Point72/20160630.csv'
with open(inputfile, encoding='utf-8') as f:
    tmpData = [tuple(line) for line in csv.reader(f)]
for i in range(3):
    line = list(tmpData[i])
    #print(line)
    line = line[:(len(line)-1)]
    #print(line)
    Header3Line += [tuple(line)]
#print(Header3Line)

outputpath = '/Users/Shared/'
coreCIKfile = '/Users/Shared/Models/CIKs.csv'
CIKfile = '/Users/Shared/Models/fullCIKs.csv'
skipCIKfile = '/Users/Shared/Models/skipCIKs.csv'
MonitorFundFile = '/Users/Shared/SC13Monitor/MonitorFunds.csv'

with open(MonitorFundFile) as f:
    tmpData = [line for line in csv.reader(f)]
MonitorFunds = [line for oneline in tmpData for line in oneline ]
#print(tmpData)



################################################################
##This script downloads a fund manager's 13F filing
##Manually copy over the filing's address
##The script scrapes the main data table into a csv file for further processing
##userid = getpass.getuser()
##if (userid == 'mintang'):
##    outputpath = '/Users/Shared/'
##else:
##    outputpath = '/Volumes/Data/Shared/'


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
            #nextDt = currDt + relativedelta(bdays=1,holidays=holidays.US())
            nextDt = currDt + relativedelta(days=1)
            
        else:
            nextDt = currDt
        nextDt = nextDt.replace(hour=MktOpenHour, minute=0)
        print('wait until')
        print(nextDt)
        return((nextDt - currDt).total_seconds())
    else:
        nextDt = currDt
        nextDt =  nextDt.replace(hour=MktCloseHour, minute=0)
        print('wait until')
        print(nextDt)
        return((nextDt - currDt).total_seconds())

##Note on Feb 25, 2015:
##Add the step of clean up the string from the data table before dumping to a file
##The step removes invisible characters, trailing spaces and comma (,) in numbers

##Note on Mar 4, 2015:
##To handle the amendaments to filings, scan filings in two steps:
##Step 1: scan the filing list as it is loaded, i.e. lastest will appear first
##        process only 13F-HR and skip all 13F-HR/A. The Update flag is still in use
##        during the update mode, the scan stops when a scraped filing is encountered
##Step 2: scan the filing list in reverse order and process only 13F-HR/A filings
##        and always dump the data table (either dump new ones or append to existing ones)



def CleanString(inputStr):
    if not inputStr:
        return('')
    else:
        p = re.compile('\s')
        inputStr = p.sub(' ',inputStr)
        p = re.compile(',')
        inputStr = p.sub('',inputStr)
        inputStr = inputStr.strip()
        return(inputStr)

def CreateFundDirectory(fundname):
    outputdir = outputpath + 'Fund Clone/' +fundname
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
   
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

def CreateDirectory(dirname):
    if not os.path.exists(dirname):
        os.makedirs(dirname)

def DumpTable(onetable,filename, replaceDuplicate=False):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        newtable = []
        newidx = 0 ;
        locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
        addedCUSIPs = []
        for line in onetable:
            if (newidx <=2):
                newtable += [line]
                newidx += 1
            else:
                if (line[2] != "000000000"):  ## skip lines with CUSIP being "000000000"
                    if (newidx <=0):
                        newtable = newtable+[line]
                        newidx += 1
                        addedCUSIPs += [line[2]]
                    else:
                        ## Consolidate line items of the same CUSIP and put/call flag
                        ## Check if the combination of CUSIP and put/call flag is already
                        ## in newtable. If yes, then add the shares/dollar amount to the existing line

                        ## Some funds would list the same underlying as different line items for different managers
                        ## Because the 13F forms are usually ordered alphabetically by the name of the issuer,
                        ## such line items tend to show up together. But it can get more complicated when there are
                        ## multiple call and put option lines, e.g. in BlackRock filings

                        ## I look back 700 lines in this process as a compromise between computation and comprehensiveness
                        ## Maybe next time I should keep track of all added CUSIPs and run an extensive search if there is a match
                        matchedline = False
                        j = 1

                        ##check CUSIP is indeed 9 characters
                        ##Add preceeding 0s if shorter than 9
                        while (len(line[2]) <9):
                            print(line[2])
                            line[2] = "0"+line[2]
                        #print(line)

                        if (line[2] in addedCUSIPs):
                            while (j < newidx):
                                if (newtable[newidx-j][2] == line[2]) and (newtable[newidx-j][6]==line[6]):
                                    matchedline = True
                                    if (replaceDuplicate):
                                        line1Dt = newtable[newidx-j][len(newtable[newidx-j])-1]
                                        line2Dt = line[len(line)-1]
                                        ##Add on May 15, 2015
                                        ##When remove duplicated lines, keep the one with a later timestamp
                                        if (line2Dt > line1Dt):
                                            newtable.pop(newidx-j)
                                            matchedline = False
                                            newidx -= 1
                                        else:
                                            matchedline = False
                                    else:
                                        #September 2, 2016
                                        #It seems that the tuple doesn't support item assignment error finally catches up with me so changed to below
                                        tmpnewline = list(newtable[newidx-j])
                                        tmpnewline[3] = str(locale.atof(tmpnewline[3]) + locale.atof(line[3]))
                                        tmpnewline[4] = str(locale.atof(tmpnewline[4]) + locale.atof(line[4]))
                                        tmpnewline[9] = str(locale.atof(tmpnewline[9]) + locale.atof(line[9]))
                                        tmpnewline[10] = str(locale.atof(tmpnewline[10]) + locale.atof(line[10]))
                                        newtable[newidx - j] = tuple(tmpnewline)
                                        #newtable[newidx-j][3] = str(locale.atof(newtable[newidx-j][3]) + locale.atof(line[3]))
                                        #newtable[newidx-j][4] = str(locale.atof(newtable[newidx-j][4]) + locale.atof(line[4]))
                                        #newtable[newidx-j][9] = str(locale.atof(newtable[newidx-j][9]) + locale.atof(line[9]))
                                        #newtable[newidx-j][10] = str(locale.atof(newtable[newidx-j][10]) + locale.atof(line[10]))
                                    break;
                                j += 1
                        if (not matchedline):
                            #line[0] = CleanString2(line[0])
                            newtable += [line]
                            newidx += 1
                            addedCUSIPs += [line[2]]

        if (len(newtable)<=3):
            return(None)

        outputpath = os.path.dirname(os.path.realpath(filename))
        CreateDirectory(outputpath)

        if not CompareData2CSV(newtable, filename):
            with open(filename, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(newtable)

        return(newtable)





def ReadTextFiling(dataurl):
    try:
        datapage = urlopen(dataurl)
        time.sleep(3)
        filingdata = [line for line in datapage]
        return(filingdata)
    except Exception as e:
        print(e)
        return None
#ReadTextFiling(testurl)

#Find the most common position for "SH" or 'PRN"
def PositionSH(dat):
    #print(dat)
    posList = [re.search(r'\s(SH|PRN)[\s|\n]', line, re.M) for line in dat]
    posList = [result for result in posList if result is not None]
    if posList and (len(posList)>=1):
        posList = [result.start(1) for result in posList]
        if (posList is None) or (len(posList) < 1):
            return (None)
        else:
            return (Counter(posList).most_common(1)[0][0])
    else:
        return None

#Find the most common position for "COM"
def PositionCOM(dat):
    posList = [re.search(r' (COM|CL|Common|Option) ', line).start(1) for line in dat]
    if (posList is None) or (len(posList) < 1):
        return(None)
    else:
        return (Counter(posList).most_common(1)[0][0])

#Find the most common position for a nine-char alpha digit string
#In 13FCusip there is a script that examines all the CUSIPs of existing filings. Unfortunately they don't necessarily conform to the rules set out by CUSIP.org
def PositionCUSIP(dat):
    posList = [re.search(r' ([0-9a-zA-Z]{9}) ', line) for line in dat]
    if (posList is None) or (len(posList) < 1):
        return (None)
    else:
        return (Counter(posList).most_common(1)[0][0])

## Find all lines that have a 9-char alpha-digit string, followed by 1 or 2 digit strings, followed by SH/PRN/Call/Put, as a possible data entry
## then process the entries line by line
def ParseTextFiling3(filingtext, dataurl, formdt):
    #1. Find all lines with SH/PRN/Call/Put
    #2. For such lines, fine 9-char alpha/digit strings
    #3. Keep only the lines that have both SH/PRN/Call/Put and
    p = re.compile('\$')
    filingtext = [p.sub(' ', line.upper()) for line in filingtext]

    cusipPosList = [(re.search(r'\s([0-9a-zA-Z]{9})\s+([-0-9\,\._]+\s+){1,2}(SH|PRN|CALL|PUT|WTS|X|SHRS|C|P|SOLE)\s', line), line) for line in filingtext]
    if (cusipPosList is None) or (len(cusipPosList) < 1):
        return(None)
    filingdat = [tuple(line) for line in cusipPosList if (line[0] is not None)]
    if (filingdat is None) or (len(filingdat) < 1):
        return(None)
    filingdat = [(re.search(r'\s(SH|PRN|CALL|PUT|WTS|X|SHRS|SOLE|C|P)\s', line[1][(line[0].start()+10):]),) + line for line in filingdat]
    #print(len(filingdat))
    #for line in filingdat:
    #   print(line)

    tmpline = filingdat[0]
    cusipStart = tmpline[1].start()+1
    shStart = tmpline[0].start() + tmpline[1].start() + 10
    shEnd = tmpline[0].end()-tmpline[0].start()+shStart-1
    datline = tmpline[2]
    #print(datline)
    #print(datline[:cusipStart])
    #print(datline[cusipStart:(cusipStart+9)])
    #print(datline[(cusipStart+9):shStart])
    #print(datline[shStart:shEnd])
    #print(datline[shEnd:])

    outputDat = []
    for line in filingdat:
        datline = line[2]
        cusipStart = line[1].start()+1
        shStart = line[0].start()+cusipStart+10
        shEnd = line[0].end()-line[0].start() + shStart-1

        #print(cusipStart)
        #print(shStart)
        #print(shEnd)
        #for i, x in enumerate(datline):
        #    print(i, x, ord(x))

        issuerName = CleanString(datline[:cusipStart].strip())
        issuerCUSIP = CleanString(datline[cusipStart:(cusipStart+9)].strip()).upper()
        shType = CleanString(datline[shStart:shEnd].strip()).upper()
        if (shType =='WTX') or (shType=='X') or (shType=='SHRS') or (shType=='SOLE'):
            shType = 'SH'
        residualLine = datline[shEnd:].strip().upper()
        if (shType=='SH'):
            if ('CALL' in residualLine):
                optType = 'Call'
            else:
                if ('PUT' in residualLine):
                    optType = 'Put'
                else:
                    optType=''
        else:
            if (shType=='PRN'):
                optType = ''
            else:
                optType=shType
                if (optType == "C") or (optType=="CALL"):
                    optType = 'Call'
                else:
                    optType = 'Put'
                shType = 'SH'

        shareAmts = CleanString(datline[(cusipStart+9):(shStart-1)].strip()).split()
        shareValue = shareAmts[0]
        try:
            shareValueTest = locale.atof(shareValue.strip())
        except Exception as e:
            if (shareValue == '-') or (shareValue == '--'):
                shareValue = '0'
            else:
                if (shareValue == '2860__'):
                    shareValue = '286000'
                else:
                    print(line)
                    print(e)
                    return (None)

        if (len(shareAmts) == 2):
            shareNum = shareAmts[1]
            try:
                shareNumTest = locale.atof(shareNum.strip())
            except Exception as e:
                if (shareNum == '-') or (shareNum == '--'):
                    shareNum = '0'
                else:
                    print(line)
                    print(e)
                    return (None)
        else:
            shareNum = '0'
        outputDat += [(issuerName, '', issuerCUSIP, shareValue, shareNum, shType, optType, '', '', '0',
                        '0', '0', formdt)]
    return(outputDat)






def ScrapeTextFilingData(dataurl, outputfile, fundname, formdt):
    #1. Find all lines with 'SH' or 'PRN' surrounded by tabs or spaces
    #2. Find out the length of such lines, probably keep only the lines with
    #3. Maybe keep just the lines a) with space-surrounded 'SH' or 'PRN', and b) with the most common line length?
    #4. Really just try to extract: CUSIP, share number, share amt, 'SH' or 'PRN', Put/Call, other information not that important
    logfile = '/Users/Shared/Text13FScrapingLog.txt'
    data13F = ReadTextFiling(dataurl)
    tmplines = [str(line, 'utf-8') for line in data13F]
    formdt = str(formdt)
    X = ParseTextFiling3(tmplines, dataurl, formdt)
    if (X is None) or (len(X) < 1):
        print("Error scraping ", fundname, ' ', formdt, ' ', dataurl)
        if (fundname in MonitorFunds):
            with open(logfile, 'a') as f:
                f.write("Error scraping " + fundname + ' ' +  formdt +  ' , ' + dataurl + '\r\n')
    else:
        outputHeader = [tuple(line) + (formdt,) for line in Header3Line]
        X = outputHeader + X
        DumpTable(X, outputfile)
    return(X)

def ReadAmendamentType(dataurl):
    data13F = ReadTextFiling(dataurl)
    tmplines = [str(line, 'utf-8') for line in data13F]
    restateTag = r'\s\[\s*(X|x)\s*\]\s*is\s*a\s*restatement'
    addnewTag = r'\s\[\s*(X|x)\s*\]\s*adds\s*new'

    restateLine = [re.search(restateTag, line) for line in tmplines]
    restateLine = [line for line in restateLine if not (line is None)]
    if restateLine and (len(restateLine)>=1):
        return('Restatement')

    addnewLine = [re.search(addnewTag, line) for line in tmplines]
    addnewLine = [line for line in addnewLine if not (line is None)]
    if addnewLine and (len(addnewLine)>=1):
        return('Add New')
    else:
        return('Not sure')

#tmpurl = 'https://www.sec.gov/Archives/edgar/data/921669/000092847504000019/restatedcci.txt'
#tmpurl = 'https://www.sec.gov/Archives/edgar/data/934639/000131586306000158/mv26a.txt'
#print(ReadAmendamentType(tmpurl))


def ScrapeFilingData(dataurl, outputfile, fundname, formdt):
    summaryURL = dataurl
    ##print(dataurl)
    try:
        summaryPage = urlopen(summaryURL)
        time.sleep(4)
        soup = BeautifulSoup(summaryPage.read().decode('utf-8', 'ignore'))
        summarytable = soup.find('table', attrs={'summary': "Form 13F-NT Header Information"})
        datarows = summarytable.findAll('tr')
        thistable = []
        for onerow in datarows:
            thisline = ()
            for cell in onerow.findAll('td'):
                if (len(cell.contents)>0):
                    thisline=thisline+(CleanString(cell.contents[0]),)
                else:
                    thisline=thisline+('',)
            #September 2, 2016, convert CUSIP to upper case
            thisline = list(thisline)
            thisline[2] = thisline[2].upper()
            thisline = tuple(thisline)
            #END September 2, 2016
            thisline = thisline + (str(formdt),)
            thistable=thistable+[list(thisline)]
        return(DumpTable(thistable, outputfile, False))
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
    except Exception as e:
        print('Error occured when scraping for ' + fundname)
        print(e)


def ScrapeAmendmentDoc(filinglink):
    filingurl = filinglink
    ##print(filingurl)
    try:
        filingpage = urlopen(filingurl)
        time.sleep(4)
        soup = BeautifulSoup(filingpage)
        summarytable = soup.find('table', attrs={'summary': 'Amendment Information'})
        datarows = summarytable.findAll('tr')
        restatementrow = datarows[1]
        addnewdatarow = datarows[2]
        restatementtag = restatementrow.find('td', attrs={'class': 'CheckBox'})
        addnewdatatag = addnewdatarow.find('td', attrs={'class': 'CheckBox'})
        if (re.match('X', restatementtag.get_text().strip())):
            return True
        else:
            return False
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return None
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
        return None
    except Exception as e:
        print('Error occured when scraping for ' + filingurl)
        print(e)
        return None
    

def Scraped13FTimeStamp(filename):
    try:
        with open(filename, 'r') as csvfile:
            scrapedData = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(filename + ' does not exist.')
        return None

    if not scrapedData:
        return None

    filingDts = set()
    for line in scrapedData:
        filingDts.add(line[len(line)-1])
    filingDts = list(filingDts)
    lastDt = None
    for curDt in filingDts:
        curDt = parser.parse(curDt)
        if not lastDt:
            lastDt = curDt
        else:
            if (lastDt < curDt):
                lastDt = curDt

    print(lastDt)
    return(lastDt)
    
    

def ScrapeOneFiling(fundname, filinglink, isAmendament=False):
    filingurl = 'http://www.sec.gov'+filinglink
    ##print(filingurl)
    try:
        filingpage = urlopen(filingurl)
        time.sleep(10)
        soup = BeautifulSoup(filingpage)
        tmpdata = soup.findAll('div', attrs={'class' : "formGrouping"})
        formdt = parser.parse(tmpdata[0].findAll('div', attrs={'class' : "info"})[1].contents[0])
        rptdt = parser.parse(tmpdata[1].findAll('div', attrs={'class' : "info"})[0].contents[0])
        outputfile = outputpath + 'Fund Clone/' +fundname +'/'+rptdt.strftime('%Y%m%d')+'.csv'
        #print(tmpdata)
        #print(formdt)
        #print(rptdt)
            

        if (not isAmendament):
            if os.path.isfile(outputfile):
                outputfiledt = datetime.datetime.fromtimestamp(os.stat(outputfile).st_mtime)
                if (outputfiledt > formdt):
                    print(outputfile + ' exists and is scraped after the form is filed.')
                    return None

        ### Add on May 15, 2015
        ##If the scraping file of the same quarter already exists
        ##Then check the latest time stamp of the line item
        ## compare that time stamp to the formdt of the filing to be scraped
        ##Scrape only if the formdt is later than the latest time stamp
        if os.path.isfile(outputfile):
            outputtimestamp = Scraped13FTimeStamp(outputfile)
            if outputtimestamp:
                if (outputtimestamp >= formdt):
                    print(outputfile + ' exists and is scraped after the amendament form is filed.')
                    return None

        
        filingtable = soup.find('table', attrs={'class' : "tableFile"})
        lines = filingtable.findAll('tr')

        ##the filing page contains a table in which there are five line items, one of which is the data table
        ##html that we'll scrape
        ##Earlier filing page contains a table which links only to a text/xml page which we'll ignore for now
        if (len(lines) == 6):
            if (isAmendament):
                #print(filinglink)
                isRestatement=False
                isReStatement = ScrapeAmendmentDoc('http://www.sec.gov'+lines[1].findAll('a')[0]['href'])
                if (isReStatement):
                    if os.path.isfile(outputfile):
                        tmpoutputfile = outputpath + 'Fund Clone/@backup/tmptmp' + fundname +'.csv'
                        newdata = ScrapeFilingData('http://www.sec.gov'+lines[3].findAll('a')[0]['href'],tmpoutputfile, fundname, formdt)
                        with open(outputfile, 'r') as csvfile:
                            scrapedData = [tuple(line) for line in csv.reader(csvfile)]

                        ## if the restatement data has less rows than the scrapedData, append rather than replace
                        ## unfortunately some funds (e.g. BlueCrest for 20140930, filed an empty restatement in Feb2015
                        if (len(newdata) < len(scrapedData)):
                            scrapedData += newdata[3:]
                            DumpTable(scrapedData, outputfile, replaceDuplicate=True)
                        else:
                            DumpTable(newdata, outputfile)
                    else:
                        ScrapeFilingData('http://www.sec.gov'+lines[3].findAll('a')[0]['href'],outputfile, fundname, formdt)
                else:
                    if os.path.isfile(outputfile):
                        tmpoutputfile = outputpath + 'Fund Clone/@backup/tmptmp' + fundname + '.csv'
                        newdata = ScrapeFilingData('http://www.sec.gov'+lines[3].findAll('a')[0]['href'],tmpoutputfile, fundname, formdt)
                        with open(outputfile, 'r') as csvfile:
                            scrapedData = [tuple(line) for line in csv.reader(csvfile)]
                        scrapedData += newdata[3:]
                        ## scrapedData should have been processed to remove all duplicated rows
                        ## when amendament is filed with duplicated CUSIPs,
                        ## it is meant to replace the rows in the original filing
                        DumpTable(scrapedData, outputfile, replaceDuplicate=True)
                    else:
                        print('No scraped filing data to add new underlyings to. Run verbose mode first.')
                            
            else:
                ScrapeFilingData('http://www.sec.gov'+lines[3].findAll('a')[0]['href'],outputfile, fundname, formdt)
            return 1
        else:
            alllinks =[]
            for line in lines[1:]:
                if line.findAll('a'):
                    tmplink = line.findAll('a')[0]['href']
                    tmplink.strip()
                    #add this to remove hidden href ends with "/"
                    #E.G in the link https://www.sec.gov/Archives/edgar/data/934639/000094130200000175/0000941302-00-000175-index.htm
                    if (tmplink[(len(tmplink)-1)] =='/'):
                        continue
                    if tmplink:
                        alllinks += [tmplink]
                        #with open('/Users/Shared/text13F.csv', "a") as myfile:
                        #    myfile.write(fundname+','+ str(rptdt) +',' + 'http://www.sec.gov' +tmplink+'\r\n')
                        #textFilingCheck = [line.find(".txt$") for line in tmplink]
                        #print(textFilingCheck)
            if (alllinks) and (len(alllinks)>0):
                alltxtlinks = [line[(len(line)-4):]=='.txt' for line in alllinks]
                alltxtlinks = sorted(list(set(alltxtlinks)))
                if (len(alltxtlinks)>1) or (not alltxtlinks[0]):
                    print( fundname, rptdt, ' not all txt filings')
                    return None
            #Don't process text Amendaments yet
            if isAmendament:
                #1. scan the filing for type: re-statement or add new holdings
                #2. if add new holdings: scrape to a tmp file, add to existing data, remove tmp file
                #3. if cannot decide for type, scrape to tmp file and compare to existing file: longer than current, replace it, shorter than half, add else raise a warning message
                #print('here')
                amendType = ReadAmendamentType('http://www.sec.gov'+alllinks[0])
                #print(amendType, 'http://www.sec.gov'+alllinks[0])
                #print(amendType, 'http://www.sec.gov'+alllinks[0] )
                #print('here')
                if (amendType =='Add New'):
                    if os.path.isfile(outputfile):
                        tmpfilename = outputpath + 'Fund Clone/@backup/tmptmp' + fundname + '.csv'
                        newdata = ScrapeTextFilingData('http://www.sec.gov'+alllinks[0], tmpfilename, fundname, formdt)
                        with open(outputfile, 'r') as csvfile:
                            scrapedData = [tuple(line) for line in csv.reader(csvfile)]

                        if (2*len(newdata) < len(scrapedData)):
                            scrapedData += newdata[3:]
                            DumpTable(scrapedData, outputfile, replaceDuplicate=True)
                        else:
                            DumpTable(newdata, outputfile)
                    else:
                        print('No scraped filing data to add new underlyings to. Run verbose mode first.')
                    return(1)
                else: # if it is a restatement or we are not sure
                    #sometimes an add-new filing can be mistakenly tagged as a restatement so we still check
                    # if the new statement has less than half of the lines of the original filing, treat it as add-new

                    tmpfilename = outputpath + 'Fund Clone/@backup/tmptmp' + fundname + '.csv'
                    newdata = ScrapeTextFilingData('http://www.sec.gov' + alllinks[0], tmpfilename, fundname,
                                                   formdt)
                    if os.path.isfile(outputfile):
                        with open(outputfile, 'r') as csvfile:
                            scrapedData = [tuple(line) for line in csv.reader(csvfile)]

                        if (2*len(newdata) < len(scrapedData)):
                            print(len(newdata))
                            scrapedData += newdata[3:]
                            DumpTable(scrapedData, outputfile, replaceDuplicate=True)
                        else:
                            DumpTable(newdata, outputfile)
                    else:
                        DumpTable(newdata, outputfile)
                    return(1)




            else:
                ScrapeTextFilingData('http://www.sec.gov'+alllinks[0], outputfile, fundname, formdt)
            return(1)
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return 1
    except URLError as e:
        print('We failed to reach a server.')
        print('Reason: ', e.reason)
        return 1
    except Exception as e:
        print('Error occured when scraping for ' + fundname)
        print(e)
        return 1


#ScrapeOneFiling('foo', '/Archives/edgar/data/1451928/000095012311098619/0000950123-11-098619-index.htm')

def ScrapeOneFund(fundname, CIK, UpdateMode=True):
    fundurl = 'http://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK='+CIK+'&type=13F-HR&owner=exclude&count=100'
    ##print(fundurl)
    print(fundname)
    try:
        fundpage = urlopen(fundurl)
        time.sleep(5)
        #print(fundurl)
        soup = BeautifulSoup(fundpage)
        #print(soup)
        filingstable = soup.find('table', attrs={'class':"tableFile2"})
        outputdir = outputpath + 'Fund Clone/' +fundname
        try:
            if os.path.exists(outputdir):
                fundcsvfiles = glob(outputdir +'/*.csv')
                if not fundcsvfiles:
                    os.rmdir(outputdir)
        except Exception as e:
            print(e)

        #print(filingstable)
        filingctr = 0 
        filinglist = filingstable.findAll('tr')
        for onefiling in filinglist:
            cells = onefiling.findAll('td')
            filingctr += 1 
            #print(onefiling)
            if (len(cells) > 0 ):
                if (re.match('13F-HR', cells[0].contents[0])):
                    if (not re.match('13F-HR/A', cells[0].contents[0])):
                        if ((ScrapeOneFiling(fundname,cells[1].findAll('a')[0]['href'], isAmendament=False) is None)):
                            if (UpdateMode):
                                break

        print('Processing amendaments......')
        if (UpdateMode):
            amendamentlist = reversed(filinglist[0:filingctr])
        else:
            amendamentlist = reversed(filinglist)
        #print(amendamentlist)
        for onefiling in amendamentlist:
            cells = onefiling.findAll('td')
            #print(onefiling)
            if (len(cells) > 0 ):
                if (re.match('13F-HR/A', cells[0].contents[0])):
                    #print(cells[0].contents[0])
                    ScrapeOneFiling(fundname,cells[1].findAll('a')[0]['href'], isAmendament=True)
                    time.sleep(2)
                            
    except HTTPError as e:
        print('The server couldn\'t fulfill the request for ' + fundname)
        print('Error code: ', e.code)
    except URLError as e:
        print('We failed to reach a server for ' + fundname)
        print('Reason: ', e.reason)
    except Exception as e:
        print('Error occured when scraping for ' + fundname)
        print(e)
        


##Scraped13FTimeStamp('/Users/shared/fund clone/3GCapitalPartnersLP/20141231.csv')

def FullScrapeAll():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]
    TargetFunds = list(set(TargetFunds))
    for fund in TargetFunds:
        ScrapeOneFund(fund[0], fund[1], False)
    
def ScrapeFunds(fundlist, UpdateMode=True):
    for fund in fundlist:
        ScrapeOneFund(fund[0], fund[1], UpdateMode)

def ScrapeFundsParalle(fundlist, NumOfThreads=4, UpdateMode=True, waittime=-1):
    if not fundlist:
        return None

    fundlist = list(set(fundlist))
    if (NumOfThreads < 1):
        NumOfThreads = 2
    if (NumOfThreads > 16):
        NumOfThreads = 16
    threads = [None] * NumOfThreads
    results = [0] * NumOfThreads
    out_q = multiprocessing.Queue()
    # print(ScrapeTickers[:10])
    # ScrapeTickers[:5]
    # t = threading.Thread(target=OptionScrape, args=("^VIX",))
    # t.start()
    # for ticker in ScrapeTickers:
    #         total_options_scraped +=OptionScrape(ticker)
    # print(str(total_options_scraped) + ' options scraped.' )
    fundListLen = len(fundlist) // NumOfThreads

    for i in range(len(threads)):
        startIdx = i * fundListLen
        if (i == (len(threads) - 1)):
            endIdx = len(fundlist)
        else:
            endIdx = (i+1) * fundListLen -1
        # threads[i] = threading.Thread(target=OptionScrapeBatch, args=(ScrapeTickers[startIdx:endIdx], results, i))
        # threads[i].start()
        threads[i] = multiprocessing.Process(target=ScrapeFunds,
                                             args=(fundlist[startIdx:(endIdx+1)], UpdateMode))
        threads[i].start()

        # a positive wait time will allow the calling process to proceed after the specified time
        # a negative wait time (default) will block the process until all parallel prcesses finish
    if (waittime > 0):
        for i in range(len(threads)):
            threads[i].join(waittime)
    else:
        for i in range(len(threads)):
            threads[i].join()
    #for i in range(len(threads)):
    #    if threads[i].is_alive():
    #        threads[i].terminate()

    return (threads)

#September 2, 2016: This is intended to convert lower-case letters in CUSIPs to upper-case,
#However, with the complexity of revised filings and the process of combining multiple lines of the same underlying,
#it is safer to just remove the file and re-scrape the filing after code is added to upper-case CUSIP at scraping time
def CUSIPUpper(fund):
    HldgDir = '/Users/shared/fund clone/' + fund
    if not os.path.exists(HldgDir):
        return

    p = re.compile('[,\s]')
    HldgFiles = glob(HldgDir + '/*.csv')

    for HldgFile in HldgFiles:
        try:
            with open(HldgFile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            print(e)
            continue

        newData = fundData[:3]
        fundModified = False

        for line in fundData[3:]:
            curCUSIP = line[2]
            newCUSIP = curCUSIP.upper()

            if (curCUSIP == newCUSIP):
                newData += [line]
            else:
                newData += [(line[0], line[1],  newCUSIP, line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12])]
                fundModified = True

        if fundModified:
            print(HldgFile)
            os.remove(HldgFile)

def CUSIPUpperAll():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    for fund in TargetFunds:
        CUSIPUpper(fund[0])

#CUSIPUpper('WinchAdvisoryServicesLLC')
#CUSIPUpperAll()

#September 2, 2016: check the consistency of scraping results, i.e. each line contains 13 elements
def CheckScrapingResults(fund):
    HldgDir = '/Users/shared/fund clone/' + fund
    if not os.path.exists(HldgDir):
        return

    p = re.compile('[,\s]')
    HldgFiles = glob(HldgDir + '/*.csv')

    for HldgFile in HldgFiles:
        try:
            with open(HldgFile, mode='r', encoding='utf-8') as infile:
                fundData = [tuple(line) for line in csv.reader(infile)]
        except Exception as e:
            print(e)
            continue

        newData = fundData[3:]
        try:
            minLineLength = min([len(tuple(line)) for line in newData])
            maxLineLength = max([len(tuple(line)) for line in newData])
        except Exception as e:
            minLineLength = 0
            maxLineLength = 0
            print(e)
            #print(HldgFile)
        #print(minLineLength, maxLineLength)

        if (minLineLength < 13) or (maxLineLength > 13):
            os.remove(HldgFile)
            print(HldgFile)

def CheckAll():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    for fund in TargetFunds:
        CheckScrapingResults(fund[0])


#CheckAll()

#check for if the fund filing directory is empty or with gaps
#This usually signals some issue with the underlying data, which should be investigated
#These funds are also sent for another round of scraping
def EmptyOrGapFiling(fund):
    HldgDir = '/Users/shared/fund clone/' + fund
    if not os.path.exists(HldgDir):
        return(False)

    p = re.compile('[,\s]')
    HldgFiles = glob(HldgDir + '/*.csv')
    if (len(HldgFiles)<1):
        return(True)

    if (len(HldgFiles) == 1):
        return(False)

    fileDts = sorted([os.path.splitext(os.path.basename(line))[0] for line in HldgFiles])
    #print(fileDts)
    fileDts = [datetime.datetime.strptime(line, '%Y%m%d').date() for line in fileDts]
    diffDays = [(fileDts[i+1]-fileDts[i]).days for i in range(len(fileDts)-1)]
    #print(fileDts)
    #print(diffDays)
    if (max(diffDays)>92):
        return(True)
    else:
        return(False)

def EmptyOrGapFilingAll():
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    Funds2ReScrape = []

    for fund in TargetFunds:
        if (EmptyOrGapFiling(fund[0])):
            print(fund)
            Funds2ReScrape += [fund]

    print(len(Funds2ReScrape))

    if Funds2ReScrape:
        return(ScrapeFundsParalle(Funds2ReScrape, NumOfThreads=3, UpdateMode=False, waittime=100))

#screen for funds with last quarterly filing but not the current one (when one should have been filed)
#This should be a sign for further investigation
#
def FundsFiledLastQtr(dtsStr):
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    Funds2ReScrape = []
    for fund in TargetFunds:
        HldgDir = '/Users/shared/fund clone/' + fund[0]
        if not os.path.exists(HldgDir):
            continue

        p = re.compile('[,\s]')
        HldgFiles = glob(HldgDir + '/*.csv')
        if (len(HldgFiles) < 1):
            continue


        fileDts = sorted([os.path.splitext(os.path.basename(line))[0] for line in HldgFiles])
        if (fileDts[len(fileDts)-1] == dtsStr):
            Funds2ReScrape += [fund]
            print(fund)
    print(len(Funds2ReScrape))

    if Funds2ReScrape:
        return (ScrapeFundsParalle(Funds2ReScrape, NumOfThreads=3, UpdateMode=True, waittime=100))


#FundsFiledLastQtr('20151231')

def CleanupTmpFiles():
    tmpdir = '/Users/shared/fund clone/@backup'
    if not os.path.exists(tmpdir):
        return

    tmpfiles = glob(tmpdir + '/*.csv')
    for tmpfile in tmpfiles:
        os.remove(tmpfile)



#print(len(MonitorFunds))
#print(MonitorFunds)
while True:
    start_time = time.time()

    SkipCIKs = []
    try:
        with open(skipCIKfile, 'r') as csvfile:
            SkipCIKs = [tuple(line) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)
    with open(coreCIKfile, 'r') as csvfile:
        coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
    print(len(coreTargetFunds))
    #print(coreTargetFunds)

    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    tmpMonitorFunds = [tuple(line) for line in TargetFunds if line[0] in MonitorFunds]
    #print(tmpMonitorFunds)

    coreTargetFunds = list(tmpMonitorFunds) + list(coreTargetFunds)
    coreTargetFunds = sorted(list(set(coreTargetFunds) - set(SkipCIKs)))
    print(len(coreTargetFunds))
    ScrapeFundsParalle(coreTargetFunds)

    tmpexistingFunds =  glob("/Users/Shared/Fund Clone/*")
    existingFunds = [os.path.splitext(os.path.basename(tmpexistingFund))[0] for tmpexistingFund in tmpexistingFunds]
    existingFunds = [tuple(line) for line in TargetFunds if line[0] in existingFunds]
    print(len(existingFunds))


    backgroundProcesses=None
    try:

        try:
            with open('/Users/Shared/Models/Funds13F.csv') as csvfile:
                Funds13F = [line for line in csv.reader(csvfile)]
        except Exception as e:
            print(e)
            Funds13F = None

        if Funds13F:
            Funds13F = [line[0] for line in Funds13F]
            Funds13F = [tuple(line) for line in TargetFunds if line[0] in Funds13F]
            Funds13F = list(set(Funds13F + coreTargetFunds))
            backgroundProcesses = ScrapeFundsParalle(Funds13F, NumOfThreads=4, UpdateMode=True)
            Funds13F += existingFunds
        else:
            Funds13F = existingFunds
        Funds13F = list(set(Funds13F))
        backgroundProcesses = ScrapeFundsParalle(Funds13F, NumOfThreads=4, UpdateMode=True)

        #tmpTargetFunds = [tuple(line) for line in TargetFunds if line[0] in Funds13F]
        ResidualFunds = list(set(TargetFunds) - set(Funds13F))
        backgroundProcesses = ScrapeFundsParalle(Funds13F, NumOfThreads=2, UpdateMode=False, waittime=2500)
        if ResidualFunds:
            print(len(ResidualFunds))
            backgroundProcesses += ScrapeFundsParalle(ResidualFunds, NumOfThreads=2, UpdateMode=False, waittime=100)

        subprocess.call(outputpath +'Models/Python/RZRQ.py')

        CUSIPUpperAll()
        CheckAll()
        backgroundProcesses += EmptyOrGapFilingAll()
        #backgroundProcesses += FundsFiledLastQtr()


    except Exception as e:
        print (e)

    print('Scraping time %s seconds ......' %(time.time()-start_time))
    start_time=time.time()

    time.sleep(12*60*60)
    #time.sleep(Time2NextUSBusinessDay())
    # if backgroundProcesses:
    #     for p in backgroundProcesses:
    #         try:
    #             if p.is_alive():
    #                 p.terminate()
    #                 print('terminating a process......')
    #         except Exception as e:
    #             print(e)

    CleanupTmpFiles()
    #break

#testurl =  'http://www.sec.gov/Archives/edgar/data/934639/000094787107000329/form13f_maverick-021407.txt'
#testurl = 'https://www.sec.gov/Archives/edgar/data/934639/000095013403002214/d03135e13fvhr.txt'
#testurl = 'https://www.sec.gov/Archives/edgar/data/921669/000092847504000070/cci.txt'

#ScrapeTextFilingData(testurl, '/Users/Shared/foo.csv', 'foo', 'foo')
#ScrapeOneFund("FMR","0000315066", False)
#ScrapeOneFund('Maverick','0000934639', False)
#ScrapeOneFund('SACCapitalAdvisorsLP','0001451928', False)
#ScrapeOneFund('Appaloosa','0001006438', False)
#ScrapeOneFund('SACCAPITALADVISORSLLC','0001018103', False)
#ScrapeOneFund('BALYASNYASSETMANAGEMENTLLC','0001218710', False)
#ScrapeOneFund('EverPoint','0001603464', False)
#ScrapeOneFund('RubricCapitalManagementLLC','0001628676', False)
#ScrapeOneFund('CubistSystematicStrategiesLLC','0001603465', False)
#ScrapeOneFund('CRIntrinsicInvestorsLLC','0001316388', False)
#ScrapeOneFund('SIGMACAPITALMANAGEMENTLLC','0001167507', False)
#ScrapeOneFund('MAVERICKCAPITALLTDADV','0000928617', False)