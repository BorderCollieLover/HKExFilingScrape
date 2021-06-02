#!/Users/mintang/anaconda3/envs/Legacy/bin/python

import xlrd
from xlrd.sheet import ctype_text 
from dateutil import parser
import re
from operator import itemgetter
import csv
import os
import pandas as pd
import datetime
from pandas import DataFrame



#Notes on the data:
#Before 20030401: PDF files
#Before 20060301: includes month-to-date data

#Data starting row:
#kind of random, usually row 6, 8 or 14 in the spreadsheet


#rptName = '/Users/shared/HKEx/Repurchase/20151106.xls'
rptName = '/Users/shared/HKEx/Repurchase/20060214.xls'
rptName = '/Users/shared/HKEx/Repurchase/20111216.xls'
rptName = '/Users/shared/HKEx/Repurchase/20120102.xls'
rptName = '/Users/shared/HKEx/Repurchase/20080822.xls'


def CleanString(inputStr):
    if not inputStr:
        return('')
    else:
##        print(inputStr)
        p = re.compile(',')
        inputStr = p.sub('',inputStr)
        p = re.compile('HKD')
        inputStr = p.sub('',inputStr)
        p = re.compile('-')
        inputStr = p.sub(' ',inputStr)
        
        p = re.compile('\s+')
        inputStr = p.sub(' ',inputStr)
        inputStr = inputStr.strip()
        if (inputStr ==' '):
            inputStr=''
        return(inputStr)
        
def RemoveDuplicatedLines(data):
    if (len(data) <= 0):
        return(data)
    else:
        newData = list(set(data))
        newData = sorted(newData, key=itemgetter(3,1), reverse=True)
        return(newData)

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
    if (len(onetable)<=0):
        print('No data to save')
    else:
##        onetable = sorted(onetable, key=itemgetter(2), reverse=True)
        onetable = RemoveDuplicatedLines(onetable)
        if not CompareData2CSV(onetable, filename):
            with open(filename, 'w', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(onetable)
    

        

def ReadOneReport(filename):
    print(filename)
    rptData = []
    lastRowIdx = 14


    #get the basename of the file, skip if before 20060301
    fileDt = os.path.splitext(os.path.basename(filename))[0]
    if (fileDt < '20060301'):
        print('Skip file before 20060301:' + fileDt)
        return(rptData)

    
    try:
        wb = xlrd.open_workbook(filename)
    except Exception as e:
        print(e)
        return(rptData)

    xlSheet = wb.sheet_by_index(0)
    #find the cell that contains 'company' as an anchor for data screening, 'Company' is the first column header in the header row
    curRowIdx = 0
    while True:
        curRow = xlSheet.row(curRowIdx)
        curCell = curRow[0]
        if (re.match('Company', curCell.value, re.I)):
            break;
        else:
            curRowIdx += 1
            if (curRowIdx > 100):
                print('Cannot find cell that contains Company: ' +  filename)
                return(rptData)
            
##    print(curRowIdx)
    RowIdx = curRowIdx + 1 # start from the row beneath the row that contains 'Company'
    #RowIdx = 7
    lastRow = False
##    print(xlSheet.nrows)
##    print(min((xlSheet.nrows-1), 14))
    while True:
        rptLine = ()
##        print(RowIdx)
        try:
            xlRow = xlSheet.row(RowIdx)
        except Exception as e:
            print(e)
            xlRow=[]
        #print(RowIdx)
        #print(xlRow)
        for idx, cell_obj in enumerate(xlRow):
            #print(idx)
            #print(cell_obj)
            cell_type_str = ctype_text.get(cell_obj.ctype, 'unknow_type')
            if (cell_type_str == 'empty'):
##                print(cell_obj.value)
##                lastRow = True;
##                break;
                next; 
            else:
                #print(cell_type_str)
                if (cell_type_str == 'number'):
                    if (idx == 1):
                        cellDat = str(int(cell_obj.value))
                    else:
                        cellDat = str(cell_obj.value)
                elif (cell_type_str == 'xldate'):
                    #do sth
                    #print(cell_obj.value)
                    cellDat=datetime.datetime(*xlrd.xldate_as_tuple(int(cell_obj.value), wb.datemode))
                    cellDat = cellDat.strftime('%Y/%m/%d')
                else: 
                    cellDat = CleanString(cell_obj.value)
                if (idx == 1):
                    # stock code, change to Yahoo ticker format
                    while (len(cellDat) < 4):
                        cellDat = '0'+cellDat
                    cellDat += '.HK'
##                elif (idx == 3 ): # trade date
##                    cellDat = parser.parse(cellDat, dayfirst=False, yearfirst=True)
                rptLine += (cellDat,)
##                print('(%s) %s %s' % (idx, cell_type_str, cell_obj.value))
        if (len(rptLine) == 0 ):
            lastRow = True
##        print(lastRow)
        if lastRow:
            if (RowIdx < min((xlSheet.nrows-1), lastRowIdx)):
                lastRow = False
                RowIdx += 1
            else:
                break;
        else:
            if (len(rptLine) == 11):
                rptData += [rptLine]
            RowIdx += 1 
    return(rptData)

def UpdateBuyBackSummary():
    i = 10;
    scrapingdts = pd.bdate_range(end=datetime.datetime.now(), periods=i).tolist()
    BuybackSummaryFile = '/Users/shared/HKEx/BuybackSummary.csv'
    try:
        with open(BuybackSummaryFile, encoding='utf-8') as f:
            x = [tuple(line) for line in csv.reader(f)]
    except Exception as e:
        print(e)
        x = []
        
    for scrapingdt in scrapingdts:
        tmp = ReadOneReport('/Users/shared/HKEx/GEMRepurchase/'+scrapingdt.strftime('%Y%m%d')+'.xls')
        x += tmp
        # DumpTable(x, BuybackSummaryFile)
    for scrapingdt in scrapingdts:
        tmp = ReadOneReport('/Users/shared/HKEx/Repurchase/'+scrapingdt.strftime('%Y%m%d')+'.xls')
        x += tmp
    DumpTable(x, BuybackSummaryFile)


buybackfile = '/Users/shared/HKEx/BuybackSummary.csv'
# xqDatafile = '/Users/shared/XQ/CurrentXQ.csv'
BuybackTickerFile = '/Users/shared/HKEx/Snapshots/tickers.csv'

with open(buybackfile, 'r', encoding='utf-8') as csvfile:
    buybackData = [line for line in csv.reader(csvfile)]
buybackHeader = ['Company', 'Stock Type', 'Trade Date', 'Number of Shares', '(High) Price', 'Low Price', 'Total',
                 'Method of Purchase', 'YTD Shares', 'YTD %']


# xqData = DataFrame.from_csv(xqDatafile, encoding='utf-8')
# Screen for the latest buyback info for each underlying
# for companies that buy back stocks over the last 6 months (can be parameterized)
# combine with fundamental data from XQ
def BuybackSnapshot(lookbackDays=180):
    UpdateBuyBackSummary()
    outputfile = '/Users/shared/HKEx/Snapshots/' + datetime.date.today().strftime('%Y%m%d') + '.csv'
    if os.path.isfile(outputfile):
        outputfiledt = datetime.datetime.fromtimestamp(os.stat(outputfile).st_mtime)
        buybacksummarydt = datetime.datetime.fromtimestamp(os.stat(buybackfile).st_mtime)
        if (outputfiledt > buybacksummarydt):
            return

    ObservedTickers = set()
    cutoffDt = datetime.datetime.now() - datetime.timedelta(days=lookbackDays)
    print(cutoffDt)
    outputData = DataFrame()
    for line in buybackData:
        ticker = line[1]
        if ticker in ObservedTickers:
            continue
        else:
            lineDt = parser.parse(line[3])
            if (lineDt > cutoffDt):
                ObservedTickers.update([ticker])
                # 1. convert tuple into a dictionary & #2. convert dictionary to a DataFrame
                # 3. combine with (if exists) data from xqData, using ticker as a reference
                # 4. Update the output data
                lineDF = DataFrame(
                    [(line[0], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10])],
                    index=[line[1]], columns=buybackHeader)
                # XQ data is no longer scraped
                # if (line[1] in xqData.index):
                #     xqDF = DataFrame(data=[xqData.loc[line[1]]], index=[line[1]])
                #     lineDF = pd.concat([lineDF, xqDF], axis=1, join='inner')

                outputData = pd.concat([outputData, lineDF])
            else:
                break

    if not outputData.empty:
        outputData.to_csv(outputfile, encoding='utf-8')
        outputData.to_csv('/Users/shared/HKEx/Snapshots/CurrentSnapshot.csv', encoding='utf-8')

    ObservedTickers = list(ObservedTickers)
    with open(BuybackTickerFile, 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for line in ObservedTickers:
            writer.writerow([line])

#UpdateBuyBackSummary()
#BuybackSnapshot(60)

##x = ReadOneReport(rptName)
