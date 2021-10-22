#!/Users/mintang/anaconda3/envs/Legacy/bin/python

import xlrd
from xlrd.sheet import ctype_text 
import re
from operator import itemgetter
import csv
import os
import pandas as pd
import datetime
import numpy as np
import requests
from pandas.tseries.offsets import BDay


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
rptName = 'c:\\users\\mtang\\HKEx\\Repurchase\\20210603.xls'
HKFilingsDir = "X:\\HKExFilings\\"

BuyBackSummaryFile = HKFilingsDir + "BuybackSummary.csv"
AllBuyBackSummaryFile = HKFilingsDir + "AllBuybackSummary.csv"
BuyBackSnapshotFile = HKFilingsDir + "BuybackSnapshot.csv"
BuybackTickerFile = HKFilingsDir + "Snapshots\\tickers.csv"




def CleanString(inputStr):
    if not inputStr:
        return('')
    else:
##        print(inputStr)
        p = re.compile(',')
        inputStr = p.sub('',inputStr)
        #p = re.compile('HKD')
        #inputStr = p.sub('',inputStr)
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
            print(e)
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
            with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(onetable)
    
def DownloadBuyBackReports():
    
    #Prior to 2003.03.31 Buyback reports are in PDF files 
    cutoffdt = pd.datetime(2003,3,31)
    ## Share repurchase reports from HKEx
    curDt = pd.datetime.today()
    failCtr = 0
    while (curDt > cutoffdt):
        filename = HKFilingsDir + "Repurchase\\" + curDt.strftime('%Y%m%d') + '.xls'
        if os.path.isfile(filename):
            break;
    
        #if os.path.isfile(filename):
        #    curDt = curDt - BDay(1)
        #    continue;
        
        fileurl = 'https://www.hkexnews.hk/reports/sharerepur/documents/SRRPT' + curDt.strftime('%Y%m%d') + '.xls'
    ##    fileurl = 'http://www.hkexnews.hk/reports/sharerepur/documents/' + curDt.strftime('%Y%m%d') + '.pdf'
        print(fileurl)
        try:
            resp = requests.get(fileurl, verify=False)
            if resp.raise_for_status() is None:
                with open(filename, 'wb') as output:
                    output.write(resp.content)
        except Exception as e:
            print(e)
            failCtr += 1
        
        curDt = curDt - BDay(1)
    
    #download repurchase reports of GEM stocks in HKEx
    curDt = pd.datetime.today()
    failCtr = 0
    while (curDt > cutoffdt):
        filename = HKFilingsDir + "GEMRepurchase\\"+ curDt.strftime('%Y%m%d') + '.xls'
        if os.path.isfile(filename):
            break;
    
        #if os.path.isfile(filename):
        #    curDt = curDt - BDay(1)
        #    continue;
        
    
        fileurl = 'https://www.hkexnews.hk/reports/sharerepur/GEM/documents/SRGemRPT' + curDt.strftime('%Y%m%d') + '.xls'
        print(fileurl)
        try:
            resp = requests.get(fileurl, verify=False)
            if resp.raise_for_status() is None:
                with open(filename, 'wb') as output:
                    output.write(resp.content)
        except Exception as e:
            print(e)
            
        curDt = curDt - BDay(1)

        

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
                #print('(%s) %s %s' % (idx, cell_type_str, cell_obj.value))
        if (len(rptLine) == 0 ):
            lastRow = True;
        elif (len(rptLine) <11) and (len(rptLine)>1):
            while (len(rptLine)<11):
                rptLine += ('-',)
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


def UpdateAnnualBuyBackData():
    scrapingdts = pd.bdate_range(start=datetime.datetime(2021,1,1), end=datetime.datetime.now()).tolist()
    x = []    
    for scrapingdt in scrapingdts:
        tmp = ReadOneReport(HKFilingsDir + "Repurchase\\"+scrapingdt.strftime('%Y%m%d')+'.xls')
        x += tmp
        # DumpTable(x, BuybackSummaryFile)
    for scrapingdt in scrapingdts:
        tmp = ReadOneReport(HKFilingsDir + "GEMRepurchase\\"+scrapingdt.strftime('%Y%m%d')+'.xls')
        x += tmp
    DumpTable(x, BuyBackSummaryFile)
    return # end


def UpdateAllBuyBackData():
    scrapingdts = pd.bdate_range(start=datetime.datetime(2003,1,1), end=datetime.datetime.now()).tolist()
    x = []    
    for scrapingdt in scrapingdts:
        tmp = ReadOneReport(HKFilingsDir + "Repurchase\\"+scrapingdt.strftime('%Y%m%d')+'.xls')
        x += tmp
        # DumpTable(x, BuybackSummaryFile)
    for scrapingdt in scrapingdts:
        tmp = ReadOneReport(HKFilingsDir + "GEMRepurchase\\"+scrapingdt.strftime('%Y%m%d')+'.xls')
        x += tmp
    DumpTable(x, AllBuyBackSummaryFile)
    return # end

def BuybackSummaryfromFile(filename=BuyBackSummaryFile):
    BuybackHeader = ['Company', 'Ticker', 'Stock Type', 'Trade Date', 'Number of Shares', 'Last Buyback High Price', 'Last Buyback Low Price', 'Last Buyback Total',
                  'Method of Purchase', 'YTD Shares', 'YTD %']
    
    BuybackData = pd.read_csv(filename, header=None, names=BuybackHeader)
    BuybackData.sort_values(by='Trade Date', inplace=True)
    tickers = np.unique(BuybackData['Ticker'])
    output_data = pd.DataFrame()
    
    
    for ticker in tickers: 
        data = BuybackData[BuybackData['Ticker']==ticker]
        if (len(data) ==1):
            if output_data.empty: 
                output_data = data
            else:
                output_data = output_data.append(data, ignore_index=True)
        
        print(ticker)
        print(data)
        share_types = np.unique(data['Stock Type'])
        if len(share_types)==1:
            output_data = output_data.append(data[-1:], ignore_index=True)
        else: 
            for share_type in share_types: 
                tmp_data = data[data['Stock Type']==share_type]
                output_data = output_data.append(tmp_data[-1:], ignore_index=True)
                
    output_data.sort_values(by=['Trade Date', 'YTD %'], inplace=True )
    output_data.drop_duplicates(inplace=True)
    output_data.to_csv(BuyBackSnapshotFile , index=False)
    return (output_data)


def BuybackAggregatefromFile(filename=BuyBackSummaryFile):
    BuybackHeader = ['Company', 'Ticker', 'Stock Type', 'Trade Date', 'Number of Shares', 'Last Buyback High Price', 'Last Buyback Low Price', 'Last Buyback Total',
                  'Method of Purchase', 'YTD Shares', 'YTD %']
    
    BuybackData = pd.read_csv(filename, header=None, names=BuybackHeader)
    BuybackData.sort_values(by='Trade Date', inplace=True)
    tickers = np.unique(BuybackData['Ticker'])
    output_data = pd.DataFrame()
    
    
    for ticker in tickers: 
        data = BuybackData[BuybackData['Ticker']==ticker]
        if (len(data) ==1):
            if output_data.empty: 
                output_data = data
            else:
                output_data = output_data.append(data, ignore_index=True)
        
        print(ticker)
        print(data)
        share_types = np.unique(data['Stock Type'])
        if len(share_types)==1:
            output_data = output_data.append(data[-1:], ignore_index=True)
        else: 
            for share_type in share_types: 
                tmp_data = data[data['Stock Type']==share_type]
                output_data = output_data.append(tmp_data[-1:], ignore_index=True)
                
    output_data.sort_values(by=['Trade Date', 'YTD %'], inplace=True )
    output_data.drop_duplicates(inplace=True)
    output_data.to_csv(BuyBackSnapshotFile , index=False)
    return (output_data)

    
DownloadBuyBackReports()
UpdateAnnualBuyBackData()
BuybackSummaryfromFile()
#UpdateAllBuyBackData()