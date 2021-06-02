import time
from urllib.parse import urlencode
import json
import sys
import locale
import csv
import multiprocessing
from operator import itemgetter
import datetime
from pytz import timezone
import socket
import quandl
import numpy as np
import re
import os


quandl.ApiConfig.api_key = "3ojxoNzaKndioRPW_hXc"
database_list = ['CME', 'CBOE', 'EEX', 'EUREX', 'ICE', 'LIFFE', 'MCX', 'MGEX', 'MX', 'ODE', 'OSE', 'SGX', 'TFX', 'SHFE', 'HKEX', 'ASX','PXDATA']
FuturesMonths = {'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X','Z'}

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

#Convert a dictionary to list for writing as a csv file
def ConvertSettoList(databaseLog):
    outputData = []
    #print('here')
    for k, v in databaseLog.items():
        outputtuple = (k,)
        try:
            others = tuple(list(v))
        except Exception as e:
            print(e)
            print(v, ' is not iterable')
            others  = (v, )
        outputtuple += others
        outputData += [outputtuple]
    outputData = sorted(outputData)
    return(outputData)


def DumpTable(onetable,filename):
    if (len(onetable)<=0):
        print('No data to save')
    else:
        if not CompareData2CSV(onetable, filename):
            with open(filename, 'w', encoding='utf-8') as csvfile:
                #writer = csv.writer(csvfile, delimiter=',', lineterminator='\r\n', quotechar = "'")
                writer = csv.writer(csvfile)
                writer.writerows(onetable)


def LoadDatabaseLog():
    #read the database log file into a set
    #return the set
    LogFile = '/Users/Shared/Quandl/Futures/DatabaseLog.csv'
    DatabaseLog = {}
    try:
        with open(LogFile, mode='r') as infile:
            reader = csv.reader(infile)
            DatabaseLog = {rows[0]: [rows[1], rows[2]] for rows in reader}
    except Exception as e:
        print(e)

    return DatabaseLog



def UpdateDatasetFile(database, paced=False):
    #read the database log file
    #load the dataset file if exists
    #read dataset information from the database
    #update the dataset file
    #update the database log
    DatabaseLog = LoadDatabaseLog()
    #print(DatabaseLog)
    if (DatabaseLog is None) or (not (database in DatabaseLog)):
        pageIdx = None
        lastItemIdx = 0
    else:
        pageIdx = int(DatabaseLog[database][0])
        lastItemIdx = int(DatabaseLog[database][1])

    while True:
        dataset_list = []

        try:
            if pageIdx:
                dataset_list = quandl.Database(database).datasets(params={'page': pageIdx})
            else:
                dataset_list = quandl.Database(database).datasets()
                pageIdx = 1
            if dataset_list:
                dataset_data = ExtractDatasets(dataset_list[lastItemIdx - dataset_list.meta['current_first_item'] + 1:])
                UpdateDatasetLog(database, dataset_data)
                DatabaseLog[database] = [pageIdx, dataset_list.meta['current_last_item']]
                DumpTable(ConvertSettoList(DatabaseLog), '/Users/Shared/Quandl/Futures/DatabaseLog.csv')
            else:
                break
            print(database, pageIdx, len(dataset_list))
            if (dataset_list.meta['current_page'] == dataset_list.meta['total_pages']):
                break
            pageIdx = dataset_list.meta['next_page']
            if paced:
                time.sleep(0.5)
        except Exception as e:
            print(e)
            break
    return

def CreateColumnStr(columnList):
    if columnList is None:
        return('')

    outputStr =''
    for colName in columnList:
        p = re.compile(',')
        colName = p.sub(' ', colName)
        p = re.compile('\s+')
        colName = p.sub(' ', colName)
        colName = colName.strip()
        if outputStr =='':
            outputStr = colName
        else:
            outputStr += '+++' + colName

    return(outputStr)


def ExtractDatasets(dataset_list):
    if dataset_list is None:
        return None

    outputData = []
    for line in dataset_list:
        nameStr = line['name']
        p = re.compile(',')
        nameStr = p.sub('--', nameStr)
        p = re.compile('\s+')
        nameStr = p.sub(' ', nameStr)
        nameStr = nameStr.strip()
        columnStr = CreateColumnStr(line['column_names'])
        outputData += [(line['id'], line['dataset_code'], line['oldest_available_date'], line['newest_available_date'], len(line['column_names']), nameStr, columnStr)]
    return(outputData)

def UpdateDatasetLog(database, dataset_data):
    #Load existing database log if exists -- if so remove lines with ID in the dataset_data
    #Append the dataset_data to existing data
    #save the file
    if dataset_data is None:
        return
    if database is None:
        return

    datasetFile = '/Users/Shared/Quandl/Futures/datasets/' + database + '.csv'
    curDatasets = []
    try:
        with open(datasetFile, mode='r') as infile:
            reader = csv.reader(infile)
            curDatasets = [ tuple(line) for line in reader]
    except Exception as e:
        print(e)

    if curDatasets:
        outputData = curDatasets + dataset_data
    else:
        outputData = dataset_data

    DumpTable(outputData, datasetFile)
    return

#Scan the
def FuturesNStartDt(database, byContract=False):
    UpdateDatasetFile(database)
    datasetFile = '/Users/Shared/Quandl/Futures/datasets/' + database + '.csv'

    curDatasets = []
    try:
        with open(datasetFile, mode='r') as infile:
            reader = csv.reader(infile)
            curDatasets = [tuple(line) for line in reader]
    except Exception as e:
        print(e)

    if curDatasets is None:
        return None

    outputData = {}
    for line in curDatasets:
        print(line[1])
        if isFutures(line[1]):
            if byContract:
                outputData[line[1]] = [line[2], line[5], line[6]]
            else:
                genericCode = GenericFuturesCode(line[1])
                #print(genericCode, line[2])
                if genericCode in outputData:
                    #check if new data has an earlier start dt
                    curDt = line[2]
                    preDt = outputData[genericCode][0]
                    if ((preDt =='') or ((curDt != '') and (curDt < preDt))):
                        outputData[genericCode] = [line[2], line[5], line[6]]
                        print(preDt, curDt, 'updated')
                        #print('Updated')
                else:
                    outputData[genericCode] = [line[2], line[5], line[6]]



    if outputData:
        DumpTable(ConvertSettoList(outputData), '/Users/Shared/Quandl/Futures/'+database+'FuturesStartDt.csv')
    return outputData

def isFutures(contractTicker):
    ticker = contractTicker.strip()
    if (len(ticker)<6):
        return(False)
    monthCode = ticker[(len(ticker)-5):(len(ticker)-4)]
    if monthCode in FuturesMonths:
        yearCode=-1
        try:
            yearCode = int(ticker[len(ticker)-4:])
        except Exception as e:
            print(e)

        if yearCode < 1900:
            return(False)
        else:
            return(True)
    else:
        return(False)

def GenericFuturesCode(ticker):
    return(ticker[:(len(ticker)-5)])


def UpdateAllDatasets(paced=False):
    for database in database_list:
        UpdateDatasetFile(database, paced)

def UpdateFuturesStartDts():
    for database in database_list:
        FuturesNStartDt(database)



#UpdateDatasetFile('HKEX')
#FuturesNStartDt('HKEX')

UpdateAllDatasets(True)
UpdateFuturesStartDts()