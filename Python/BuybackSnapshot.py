#!/usr/local/bin/python3

import csv
import datetime
from pandas import DataFrame
from dateutil import parser
import pandas as pd
import os


buybackfile = '/Users/shared/HKEx/BuybackSummary.csv'
#xqDatafile = '/Users/shared/XQ/CurrentXQ.csv'
BuybackTickerFile = '/Users/shared/HKEx/Snapshots/tickers.csv'

with open(buybackfile, 'r', encoding='utf-8') as csvfile:
    buybackData = [line for line in csv.reader(csvfile)]
buybackHeader=['Company', 'Stock Type', 'Trade Date', 'Number of Shares', '(High) Price', 'Low Price', 'Total', 'Method of Purchase', 'YTD Shares', 'YTD %']

#xqData = DataFrame.from_csv(xqDatafile, encoding='utf-8')
#Screen for the latest buyback info for each underlying
#for companies that buy back stocks over the last 6 months (can be parameterized)
#combine with fundamental data from XQ
def BuybackSnapshot(lookbackDays=180):
    outputfile = '/Users/shared/HKEx/Snapshots/'+datetime.date.today().strftime('%Y%m%d')+'.csv'
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
            lineDt= parser.parse(line[3])
            if (lineDt > cutoffDt):
                ObservedTickers.update([ticker])
                #1. convert tuple into a dictionary & #2. convert dictionary to a DataFrame
                #3. combine with (if exists) data from xqData, using ticker as a reference
                #4. Update the output data
                lineDF = DataFrame([(line[0], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10])], index=[line[1]], columns=buybackHeader)
                # XQ data is no longer scraped
                # if (line[1] in xqData.index):
                #     xqDF = DataFrame(data=[xqData.loc[line[1]]], index=[line[1]])
                #     lineDF = pd.concat([lineDF, xqDF], axis=1, join='inner')
                
                outputData =pd.concat([outputData, lineDF])
            else:
                break

    if not outputData.empty:
        outputData.to_csv(outputfile, encoding='utf-8')
        outputData.to_csv('/Users/shared/HKEx/Snapshots/CurrentSnapshot.csv', encoding='utf-8')

    ObservedTickers = list(ObservedTickers)
    with open(BuybackTickerFile, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        for line in ObservedTickers :
            writer.writerow([line])

                
        

#BuybackSnapshot(60)
