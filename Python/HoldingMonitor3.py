#!/Users/mintang/anaconda3/envs/Legacy/bin/python


import csv
import subprocess
import HoldingAnalysisModule as HAM
import multiprocessing
import datetime
from pytz import timezone


########################################################################################
#### Initialize fund list and open position list
#### Run Holding Analysis and update holding history for open positions
########################################################################################

Fundfile = '/Users/Shared/SC13Monitor/MonitorFunds.csv'
with open(Fundfile, 'r') as csvfile:
    coreTargetFunds = [tuple(line) for line in csv.reader(csvfile)]
coreFunds = sorted([line[0] for line in coreTargetFunds])
#print(coreFunds)

#The OpenPosition List is of the format (ticker, CUSIP).
#Holding history of underlyings in OpenPosition list by coreFunds is generated
#The information is reviewed when assess underlyings for trading
#The OpenPosition List used to be updated manually
#Now automate this process by taking tickers from positions.csv and point72.csv
# and combine with sc13tickers to create an OpenPositionsList
def CreateOpenPositionsList(tickerfiles):
    #1. Get tickers from positions.csv and Point72.csv and get CUSIPs
#    tickerfiles = ['positions', 'Point72', 'watchlist', 'ChineseFundTickers']
    tickers = []
    for tickerfilename in tickerfiles:
        curTickerFile = '/Users/Shared/SC13Monitor/Lists/' + tickerfilename + '.csv'
        try:
            with open(curTickerFile, encoding='utf-8') as csvfile:
                curTickers = [tuple(line) for line in csv.reader(csvfile)]
            tickers = list(set(tickers + curTickers))
        except Exception as e:
            print(e)
    if tickers:
        tickers =[line[0] for line in tickers]

    if tickers:
        CUSIPTickerFile =  '/Users/Shared/Models/CUSIPTicker.csv'
        with open (CUSIPTickerFile, encoding='utf-8') as csvfile:
            CUSIPTickers = [tuple(line) for line in csv.reader(csvfile)]

        curTickerCUSIPs = [(line[1], line[0]) for line in CUSIPTickers if (line[1] in tickers)]
    else:
        curTickerCUSIPs = []

    #2. Combine with sc13tickers data
    sc13tickers = []
    try:
        with open('/Users/Shared/SC13Monitor/Lists/sc13tickers.csv', encoding='utf-8') as csvfile:
            sc13tickers = [(line[0], line[1]) for line in csv.reader(csvfile)]
    except Exception as e:
        print(e)

    curTickerCUSIPs = list(set(curTickerCUSIPs + sc13tickers))
    return(curTickerCUSIPs)


def UpdateOpenPositionsParallel():
    OpenPositions = CreateOpenPositionsList( ['positions', 'Point72', 'watchlist', 'ChineseFundTickers'])

    if OpenPositions:
        NumOfThreads=4
        threads = [None]* NumOfThreads
        out_q = multiprocessing.Queue()
        tickerListLen = len(OpenPositions) // NumOfThreads
        for i in range(len(threads)):
            startIdx = i*tickerListLen
            if (i==(len(threads)-1)):
                endIdx = len(OpenPositions)
            else:
                endIdx = (i+1)*tickerListLen-1
            threads[i] = multiprocessing.Process(target=HAM.HoldingAnalysisOpenPositions, args=(OpenPositions[startIdx:(endIdx+1)], coreFunds))
            threads[i].start()
        for i in range(len(threads)):
            threads[i].join()

    # HAM.OpenPositionsCleanup(list(set([line[0] for line in OpenPositions])))
##

x = HAM.HoldingMonitor(coreFunds)
print(x)

if True:
    HAM.UpdateOpenPositionList(coreFunds, 30, 0.5)
    x = HAM.HoldingMonitorUpdate(coreFunds)

    command = '/Users/mintang/anaconda3/bin/r --no-save < /Users/Shared/models/tools/SizeAnalysis.R'
    subprocess.call(command, shell=True)
    outputpath = '/Users/Shared/'
    subprocess.call(outputpath +'Models/Python/RZRQ.py', shell=True)

    currDt = datetime.datetime.now(timezone('EST'))
    if ((currDt.weekday()>=5) or ((currDt.weekday()==4) and (currDt.hour>17))):
        UpdateOpenPositionsParallel()



#OpenPositions = CreateOpenPositionsList( ['positions', 'Point72', 'watchlist', 'ChineseFundTickers'])
#print(OpenPositions)
#tmp =[tuple(line) for line in OpenPositions if line[0]=='BITA']
#print(tmp)
#HAM.HoldingAnalysisOpenPositions([('HTZ', '42806J106'), ('HTZ', '42805T105')], coreFunds)

#HoldingAnalysisOpenPositions()
#Scan4Fundswithout13F()
##x = HoldingHistory('48138L107', 'GreenWoods')

##x = HoldingAnalysis('92828Q109', coreFunds)
##print(x)
##x = HoldingHistory("86732Y109", "Point72")
##y = SegmentHoldingHistory(x)
##print(x)
##print(y)
#FundHoldingAnalysis('Point72', coreFunds)

#for fund in coreFunds:
#    HAM.TradeDatafromSC13Signals(fund)

