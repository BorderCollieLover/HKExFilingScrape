import csv
import re
import os

def SaveDict2CSV(filename, datDict):
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for key, value in datDict.items():
            writer.writerow([key, value])

def SaveSet2CSV(filename, datSet):
    ##print(datSet)
    datTmp = list(datSet)
    ##print(datTmp)
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        for line in datTmp :
            writer.writerow([line])

datapath = "/Users/Shared/Fund Clone/"
ignoreSet =set(['NEW', 'USD', 'ADR', 'ADS', 'REIT', 'THE', 'CHARLES', 'ETF', 'BERMUDA', 'CONV', 'NY', 'NYSE', 'EI', 'ELI', 'TEXAS', 'OLD', 'WARRANT', 'WARRANTS', 'ALLYPRB'])


funds = ['GOLDMANCAPITALMANAGEMENTINC',
         'PVGASSETMANAGEMENTCORP',
         'SummitGlobalManagementInc',
         'STGERMAINDJCOINC',
         'CliftonGroupInvestmentManagementCo',
         'SECURITYBANCCORP',
         'GREENHAVENASSOCIATESINC',
         'PsagotInvestmentHouseLtd',
         'CHATHAMCAPITALGROUPINC',
         'WAFRAINVESTMENTADVISORYGROUPINCNY',
         'HARVESTCAPITALMANAGEMENTINC',
         'BollardGroupLLC',
         'CORBYNINVESTMENTMANAGEMENTINCMD',
         'PVGAssetManagement',
         'StevensFirstPrinciplesInvestmentAdvisors',
         'FORTISADVISERSINC',
         'FORTISADVISERSINCMNADV',
         'OCONNELLINVESTMENTSERVICESINC',
         'WALLSTREETASSOCIATES',
         'WESTELLISINVESTMENTMANAGEMENTINC',
         'SageAdvisoryServicesLtdCo',
         'NORRISPERNEFRENCHLLPMI',
         'AUGUSTINEASSETMANAGEMENTINC',
         'PATTENPATTENINCTN'
         ]

def ExtractOneFile(filename):
    with open(filename, mode='r') as csvfile:
        data = [tuple(line) for line in csv.reader(csvfile)]

    data = data[3:]
    tickerCUSIPs = [(line[0], line[2]) for line in data]
    output = []
    for line in tickerCUSIPs:
        curCUSIP = line[1]
        tmpTicker = line[0]
        tickerPattern = re.search('\([a-zA-Z]+\)', tmpTicker)
        #print(tickerPattern[0])
        if tickerPattern:
            ticker = tickerPattern[0]
            ticker = ticker.strip()
            ticker = ticker[1:(len(ticker)-1)]
            ticker = ticker.upper()
            if ticker not in ignoreSet:
                #print(line, curCUSIP, ticker)
                output += [(curCUSIP,ticker.upper())]

    output = list(set(output))
    #print(output)
    return(output)
    #print(tickerCUSIPs)

def ProcessAllFiles(fund):
    outputData = []

    allFilings = []
    try:
        allFilings = list(set(os.listdir('/Users/Shared/Fund Clone/' + fund)))
    except Exception as e:
        print(e)

    for file in allFilings:
        outputData += ExtractOneFile('/Users/Shared/Fund Clone/'+fund+'/'+file)

    outputData = list(set(outputData))
    #print(outputData)
    return(outputData)


def SafeAdd2CUSIPTicker(newCUSIPTicker):
    fileCUSIPTicker = '/Users/Shared/Models/CUSIPTicker.csv'
    try:
        with open(fileCUSIPTicker, mode='rt') as csvfile:
            dictCUSIPTicker = dict(csv.reader(csvfile))
    except Exception as e:
        print(e)
        dictCUSIPTicker = dict()

    fileCUSIPIgnore = '/Users/Shared/Models/CUSIPIgnore.csv'
    try:
        with open(fileCUSIPIgnore, mode='rt') as csvfile:
            CUSIPIgnore = set([row[0] for row in csv.reader(csvfile)])
    except Exception as e:
        print(e)
        CUSIPIgnore = set()

    dictCUSIPTickerUpdated = False
    CUSIPIgnoreUpdated = False
    for key in newCUSIPTicker:
        #print(key, newCUSIPTicker[key])
        if dictCUSIPTicker:
            if key in dictCUSIPTicker:
                if (dictCUSIPTicker[key] != newCUSIPTicker[key]):
                    print(key + ' exists with a different ticker: ' + dictCUSIPTicker[key] + ' instead of ' +
                          newCUSIPTicker[key])
                    continue
                else:
                    continue
            else:
                dictCUSIPTicker[key] = newCUSIPTicker[key]
                dictCUSIPTickerUpdated = True
                print(key, newCUSIPTicker[key], 'Added')
        else:
            dictCUSIPTicker[key] = newCUSIPTicker[key]
            dictCUSIPTickerUpdated = True

        if CUSIPIgnore:
            if key in CUSIPIgnore:
                CUSIPIgnore.remove(key)
                CUSIPIgnoreUpdated = True

    print(CUSIPIgnoreUpdated)
    print(dictCUSIPTickerUpdated)

    if dictCUSIPTickerUpdated:
        SaveDict2CSV(fileCUSIPTicker, dictCUSIPTicker)

    if CUSIPIgnoreUpdated:
        SaveSet2CSV(fileCUSIPIgnore, CUSIPIgnore)

    return

#i = 22
newCUSIPTickers = []
for fund in funds:
    #print(fund)
    newCUSIPTickers += ProcessAllFiles(fund)
newCUSIPTickers = list(set(newCUSIPTickers))
print(len(newCUSIPTickers))
SafeAdd2CUSIPTicker(dict(newCUSIPTickers))

#newCUSIPTickers = ProcessAllFiles()
#SafeAdd2CUSIPTicker(dict(newCUSIPTickers))

#ExtractOneFile("/Users/Shared/Fund Clone/GOLDMANCAPITALMANAGEMENTINC/20160930.csv")

def CheckAllFilings():
    allFunds = list(set(os.listdir('/Users/Shared/Fund Clone/')))
    #print(allFunds)
    logfile = '/Users/Shared/13FwithTickers.txt'
    for fund in allFunds:
        print(fund)
        allFilings = []
        try:
            allFilings = list(set(os.listdir('/Users/Shared/Fund Clone/'+fund)))
        except Exception as e:
            print(e)
        #print(allFilings)
        tmpOutput = []
        for filing in allFilings:
            try:
                tmpOutput = ExtractOneFile('/Users/Shared/Fund Clone/'+fund+'/'+filing)
            except Exception as e:
                print(e)

            if tmpOutput:
                if (len(tmpOutput)>0):
                    with open(logfile, 'a') as f:
                        for line in tmpOutput:
                            f.write(fund + ', ' +  filing + ',' + line[0] +', '+line[1]+'\r\n')

    return


#CheckAllFilings()