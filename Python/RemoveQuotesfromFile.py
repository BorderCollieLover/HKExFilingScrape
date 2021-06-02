from glob import glob
import subprocess


def RemoveQuotesfromFile(fp):
    command = '/usr/bin/perl -pi -w -e \'s/\\"//g, \' ' + fp
    subprocess.call(command, shell=True)


def RemoveQuotesByDirectory(dirp):
    allfiles = glob(dirp+"*.csv")
    for file in allfiles:
        filestr = '"'+file+'"'
        RemoveQuotesfromFile(filestr)


def RemoveQuotesAllPrices():
    SrcDirectories = ['/Users/Shared/Quandl/EOD/Daily/',"/Users/Shared/Price Data/Daily/"]
    #SrcDirectories = ['/Users/Shared/Quandl/EOD/Daily/']
    SrcDirectories += ["Demark/Sequential", "Demark/Combo", "Trend1", "MultiDay Return/log", "RUN", "SinglePeriodReturn/log", "SMA", "EMA", "Force", "EMACHG", "MACD", "TripleScreen", "ATR", "CHD", "PENERTRATION/EMA/High", "PENERTRATION/EMA/Low", "PENERTRATION/Hilo/High", "PENERTRATION/HiLo/Low", "AVGP/22/EMA/High",  "AVGP/22/EMA/Low", "AVGP/22/Hilo/High",  "AVGP/22/Hilo/Low", "Force" ]
    frequencies = ['Daily']
    rootDir = '/Users/Shared/'
    for srcDir in SrcDirectories:
        RemoveQuotesByDirectory(rootDir + srcDir + '/Daily/')
        

def MakeUnique(fp):
    #command = '/usr/bin/perl -pi -w -e \'s/\\"//g, \' ' + fp
    command = 'sort ' + fp + ' | uniq > foo'
    subprocess.call(command, shell=True)
    

    

#MakeUnique("/Users/Shared/Quandl/EOD/Daily/SPY.csv")

#RemoveQuotesAllPrices()