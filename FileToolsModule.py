import os
import re
import csv
from operator import itemgetter
import datetime
import pandas as pd


#returns true if both tgtfile and srcfile exist and tgtfile is "newer" than srcfile
#srcfile is usually the "original data", e.g. price file
#tgtfile is usually the "derived data", e.g. calcualtion based on price 
#use to determine if tgtfile needs updating
def CheckTargetFileNewer(tgtfile, srcfile):
    if (os.path.isfile(tgtfile) and os.path.isfile(srcfile)):
        tgtfile_time = datetime.datetime.fromtimestamp( os.path.getmtime(tgtfile))
        srcfile_time = datetime.datetime.fromtimestamp( os.path.getmtime(srcfile))
        if ((tgtfile_time - srcfile_time).total_seconds() >0):
            return True
        else:
            return False
    else:
        return False
    
def Deduplicate(datafile):
    if os.path.isfile(datafile):
        data = pd.read_csv(datafile, index_col=0)
        old_rows = len(data.index)
        #print(ticker, old_rows)
        #data = ((data.reset_index()).drop_duplicates(subset='index', keep='last')).set_index('index')
        data=data[~data.index.duplicated(keep='last')]
        new_rows = len(data.index)
        if (new_rows< old_rows):
            print(datafile, old_rows, new_rows)
            data.to_csv(datafile)
    return


#1. Append a string to a file
def AddString2File(filename, datastr):
    fileExists = os.path.isfile(filename)
    if fileExists:
        with open(filename, 'a+') as f:
            f.write(datastr)
            f.write('\n')
    else:
        with open(filename, 'w') as f:
            f.write(datastr)
            f.write('\n')
    return

def CleanString(inputStr):
    if not inputStr:
        return ('')
    else:
        p = re.compile(',')
        inputStr = p.sub(' ', inputStr)
        p = re.compile('_')
        inputStr = p.sub(' ', inputStr)
        p = re.compile('-')
        inputStr = p.sub(' ', inputStr)
        p = re.compile('\<\s*FONT\s*.*\>')
        inputStr = p.sub('', inputStr)
        p = re.compile('\<\s*DIV\s*.*\>')
        inputStr = p.sub('', inputStr)

        p = re.compile('\<\s*br\s*(\/)?\>', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\<\s*\/FONT\s*\>', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\<\s*\/TD\s*\>', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\<\s*\/B\s*\>', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\<\s*\/p\s*\>', re.I)
        inputStr = p.sub(' ', inputStr)
        p = re.compile('\<\s*\/div\s*\>', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\<\s*\/CENTER\s*\>', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\*', re.I)
        inputStr = p.sub('', inputStr)
        p = re.compile('\s+')
        inputStr = p.sub(' ', inputStr)
        inputStr = inputStr.strip()
        if (inputStr == ' '):
            inputStr = ''
        return (inputStr)


def SafeSaveData(tgtFile, fileData):
    if (fileData is None) or (len(fileData)<1):
        return

    if os.path.isfile(tgtFile):
        try:
            try:
                with open(tgtFile, encoding='utf-8') as f:
                    tmpData = [tuple(line) for line in csv.reader(f)]
            except Exception as e:
                print(e)
            try:
                if (fileData == tmpData):
                    return
                else:
                    if len(fileData) < len(tmpData):
                        print("Warning: " +tgtFile + " will contain less records!")
                    try:
                        with open(tgtFile, 'w', encoding='utf-8',  newline='') as csvfile:
                            writer = csv.writer(csvfile)
                            writer.writerows(fileData)
                            #print('written')
                    except Exception as e:
                        print(e)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
            return
    else:
        dir_path = os.path.dirname(os.path.realpath(tgtFile))
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(tgtFile, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(fileData)
    return


def SafeAddData(tgtFile, fileData, sortKeys=[]):
    if (fileData is None) or (len(fileData)<1):
        return

    if os.path.isfile(tgtFile):
        try:
            with open(tgtFile, encoding='utf-8') as f:
                tmpData = [tuple(line) for line in csv.reader(f, csv.QUOTE_NONNUMERIC)]
            #print(tmpData[:2])
            #print(fileData[:2])
            fullData = fileData + tmpData
            fullData = list(set(fullData))
            #if (fileData == tmpData):
            #    return
            #else:
            if (len(sortKeys)>0):
                fullData = sorted(fullData, key=itemgetter(*sortKeys))
                if (fullData == tmpData):
                    return

            if len(fullData) < len(tmpData):
                print("Warning: " +tgtFile + " will contain less records!")
            with open(tgtFile, 'w', encoding='utf-8',  newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(fullData)
        except Exception as e:
            return
    else:
        dir_path = os.path.dirname(os.path.realpath(tgtFile))
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(tgtFile, 'w', encoding='utf-8',  newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(fileData)
    return


def ReadCSV2Dict(infile):
    new_dict = dict()

    if os.path.isfile(infile):
        try:
            with open(infile, encoding='utf-8') as f:
                tmpData = [tuple(line) for line in csv.reader(f)]
        except Exception as e:
            print(e)
        try:
            for row in tmpData:
                if len(row)==2:
                    new_dict[row[0]]=row[1]
                else:
                    new_dict[row[0]]=row[1:]
        except Exception as e:
            print(e)
    else:
        print(infile + " doesn't exist.")

    return(new_dict)


def SaveDict2CSV(tofile, data_dict):
    if len(data_dict)<1:
        return

    outputData = []
    for key, value in data_dict.items():
        if isinstance(value, str):
            outputData += [(key, value)]
        else:
            #value_iterable = False
            try:
                value_iterator = iter(value)
                #print('here')
                #for val in value_iterator:
                #    print(val)
                outputData += [(key,) + tuple(value_iterator)]
            except TypeError as te:
                print(str(value)+' is not iterable')
                print(te)
                outputData += [(key, value)]

    #print(outputData)
    SafeSaveData(tofile, outputData)
    return


    

# x = ReadCSV2Dict("/Users/Shared/Models/MostLiquidETFs.csv")
# print(x)
# print(x['SPY'])
# SPY_volume = x['SPY'][1]
# SPY_volume = int(SPY_volume)+1
# print(SPY_volume)
# x['SPY']=(x['SPY'][0], int(x['SPY'][1])+1, float(x['SPY'][1])+2)
# print(x['SPY'])
#
# SaveDict2CSV("/Users/Shared/foo", x)
#
# x = ReadCSV2Dict("/Users/Shared/Models/CUSIPTicker.csv")
# print(x)
# SaveDict2CSV("/Users/Shared/foo1", x)
