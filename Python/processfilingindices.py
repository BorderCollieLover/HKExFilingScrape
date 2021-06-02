#!/usr/local/bin/python3


import csv
import re
from operator import itemgetter
import urllib.request
import os
import subprocess


## download company.idx from EDGAR ftp website
## grep 13F-HR company.idx > 13Fs.idx
## grep "SC 13" company.idx > SC13s.idx

## company name: 0~61 (with trailing blanks)
## CIK: 74:86 (with trailing blanks)

## This script examines the 13Fs.idx file and SC13s.idx files
## And then generate two files: full names versus (unique) CUSIPs and collapsed name versus (unique) CUSIPs
## While entities contained in 

def DownloadIdxFile():
    srcFile = 'https://www.sec.gov/Archives/edgar/full-index/company.idx'
    tgtFile = '/Users/Shared/Models/company.idx'
    if  os.path.isfile(tgtFile):
        os.remove(tgtFile)

    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
    except Exception as e:
        print(tgtFile)
        print(e)
    return

def GrepIdxFile():
    command = """/usr/bin/grep 13F-HR /Users/Shared/Models/company.idx > /Users/Shared/Models/13Fs.idx"""
    subprocess.call(command, shell=True)

    command = """/usr/bin/grep 'SC 13' /Users/Shared/Models/company.idx > /Users/Shared/Models/SC13s.idx"""
    subprocess.call(command, shell=True)
    return

def CleanString(inputStr):
    if not inputStr:
        return('')
    else:
        p = re.compile(',')
        inputStr = p.sub(' ',inputStr)
        p = re.compile('_')
        inputStr = p.sub(' ',inputStr)
        p = re.compile('\s+')
        inputStr = p.sub(' ',inputStr)
        inputStr = inputStr.strip()
        return(inputStr)

def CleanCIK(inputStr):
    if not inputStr:
        return('')
    else:
        inputStr = inputStr.strip()
        while (len(inputStr) < 10 ):
            inputStr = '0' + inputStr
        return(inputStr)


def fundtofilename(inputStr):
    p = re.compile('\W')
    inputStr = p.sub('', inputStr)
    p = re.compile('\s')
    inputStr = p.sub('', inputStr)
    return(inputStr)


def UpdateFilingIndices():
    CIKfile = '/Users/Shared/Models/fullCIKs.csv'
    with open(CIKfile, 'r') as csvfile:
        TargetFunds = [tuple(line) for line in csv.reader(csvfile)]

    dictCIKFund = dict()
    for fund in TargetFunds:
        dictCIKFund[fund[1]] = fund[0]

    ### Generate files from 13Fs.idx
    fund13Flist = '/Users/shared/models/13Fs.idx'
    with open(fund13Flist) as f:
        content = f.readlines()

    newCIKFund = dict()
    for line in content:
        fundname = CleanString(line[0:61])
        fundcik = CleanCIK(line[74:86])
        if not ((fundcik in dictCIKFund) or (fundcik in newCIKFund)):
            newCIKFund[fundcik] = fundname

    newFunds = []
    for key, value in newCIKFund.items():
        newFunds += [(value, key)]

    newFunds = sorted(newFunds, key=itemgetter(0))
    filename = '/Users/Shared/Models/13F CIK fullname.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(newFunds)

    newFunds = []
    for key, value in newCIKFund.items():
        value = fundtofilename(value)
        newFunds += [(value, key)]

    newFunds = sorted(newFunds, key=itemgetter(0))
    filename = '/Users/Shared/Models/13F CIKs.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(newFunds)


    ### Generate files from SC13s.idx
    SC13list = '/Users/shared/models/SC13s.idx'
    with open(SC13list) as f:
        content = f.readlines()

    newCIKFund2 = dict()
    for line in content:
        fundname = CleanString(line[0:61])
        fundcik = CleanCIK(line[74:86])
        if not ((fundcik in dictCIKFund) or (fundcik in newCIKFund) or (fundcik in newCIKFund2)) :
            newCIKFund2[fundcik] = fundname


    newFunds2 = []
    for key, value in newCIKFund2.items():
        newFunds2 += [(value, key)]

    newFunds2 = sorted(newFunds2, key=itemgetter(0))
    filename = '/Users/Shared/Models/SC13 CIK fullname.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(newFunds2)

    newFunds2 = []
    for key, value in newCIKFund2.items():
        value = fundtofilename(value)
        newFunds2 += [(value, key)]

    newFunds2 = sorted(newFunds2, key=itemgetter(0))
    filename = '/Users/Shared/Models/SC13 CIKs.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(newFunds2)


    ### Create the full CIKs.csv file by combining unique CIKs from 13Fs.idx and SC13s.idx
    newFunds = []
    for key, value in newCIKFund.items():
        newFunds += [(value, key)]
    for key, value in newCIKFund2.items():
        newFunds += [(value, key)]
    newFunds = sorted(newFunds, key=itemgetter(0))
    filename = '/Users/Shared/Models/new CIK fullname.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(newFunds)

    newFunds = []
    for key, value in newCIKFund.items():
        value = fundtofilename(value)
        newFunds += [(value, key)]
    for key, value in newCIKFund2.items():
        value = fundtofilename(value)
        newFunds += [(value, key)]
    newFunds = sorted(newFunds, key=itemgetter(0))
    filename = '/Users/Shared/Models/new CIKs.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(newFunds)


    ## Combine with initial manually setup list to create the full CIKs.csv file
    TargetFunds += newFunds
    filename = '/Users/Shared/Models/fullCIKs.csv'
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer=csv.writer(csvfile)
        writer.writerows(TargetFunds)

    return


def CleanupTmpIndexFiles():
    os.remove('/Users/Shared/Models/company.idx')
    os.remove('/Users/Shared/Models/13Fs.idx')
    os.remove('/Users/Shared/Models/SC13s.idx')
    os.remove('/Users/Shared/Models/13F CIK fullname.csv')
    os.remove('/Users/Shared/Models/13F CIKs.csv')
    os.remove('/Users/Shared/Models/SC13 CIK fullname.csv')
    os.remove('/Users/Shared/Models/SC13 CIKs.csv')
    os.remove('/Users/Shared/Models/new CIK fullname.csv')
    os.remove('/Users/Shared/Models/new CIKs.csv')

    return




DownloadIdxFile()
GrepIdxFile()
UpdateFilingIndices()
CleanupTmpIndexFiles()
