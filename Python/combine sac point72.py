import datetime
import re
from dateutil import parser
import csv
import os
import pandas as pd
from pandas.tseries.offsets import BDay
import gzip
from pytz import timezone
import pytz
import unicodedata
import sys
import locale
import getpass
import time
from glob import glob
from operator import itemgetter
from collections import Counter


outputpath = '/Users/Shared/'
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
dataFile = outputpath + 'SC13Forms/Point72.csv'
dataFile2 = outputpath + 'SC13Forms/SACCAPITALADVISORSLLC.csv'
dataFile2 = outputpath + 'SC13Forms/SIGMACAPITALMANAGEMENTLLC.csv'
try:
    with open(dataFile, 'r') as csvfile:
        fundData = [tuple(line) for line in csv.reader(csvfile)]
except Exception as e:
    print (e)
    fundData = None

try:
    with open(dataFile2, 'r') as csvfile:
        fundData2 = [tuple(line) for line in csv.reader(csvfile)]
except Exception as e:
    print (e)
    fundData2 = None


datalink1 = [line[len(line)-1] for line in fundData]
datalink1 = list(set(datalink1))
print(len(fundData))
print(len(datalink1))

print(len(fundData2))
tmpData = [tuple(line) for line in fundData2 if line[len(line)-1] in datalink1]
fundData2 = list(set(fundData2) - set(tmpData))
print(len(fundData2))


##find repeated urls
datalink1 = [line[len(line)-1] for line in fundData]
datalinkcounter = Counter(datalink1).most_common(10)
duplicatedlinks = [line[0] for line in datalinkcounter if line[1]>1]
print(duplicatedlinks)
duplicatedlines = [tuple(line) for line in fundData if line[len(line)-1] in duplicatedlinks]
for line in duplicatedlines:
    print(line)



print(len(fundData2))
tmpData = [tuple(line) for line in fundData2 if line[len(line)-1] in datalink1]
fundData2 = list(set(fundData2) - set(tmpData))
print(len(fundData2))
#print(fundData2[460:])

fundData += fundData2


#with open(dataFile, 'w', encoding='utf-8') as csvfile:
#    writer = csv.writer(csvfile)
#    writer.writerows(fundData)
