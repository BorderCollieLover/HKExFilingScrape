from selenium import webdriver
import time
from urllib.parse import urlencode
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
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
import os
os.environ['R_HOME'] = '/Library/Frameworks/R.framework/Resources'
#import rpy2.robjects.packages as rpackages
#import rpy2
from glob import glob
import re
import math
import holidays
from bdateutil import isbday
import pytz
#import urllib
#from urllib import urlopen
from socket import timeout
import socket
import io
import signal
from subprocess import check_output
import zipfile
from zipfile import ZipFile
#import dask.dataframe as dd
import pandas as pd


import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
from statsmodels.sandbox.regression.predstd import wls_prediction_std
np.random.seed(9876789)
nsample = 100
x = np.linspace(0, 10, 100)
X = np.column_stack((x, x**2))
beta = np.array([1, 0.1, 10])
e = np.random.normal(size=nsample)

exit()

x = [0,1,2,3,4,5,6,7,8,9]
print(x)
print(x[:5])
print(x[len(x):])
y = x[len(x):]
print(y is None)
print(y ==[])
print(len(y))
exit()


filename = '/Users/Shared/Quandl/EOD/tmp/EOD_20170617.csv'
df = dd.read_csv(filename, header=None)
df2 = df[df[0] == 'A']
#maddts = df.groupby(df[0])[1].max()
#summaryData = pd.DataFrame({'ticker':df[0], 'startDt':df.groupby(df[0])[1].min().compute(), 'endDt':df.groupby(df[0])[1].max().compute()})
#print(summaryData)
s = df2[1].min().compute()
s2 = df2[1].max().compute()
#print(df.head(2))
#print(s2.value
#print(df2[1])
print(s,s2)
#print(s.values(), s2.values())
#print(s.head())
#print(df2.max(df2[1]))
#print(df2.min(df2[1]))


exit()

#with ZipFile('/Users/Shared/Quandl/EOD/tmp/2017-05-24.zip', 'w') as myzip:
#    myzip.write('/Users/Shared/Quandl/EOD/tmp/2017-05-24.csv', os.path.basename('/Users/Shared/Quandl/EOD/tmp/2017-05-24.csv'),  compress_type=zipfile.ZIP_DEFLATED)
#ZipFile.close()



def DownloadCurrentCrawler():
    srcFile = 'https://www.sec.gov/Archives/edgar/full-index/crawler.idx'
    tgtFile = '/Users/Shared/SEC/Crawlers/Crawler.idx'
    try:
        urllib.request.urlretrieve(srcFile, tgtFile)
        urllib.request.urlcleanup()
    except Exception as e:
        print(tgtFile)
        print(e)

    #Crawler2LinksTable(tgtFile)

DownloadCurrentCrawler()
exit()

quandl.ApiConfig.api_key = "3ojxoNzaKndioRPW_hXc"




dataset_list = []
next_page_id = 2
while True:
    if next_page_id:
        tmp = quandl.Database('HKEX').datasets(params = { 'page' : next_page_id})
    else:
        tmp = quandl.Database('HKEX').datasets()
    print(tmp.meta)
    next_page_id = tmp.meta['next_page']
    dataset_list += tmp
    if (next_page_id > 3):
        break
    if not next_page_id:
        break

print(len(dataset_list))
print(dataset_list)
for kk in range(2):
    x = dataset_list[kk].data_fields()
#x = tmp[0].data()
    for field in x:
        print(field, ': ', dataset_list[kk][field])
#print(tmp[0]['column_names'])

print(dataset_list[0].data())
exit()


myData = quandl.get("ODE/ABU2014")
print(len(myData))
tmp1 = myData.keys()
print(tmp1[1])
print(myData[tmp1[1]])
x = myData[tmp1[1]]
print(x.keys())
print(myData)
tmp = np.array(myData)
print(tmp)
print(tmp.dtype.names)
filename = '/Users/Shared/test.csv'
with open(filename, 'w', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(myData)

exit()


