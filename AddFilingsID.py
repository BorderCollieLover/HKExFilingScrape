# -*- coding: utf-8 -*-
"""
Created on Thu Jun  2 16:04:33 2022

@author: mtang
"""


from bs4 import BeautifulSoup
from urllib.request import urlopen
import datetime
import re
import csv
import os
import pandas as pd
from urllib.error import URLError, HTTPError
from operator import itemgetter
import locale
import shutil
from glob import glob
import numpy as np



import FileToolsModule as FTM
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')



DriveName = "V:"
HKFilingsDir = DriveName+"\\HKExFilings\\"
FilingsByTickerDir = HKFilingsDir + "FilingsByTicker\\"
FilingsByFundDir = HKFilingsDir + "FilingsByFund\\"
BackupDir =  HKFilingsDir + "Backup\\"
DailyFilingsDir = HKFilingsDir + "DailyFilings\\"
DIDir = HKFilingsDir + "DI\\"

FundsList = HKFilingsDir + "Funds.csv"
CompleteFundsList = HKFilingsDir + "AllFunds.csv"
ScrapedFormsList = HKFilingsDir + "FullScrapingList.csv"
HistoricalForms2ScrapeList = HKFilingsDir + "HistoricalList2Scrape.csv"
ErrorLog = HKFilingsDir + "Log.csv"


def AddFormIDtoFilingData(filename):
    FilingData = pd.read_csv(filename)
    return(FilingData)


    