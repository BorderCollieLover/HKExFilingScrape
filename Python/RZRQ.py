import datetime
import os
from pytz import timezone
import requests
import pandas as pd 
from pandas.tseries.offsets import BDay
import socket
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities


user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100'
headers = { 'User-Agent' : user_agent }

seleniumopts = Options()
seleniumopts.add_argument("user-agent=" + user_agent)

dcap = dict(DesiredCapabilities.PHANTOMJS)
dcap["phantomjs.page.settings.userAgent"] = (
    user_agent
)


def ScrapeSSE():
    ScrapingDt = datetime.datetime.now(timezone('EST'))
    webdriverwaittime = 0.1
    webdrivertimeout = 30
    # failCtrThreshold is used to control re-try of finding elements in PhantomJS returns
    # use +1 to so that
    failCtrThreshold = int((webdrivertimeout / webdriverwaittime))+1



    scrapingdtstring = ScrapingDt.strftime('%Y-%m-%d %H:%M')

    sseUrl = "https://www.szse.cn/main/disclosure/rzrqxx/rzrqjy/"

    driver = webdriver.PhantomJS(executable_path='/usr/local/Cellar/phantomjs/2.1.1/bin/phantomjs',
                                 desired_capabilities=dcap,
                                 service_args=['--ignore-ssl-errors=true', '--ssl-protocol=any'])

    tmps = None
    try:
        driver.set_page_load_timeout(webdrivertimeout)
        driver.get(sseUrl)
        #driver.implicitly_wait(webdriverwaittime)
        #time.sleep(webdriverwaittime)
        #print(len(tmps))
        #WebDriverWait(driver, webdriverwaittime).until(EC.presence_of_element_located(By.TAG_NAME, "select"))
        #WebDriverWait(driver, webdriverwaittime).until(EC.visibility_of_element_located(By.TAG_NAME, "Select"))
        #WebDriverWait(driver, webdriverwaittime)
        failCtr = 1
        tmp = None
    except Exception as e:
        print(e)

    print('finished loading')



# timeout in seconds
timeout = 2
socket.setdefaulttimeout(timeout)

        
## Margin activity reports from SSE
##'http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk20150706.xls'
curDt = pd.datetime.today()-BDay(1)
failCtr = 0 
while True:
    filename = '/Users/shared/RZRQ/'+ curDt.strftime('%Y%m%d') + '.xls'
    if os.path.isfile(filename):
        break;
##    if os.path.isfile(filename):
##        curDt = curDt - BDay(1)
##        continue;
    fileurl = 'http://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk' + curDt.strftime('%Y%m%d') + '.xls'
    print(fileurl)
    try:
        resp = requests.get(fileurl)
        if resp.raise_for_status() is None:
            with open(filename, 'wb') as output:
                output.write(resp.content)
    except Exception as e:
        print(e)
        failCtr += 1
        
    if (failCtr == 100):
        failCtr = 0
        curDt = pd.datetime.today()
    else:
        curDt = curDt - BDay(1)


## Share repurchase reports from HKEx
curDt = pd.datetime.today()
failCtr = 0
while True:
    filename = '/Users/shared/HKEx/Repurchase/'+ curDt.strftime('%Y%m%d') + '.xls'
    if os.path.isfile(filename):
        break;

##    if os.path.isfile(filename):
##        curDt = curDt - BDay(1)
##        continue;
    
    fileurl = 'https://www.hkexnews.hk/reports/sharerepur/documents/SRRPT' + curDt.strftime('%Y%m%d') + '.xls'
##    fileurl = 'http://www.hkexnews.hk/reports/sharerepur/documents/' + curDt.strftime('%Y%m%d') + '.pdf'
    print(fileurl)
    try:
        resp = requests.get(fileurl, verify=False)
        if resp.raise_for_status() is None:
            with open(filename, 'wb') as output:
                output.write(resp.content)
    except Exception as e:
        print(e)
        failCtr += 1
        
    if (failCtr == 100):
        failCtr = 0
        curDt = pd.datetime.today()
    else:
        curDt = curDt - BDay(1)

#download repurchase reports of GEM stocks in HKEx
curDt = pd.datetime.today()
failCtr = 0
while True:
    filename = '/Users/shared/HKEx/GEMRepurchase/'+ curDt.strftime('%Y%m%d') + '.xls'
    if os.path.isfile(filename):
        break;

##    if os.path.isfile(filename):
##        curDt = curDt - BDay(1)
##        continue;
    

    fileurl = 'https://www.hkexnews.hk/reports/sharerepur/GEM/documents/SRGemRPT' + curDt.strftime('%Y%m%d') + '.xls'
    print(fileurl)
    try:
        resp = requests.get(fileurl, verify=False)
        if resp.raise_for_status() is None:
            with open(filename, 'wb') as output:
                output.write(resp.content)
    except Exception as e:
        print(e)
        failCtr += 1
        
    if (failCtr == 100):
        failCtr = 0
        curDt = pd.datetime.today()
    else:
        curDt = curDt - BDay(1)

#http://www.szse.cn/szseWeb/FrontController.szse?ACTIONID=8&CATALOGID=1837_xxpl&tab2PAGENUM=1&ENCODE=1&TABKEY=tab2
#http://www.szse.cn/szseWeb/FrontController.szse?ACTIONID=8&CATALOGID=1837_xxpl&txtDate=2015-07-02&tab2PAGENUM=1&ENCODE=1&TABKEY=tab2
