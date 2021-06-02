import urllib.request
from datetime import date
import os
import socket

timeout = 5
socket.setdefaulttimeout(timeout)

startY = 1993
endY = date.today().year+1
qtrs =['QTR1', 'QTR2', 'QTR3', 'QTR4']

while True:
    for i in range(startY, endY):
        for curQ in qtrs:
            srcFile = 'ftp://ftp.sec.gov/edgar/full-index/' + str(i) +'/' + curQ + '/' + 'crawler.idx'
            tgtFile = '/Users/Shared/SEC/Crawlers/'+str(i) + curQ+'.idx'
            #print(srcFile)
            #print(tgtFile)
            if (not os.path.isfile(tgtFile)):
                try:
                    urllib.request.urlretrieve(srcFile, tgtFile)
                except Exception as e:
                    print(tgtFile)
                    print(e)

