# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 17:43:38 2018

@author: mintang
"""

import subprocess
import sys
sys.path.insert(0, '/Users/Shared/Models/Python/')
sys.path.insert(0, '/Users/Shared/USMarkets/Scripts/')

try:
    import DownloadNDX100
    import NDXSectorInfo
except Exception as e: 
    print(e)
sys.path.insert(0, '/Users/Shared/SECFTD/Scripts/')
import DownloadFtDData as DFD

import datetime as dt
if (dt.date.today().isoweekday() == 6 ):
    try:
        #subprocess.call('/Users/Shared/Models/Python/processfilingindices.py',shell=True)
        import processfilingindices
    except Exception as e: 
        print(e)
    
    try:
        import ScrapeWikiSP500
    except Exception as e: 
        print(e)
    
    try:
        DFD.DownloadFtD()
    except Exception as e: 
        print(e)
        
    try:
        import QuandlLiquidityTools as QLT
        QLT.UpdateQuandlLiquidity()
    except Exception as e: 
        print(e)
    
    try:
        import BulkDownloadQuandlEOD
    except Exception as e: 
        print(e)
        
    
