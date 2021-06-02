import quandl
import pandas as pd
import os
import datetime
import re
import csv
#import numpy as np
import signal 


def timeout_handler(num, stack):
    print("Received SIGALRM")
    raise Exception("Quandl call hang")


EOD_METADATA_FILE = '/Users/Shared/QUANDL/EOD/MetaData.csv'
if os.path.isfile(EOD_METADATA_FILE):
    EOD_METADATA = pd.read_csv(EOD_METADATA_FILE, header=0, index_col=0, keep_default_na=False)
else:
    EOD_METADATA = pd.DataFrame()

def DeduplicateEODData(ticker):
    datafile = '/Users/Shared/Quandl/EOD/Daily/' + ticker + '.csv'
    #datafile = '/Users/Shared/foo.csv'
    if os.path.isfile(datafile):
        data = pd.read_csv(datafile, index_col=0)
        old_rows = len(data.index)
        #print(ticker, old_rows)
        #data = ((data.reset_index()).drop_duplicates(subset='index', keep='last')).set_index('index')
        data=data[~data.index.duplicated(keep='last')]
        new_rows = len(data.index)
        if (new_rows< old_rows):
            print(ticker, old_rows, new_rows)
            data.to_csv(datafile)
    return


#                signal.signal(signal.SIGALRM, timeout_handler)
#                signal.alarm(10)
#                try:
#                    quandl_price = quandl.get(ticker_str, start_date=start_str, collapse='quarterly')
#                except Exception as e: 
#                    print(e)
#                    signal.alarm(0)
#                    continue
#                finally:
#                    signal.alarm(0)
#                signal.alarm(0)


def UpdateEODData(ticker, freq='none',EOD_METADATA = EOD_METADATA ):
    quandl.ApiConfig.api_key = '3ojxoNzaKndioRPW_hXc'
    p = re.compile('\.')
    ticker = p.sub('_', ticker)
    
    if not EOD_METADATA.empty:
        if not (ticker in EOD_METADATA.index):
            return
        else:
            if ((len(EOD_METADATA.loc[ticker, 'from_date'])<1) or (len(EOD_METADATA.loc[ticker, 'to_date'])<1)):
                return
        
    
    if freq=='none':
        datafile = '/Users/Shared/Quandl/EOD/Daily/' + ticker + '.csv'
    else:
        datafile = '/Users/Shared/Quandl/EOD/' + freq + '/' +  ticker + '.csv'
        if not  os.path.exists('/Users/Shared/Quandl/EOD/' + freq + '/' ):
            os.makedirs('/Users/Shared/Quandl/EOD/' + freq + '/' )
        
    #datafile = '/Users/Shared/foo.csv'
    if os.path.isfile(datafile):
        try:
            curData = pd.read_csv(datafile, index_col=0, parse_dates=True)
        except Exception as e:
            curData = pd.DataFrame()
            print(e)
            os.remove(datafile)
        #1. get the last line in the pandas dataframe
        #2. Retrieve data from the last known date
        #3. If data len is greater than 1, then
        #3.1 compare the adj_close of the overlapping date
        #3.2 if the same, then append the data
        #3.3 else retrieve full data and save to file
        #print(curData.iloc[len(curData)-1])
        if curData.empty:
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(10)
            try:
                data = quandl.get('EOD/' + ticker, collapse=freq)
            except Exception as e:
                print(e)
                signal.alarm(0)
                return
            finally:
                signal.alarm(0)
            signal.alarm(0)
                
        else:
            lastDtStr = (curData.index)[len(curData.index)-1]

            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(10)
            try:
                data = quandl.get('EOD/' + ticker,  start_date=lastDtStr, collapse=freq)
            except Exception as e: 
                print(e)
                signal.alarm(0)
                return
            finally:
                signal.alarm(0)
            signal.alarm(0)
            
            if (len(data)>1):
                newDt = data.index[0]
                #print(curData.iloc[len(curData)-1])
                #print(data.iloc[0])
                #newDtStr = datetime.datetime.strftime(newDt,'%Y-%m-%d')
                if (lastDtStr == newDt):
                    if (curData.loc[lastDtStr, 'Adj_Close'] != data.loc[newDt, 'Adj_Close']):
                        
                        signal.signal(signal.SIGALRM, timeout_handler)
                        signal.alarm(10)
                        try:
                            data = quandl.get('EOD/' + ticker, collapse=freq)
                        except Exception as e: 
                            print(e)
                            signal.alarm(0)
                            return
                        finally:
                            signal.alarm(0)
                        signal.alarm(0)
                        
                    else:
                        data.drop(data.head(1).index, inplace=True)
                        data=curData.append(data)
                else:
                    print('Something is wrong')
                    
                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(10)
                    try:
                        data = quandl.get('EOD/' + ticker, collapse=freq)
                    except Exception as e: 
                        print(e)
                        signal.alarm(0)
                        return
                    finally:
                        signal.alarm(0)
                    signal.alarm(0)
            else:
                return
    else:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(10)
        try:
            data = quandl.get('EOD/' + ticker, collapse=freq)
        except Exception as e:
            print(e)
            signal.alarm(0)
            return
        finally:
            signal.alarm(0)
        signal.alarm(0)
        
    data = data.drop_duplicates()
    #for lower frequency data (e.g. quarterly), quandl seems to fill forward to quarter end using last available data 
    while True:
        last_data_dt = data.index[len(data.index)-1]
        #print(last_data_dt)
        if (datetime.datetime.now() <last_data_dt):
            data.drop(data.tail(1).index, inplace=True)
        else:
            break
    
    data.to_csv(datafile)
    DeduplicateEODData(datafile)
    return(data)
    



def DeduplicateAll():
    quand_ticker_file = "/Users/Shared/Quandl/EOD/tickers.txt"
    tickers = []
    try:
        with open(quand_ticker_file, 'r') as csvfile:
            tickerlines = [tuple(line) for line in csv.reader(csvfile)]
            tickers = list(set([line[0] for line in tickerlines]))
    except Exception as e:
        print(e)
        
    if (len(tickers)>1):
        for ticker in tickers: 
            DeduplicateEODData(ticker)
    return
            
        
    

#DeduplicateAll()
#DeduplicateEODData("CTRP")
#UpdateEODData('CTRP')