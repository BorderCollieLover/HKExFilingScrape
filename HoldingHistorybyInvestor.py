# -*- coding: utf-8 -*-
"""
Created on Tue Dec 28 13:13:59 2021

@author: mtang
"""

#Holding History
#To extract the entire holdings history of one underlying by one investor

import pandas as pd
import numpy as np
import datetime as dt
from glob import glob
import os

HKFilingsDir = "X:\\HKExFilings\\"
FilingsByTickerDir = HKFilingsDir + "FilingsByTicker\\"
DIDir = HKFilingsDir + "DI\\"

#ticker = '00868'
#to_dt = dt.datetime.today()
#from_dt = to_dt - dt.timedelta(100)
#investor = 'lee sing din'




def holding_history_by_investor (ticker, investor):
    #from the filing by ticker file, extract the holding history of a particular investor
    ticker_filing_file = FilingsByTickerDir + ticker + ".csv"
    ticker_filing_header = ['Date', 'Code', 'Investor', 'Shares', 'Currency', 'HighPrice', 'AvgPrice', 'OffExHighPrice', 'OffExAvgPrice', 'LongPos', 
                            'LongPct', 'ShortPos', 'ShortPct', 'LongPosPre', 'LongPctPre', 'ShortPosPre', 'ShortPctPre', 'FilingURL', 'FileDate' ]
    data = pd.read_csv(ticker_filing_file, header=None, names =ticker_filing_header,  parse_dates=['Date', 'FileDate'])
    #data.columns = ticker_filing_header
    data = data[data.Investor==investor]
    output_data = data[['Date', 'Code', 'Shares', 'HighPrice', 'AvgPrice', 'OffExHighPrice', 'OffExAvgPrice','LongPos', 
                            'LongPct', 'ShortPos', 'ShortPct', 'LongPosPre', 'LongPctPre', 'ShortPosPre', 'ShortPctPre', 'FileDate', 'FilingURL', 'FileDate' ]]
    
    return(output_data)
    
def analyze_one_investor_history(investor_data):
    #codes to consider: 
    #Long: 1101: purchase, 1110/1111: being placed shares, 1113: others
    #Short: 1201: sale, 1209/1210: placing shares, 1213
    #Consider: a) net change of shares over the period examined; b) reported purchases and amt paid (and avg. price); c) reported sales and proceeds (and avg. price); 
    # and d) last reported purchase/sales dates; e) beginning of period and end of period % holdings 

    #focus on the following DI codes for now:
    #LONG: 1001, 1101, 1110, 1113
    #SHORT: 1201, 1209, 1211, 1213
    init_long_shrs = investor_data.iloc[0]['LongPosPre']
    init_long_pct = investor_data.iloc[0]['LongPctPre']
    init_short_shrs = investor_data.iloc[0]['ShortPosPre']
    init_short_pct = investor_data.iloc[0]['ShortPctPre']
    
    end_idx = len(investor_data.index)-1
    end_long_shrs = investor_data.iloc[end_idx]['LongPos']
    end_long_pct = investor_data.iloc[end_idx]['LongPct']
    end_short_shrs = investor_data.iloc[end_idx]['ShortPos']
    end_short_pct = investor_data.iloc[end_idx]['ShortPct']
    
    long_changes = end_long_shrs - init_long_shrs
    short_changes = end_long_shrs - init_long_shrs
    
    long_rpt_shrs = 0 
    long_rpt_amt = 0 
    short_rpt_shrs = 0
    short_rpt_amt =  0 
    last_long_dt = None
    last_short_dt = None
    form_code = None
    for i in range(len(investor_data.index)):
        highpx = investor_data.iloc[i]['HighPrice']
        avgpx = investor_data.iloc[i]['AvgPrice']
        highpxoff = investor_data.iloc[i]['OffExHighPrice']
        avgpxoff = investor_data.iloc[i]['OffExAvgPrice']
        
        if not pd.isna(avgpx): 
            px = avgpx;
        else:
            if not pd.isna(highpx):
                px = highpx;
            else:
                if not pd.isna(avgpxoff):
                    px = avgpxoff;
                else:
                    if not pd.isna(highpxoff):
                        px = highpxoff;
                    else:
                        px= 0 ; 
                        continue; 
                        
        shrs = investor_data.iloc[i]['Shares']
        amt = px * shrs 
        
        code = investor_data.iloc[i]['Code']
        
        if amt > 0: #only account for data that reported price for transactions 
            if ((code == 1001) or (code == 1101) or (code == 1110) or (code == 1113)):
                long_rpt_shrs = long_rpt_shrs + shrs
                long_rpt_amt = long_rpt_amt + amt
                last_long_dt = investor_data.iloc[i]['Date']
            else:
                if ((code == 1201) or (code == 1209) or (code == 1210) or (code == 1213)):
                    short_rpt_shrs = short_rpt_shrs + shrs 
                    short_rpt_amt = short_rpt_amt + amt
                    last_short_dt = investor_data.iloc[i]['Date']
                
        #form code:
        form_url = investor_data.iloc[i]['FilingURL']
        if ('Form1' in form_url):
            form_code = 'Form1'
        else:
            if ('Form2' in form_url):
                form_code = 'Form2'
            else:
                if ('Form3A' in form_url):
                    form_code = 'Form3A'
                    
    print([long_changes, long_rpt_shrs, long_rpt_amt,  last_long_dt, short_rpt_shrs, short_rpt_amt, last_short_dt, form_code,  end_long_shrs, init_long_shrs, end_long_pct, init_long_pct])
    return([long_changes, long_rpt_shrs, long_rpt_amt,  last_long_dt, short_rpt_shrs, short_rpt_amt, last_short_dt, form_code,  end_long_shrs, init_long_shrs, end_long_pct, init_long_pct])

    

def period_holding_changes(ticker, from_dt, to_dt):
    #examine the change in holdings over the period from from_dt to to_dt 
    #use as input the output from holding history by investor
    #try to ascertain a) the actual change in shares for the given period of time; b) the number of reported on/off exchange purchases and average price, 
    # c) the number of non-cash changes; and c) differences or residuals, i.e. changes in shares but not explicitly reported
    ticker_filing_file = FilingsByTickerDir + ticker + ".csv"
    ticker_filing_header = ['Date', 'Code', 'Investor', 'Shares', 'Currency', 'HighPrice', 'AvgPrice', 'OffExHighPrice', 'OffExAvgPrice', 'LongPos', 
                            'LongPct', 'ShortPos', 'ShortPct', 'LongPosPre', 'LongPctPre', 'ShortPosPre', 'ShortPctPre', 'FilingURL', 'FileDate' ]
    data = pd.read_csv(ticker_filing_file, header=None, names =ticker_filing_header,  parse_dates=['Date', 'FileDate'])
    #find the dates that are closest to from_dt and to_dt:
    data['Check_From_Dt'] = [(dt - from_dt).days >=0 for dt in data['Date']]
    data['Check_To_Dt'] = [(to_dt - dt).days >=0 for dt in data['Date']]
    data['Check_Dts'] = np.logical_and(data.Check_From_Dt, data.Check_To_Dt)
    
    data = data[data['Check_Dts']]
    
    if data.empty:
        return
    
    data.sort_values(by='Date', inplace=True)
    output_data = pd.DataFrame(data=None,columns=['Ticker', 'Investor', 'Long Delta', 'Rpt Purchase Shrs', 'Rpt Purchase Amt', 'Last Rpt Purchase Dt', 'Rpt Sale Shrs', 'Rpt Sale Amt', 'Last Rpt Sale Date', 'Form Code', 'End Long Shrs', 'Init Long Shrs', 'End Long Pct', 'Init Long Pct'] )
    
    investors = np.unique(data['Investor'])
    for investor in investors:
    #if True:
        #investor = 'quinn noel paul'
        investor_data = data [ data['Investor'] == investor]
        # beginning long, beginning long pct; 
        # end long, end long pct 
        # long shr chg, long shr rel chg 
        # reported buy shares; reported buy price 
        # reported sell shares; reported sell price 
        # last reported buy date 
        # last reported sell date 
        print(investor_data)
        investor_holding_summary =  analyze_one_investor_history(investor_data)
        output_data = output_data.append({'Ticker': ticker, 
                                          'Investor' : investor,
                                          'Long Delta': investor_holding_summary[0],
                                          'Rpt Purchase Shrs': investor_holding_summary[1],
                                          'Rpt Purchase Amt': investor_holding_summary[2],
                                          'Last Rpt Purchase Dt': investor_holding_summary[3], 
                                          'Rpt Sale Shrs': investor_holding_summary[4],
                                          'Rpt Sale Amt': investor_holding_summary[5], 
                                          'Last Rpt Sale Dt': investor_holding_summary[6],
                                          'Form Code': investor_holding_summary[7], 
                                          'End Long Shrs': investor_holding_summary[8], 
                                          'Init Long Shrs': investor_holding_summary[9], 
                                          'End Long Pct': investor_holding_summary[10],
                                          'Init Long Pct': investor_holding_summary[11]
                                          }, ignore_index=True)
        
    
    return(output_data)

dt1 = dt.datetime(year=1990,month=1,day=1)
dt2 = dt.datetime(year=2022,month=1,day=1)



def test_run_all(dt1, dt2):

    tickerfiles = glob(FilingsByTickerDir+ "*.csv")
    tickers = [os.path.splitext(os.path.basename(tickerfile))[0] for tickerfile in tickerfiles]
    
    for ticker in tickers:
        print(ticker)
        period_holding_changes(ticker, dt1, dt2)
        
def DailyUpdate():
    to_dt = dt.datetime.today()
    from_dt = to_dt - dt.timedelta(365)
    
    output_data = pd.DataFrame(data=None,columns=['Ticker', 'Investor', 'Long Delta', 'Rpt Purchase Shrs', 'Rpt Purchase Amt', 'Last Rpt Purchase Dt', 'Rpt Sale Shrs', 'Rpt Sale Amt', 'Last Rpt Sale Date', 'Form Code', 'End Long Shrs', 'Init Long Shrs', 'End Long Pct', 'Init Long Pct'] )
    
    tickerfiles = glob(FilingsByTickerDir+ "*.csv")
    tickers = [os.path.splitext(os.path.basename(tickerfile))[0] for tickerfile in tickerfiles]
    
    for ticker in tickers:
        print(ticker)
        ticker_data = period_holding_changes(ticker, from_dt, to_dt)
        output_data = pd.concat([output_data, ticker_data], ignore_index=True)
        
        
    #write out the data:
    output_data.to_csv(DIDir+to_dt.strftime("%Y%m%d")+".csv", index=False)
    for form_type in ["Form1", "Form2", "Form3A"]:
        form1_data = output_data[output_data['Form Code']==form_type]
        form1_data.to_csv(DIDir+form_type+".csv", index=False)
    
    

    
    
    
    
    
