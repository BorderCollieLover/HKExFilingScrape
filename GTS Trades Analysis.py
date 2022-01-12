# -*- coding: utf-8 -*-
"""
Created on Thu Dec 23 14:52:33 2021

@author: mtang
"""

import pandas as pd
#import xlwt

input_file = "V:\\10237 2021 Trades.csv"
output_file = "V:\\10237 2021 Trade Summary.csv"

all_mk_accts = ['00005', '00006', '10040', '10041', '10123']

def trade_analysis_onefile (input_file, output_file):
    if input_file.lower().endswith('.csv'):
        trades = pd.read_csv(input_file)
    else:
        if input_file.lower().endswith('.xlsx'):
            trades = pd.read_excel(input_file, engine="openpyxl")
        else:
            return
    tickers = trades['product_id'].unique()
    
    
    trade_summary_header = ['product_id', 
                     'Long Position', 'Avg. Long Px', 'Short Position', 'Avg. Short Px', 'Net Position', 'PnL', 'Net PnL Per Share']
    trade_summary = pd.DataFrame(columns=trade_summary_header)
    
    for ticker in tickers: 
        trade_data = trades[trades['product_id']==ticker]
        b_data = trade_data[trade_data['bs_flag']=='B']
        s_data = trade_data[trade_data['bs_flag']=='S']
        long_position = sum(b_data['qty'])
        short_position = sum(s_data['qty'])
        long_costs = sum(b_data['gross_amt'])
        short_proceeds = sum(s_data['gross_amt'])
        net_position = long_position - short_position
        if (net_position == 0):
            pnl = short_proceeds - long_costs 
            net_per_share = pnl/long_position
        else:
            pnl = 'N/A'
            net_per_share = 'N/A'
        if (long_position > 0):
            avg_long = long_costs/long_position 
        else:
            avg_long ='N/A'
        if (short_position>0):
            avg_short = short_proceeds / short_position
        else:
            avg_short = 'N/A'
        trade_summary = trade_summary.append({
            'product_id': ticker, 
            'Long Position': long_position,
            'Short Position': short_position,
            'Avg. Long Px': avg_long,
            'Avg. Short Px': avg_short, 
            'Net Position': net_position,
            'PnL': pnl,
            'Net PnL Per Share': net_per_share
            
        }, ignore_index=True)
        
    trade_summary.to_excel(output_file, index=False, engine="openpyxl")

def analyze_all_mk_trades(path):
    all_trades = pd.DataFrame()
    for acct_type in ['C', 'M']:
        for acct_no in all_mk_accts:
            input_file = path+"\\"+acct_type+acct_no+".xlsx"
            trades = pd.read_excel(input_file)
            if all_trades.empty:
                all_trades = trades 
            else:
                all_trades = pd.concat([all_trades, trades])
            trade_analysis_onefile (input_file,  path+"\\"+acct_type+acct_no+" Summary.xlsx")
                
            
                
    all_trades.to_excel(path+"\\all_mk_trades.xlsx", index=False,engine="openpyxl")
    trade_analysis_onefile(path+"\\all_mk_trades.xlsx",path+"\\all_mk_trades summary.xlsx" )
    
    
    

    

