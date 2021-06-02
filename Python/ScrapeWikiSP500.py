#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  7 16:15:52 2018

@author: mintang
"""

# =============================================================================
# scrape list of S&P500 companies from wikipedia using python 3
# Wikpedia page: https://en.wikipedia.org/wiki/List_of_S%26P_500_companies 
# Generates 2 main output lists: 
# 1. sp_current_companies
# general format: 
# [['Ticker Symbol', 'Company Name', 'GICS Sector', 'GICS Sub Indudtry']]
# example element: 
# [['MMM', '3M Company', 'Industrials', 'Industrial Conglomerates']]
# 2. sp_company_changes
# general format: 
# [[['start_date', 'end_date'], 
# ['added_co1', 'added_co2', ...], 
# ['removed_co1', removed_co2, ...], 
# [List of Ticker Symbols Active in Date Range]]]
# example element: 
# [[['2017-06-19', '2017-07-16'], 
# ['HLT', 'ALGN', 'ANSS', 'RE'], 
# ['YHOO', 'TDC', 'R', 'MJN'], 
# ['A', 'AAL', 'AAP', 'AAPL', 'ABBV', ...]]]
# =============================================================================

from lxml import etree
import requests
import time
import csv
import os
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/Users/Shared/Models/Python/')
import FileToolsModule as FTM


r = requests.get(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

#there are 2 main tables to scrape, but both have the same name
# so grab each block of html table text separately 
sp_co_start = r.text.index('<table class="wikitable sortable" id="constituents">')
sp_co_end = r.text.index('</table>')
print(sp_co_start)
print(sp_co_end)
sp_co_text = r.text[sp_co_start:sp_co_end]
#print(len(sp_co_text))
#print(sp_co_text)
#r_start = sp_co_text.index('<tbody>')
#r_end = sp_co_text.index('</tbody>')
#sp_co_text = sp_co_text[r_start:r_end]

sp_chg_start =  r.text.index('<table class="wikitable sortable">', sp_co_end)
sp_chg_end = r.text.index('</table>', sp_chg_start) 
sp_chg_text = r.text[sp_chg_start:sp_chg_end]

del r 


#loop over HTML text and extract the table rows into list elements for 
# parsing later
trList = []
startpos = sp_co_text.index('<tr>')
endpos = sp_co_text.index('</tr>')

while startpos < len(sp_co_text):
    tempList = []
    searchText = sp_co_text[startpos:endpos]
    for line in searchText.splitlines():
        tempList.append(line)
    trList.append(tempList[1:])
    try:
        startpos = sp_co_text.index('<tr>', endpos)
    except ValueError:
        break
    try:
        endpos = sp_co_text.index('</tr>', startpos)
    except ValueError:
        break


#parse headers into list
headerList = ['Symbol', 'Security', 
              'Global_Industry_Classification_Standard', 'GICS Sub Industry', 'Location', 'Date first added', 'CIK']
headerColumn = []

#get table columns of each header (in case wikipedia changes the layout)
for i in headerList:
    for j in range(len(trList[0])-1):
        if i in trList[0][j]: 
            headerColumn.insert(j, j)

    
#build list of lists with each of the 4 data elements in headerList
sp_current_companies = []
#print(len(trList))
#print(headerColumn)
for table_row in trList[1:]:
    #print(table_row)
    column_offset = 0
    row_list = []
    #extract ticker symbols
    ticker_string = etree.HTML(table_row[headerColumn[0]])
    ticker_symbol = ticker_string.xpath('//a/text()')
    row_list.append(ticker_symbol[0])
    #extract security names
    security_string = etree.HTML(table_row[headerColumn[1]])
    security_symbol = security_string.xpath('//a/text()')
    row_list.append(security_symbol[0])
    #extract GICS category
    GICS_string = etree.HTML(table_row[headerColumn[2]])
    GICS_symbol = GICS_string.xpath('//td/text()')
    row_list.append(GICS_symbol[0])
    #exrtract GICS sub category
    GICS_sub_string = etree.HTML(table_row[headerColumn[3]])
    #since 2018/09/22, there was a slight coding error in the webpage where <td>.....</td> for Verizon for sub industry is split into <td>.....  </td>
    #which cause the original code to break down, hence add a shift to column index 
    try:
        GICS_sub_symbol = GICS_sub_string.xpath('//td/text()')
    except Exception as e: 
        print(e)
        GICS_sub_string = etree.HTML(table_row[headerColumn[3]+1])
        GICS_sub_symbol = GICS_sub_string.xpath('//td/text()')
        column_offset = 1 
        
    if GICS_sub_symbol:
        row_list.append(GICS_sub_symbol[0])
        
    
    CIK_string = etree.HTML(table_row[headerColumn[6]+column_offset])
    CIK_symbol = CIK_string.xpath('//td/text()')
    row_list.append(CIK_symbol[0])
    
    date_added_string = etree.HTML(table_row[headerColumn[5]+column_offset])
    date_added_symbol = date_added_string.xpath('//td/text()')
    if (date_added_symbol):
        row_list.append(date_added_symbol[0])
    else:
        row_list.append("")
        
    
    sp_current_companies.append(row_list)    


del trList


"""
Build out historical ticker lists
"""

#loop over HTML text and extract each table row into a list element for 
# parsing later
chgList = []
startpos = sp_chg_text.index('<tr>')
endpos = sp_chg_text.index('</tr>')

while startpos < len(sp_chg_text):
    tempList = []
    searchText = sp_chg_text[startpos:endpos]
    for line in searchText.splitlines():
        tempList.append(line)
    chgList.append(tempList[1:])
    try:
        startpos = sp_chg_text.index('<tr>', endpos)
    except ValueError:
        break
    try:
        endpos = sp_chg_text.index('</tr>', startpos)
    except ValueError:
        break

#first 2 elements are header rows - remove them to make parsing easier
del chgList[:2]



"""
List of S&P 500 Component Company Changes
this code loops over the table of changes to the S&P 500 components list, 
extracts the companies coming in and out, and the date of the change
"""

sp_company_changes_dupdates = []
i = 0 
while i < len(chgList):
    row_list = []
    if chgList[i][0].find('rowspan') > -1: 
        #extract rowspan value
        tgtSt = str(chgList[i][0]).index('rowspan')
        tgtEnd = str(chgList[i][0]).index('">')
        tgtString = str(chgList[i][0])[tgtSt+9:tgtEnd]
        new_increment = int(tgtString)
        #retrieve data points 
        chgDate = etree.HTML(chgList[i][0]).xpath('//td/text()')
        inCo = etree.HTML(chgList[i][1]).xpath('//td/text()')
        outCo = etree.HTML(chgList[i][3]).xpath('//td/text()')
        #loop over additional inner rows and retrieve values
        for j in range(1, new_increment):
            inCo += etree.HTML(chgList[i+j][0]).xpath('//td/text()')
            outCo += etree.HTML(chgList[i+j][2]).xpath('//td/text()')
        row_list.extend((chgDate, inCo, outCo))
        i += new_increment
    elif chgList[i][0].find('rowspan') == -1:
        #retrieve data points 
        chgDate = etree.HTML(chgList[i][0]).xpath('//td/text()')
        inCo = etree.HTML(chgList[i][1]).xpath('//td/text()')
        outCo = etree.HTML(chgList[i][3]).xpath('//td/text()')
        row_list.extend((chgDate, inCo, outCo))
        i += 1
    sp_company_changes_dupdates.append(row_list)
    
del chgList


"""
in order to account for dates with multiple entries, the following
very inefficient code was added, but at least it works
"""

#get list of unique change dates 
sp_company_changes = []
for i in sp_company_changes_dupdates: 
    if [i[0]] not in sp_company_changes:
        sp_company_changes.append([i[0]])

#super inefficient merge of the 2 lists to consolidate all component changes  
# for each date into one entry
for dt in sp_company_changes:    
    dt.append([])
    dt.append([])
    for i in sp_company_changes_dupdates:
        if i[0] == dt[0]:
            dt[1].extend(i[1])
            dt[2].extend(i[2])


#create start and end dates for each company change entry
dateHolder = ''
for idx, val in enumerate(sp_company_changes):
    inputDate = datetime.strptime(val[0][0], '%B %d, %Y')
    startDate = datetime.strftime(inputDate, '%Y-%m-%d')
    if idx == 0: 
        if inputDate > datetime.today(): 
            endDate = datetime.strftime(inputDate, '%Y-%m-%d')
        elif inputDate <= datetime.today():
            endDate = datetime.today().strftime('%Y-%m-%d')
    elif idx > 0:
        endDate = datetime.strftime(dateHolder - timedelta(days=1), '%Y-%m-%d')
    val[0][0] = startDate
    val[0].append(endDate)    
    dateHolder = inputDate


#Wikipedia's ticker "UA-C" is actually "UA" so need to find and replace it
for i in range(len(sp_company_changes)):
    for j in range(len(sp_company_changes[i][1])):
        if sp_company_changes[i][1][j] == 'UA-C': 
            holder = sp_company_changes[i][1][j]
            sp_company_changes[i][1][j] = holder.replace("-C", '')
    for k in range(len(sp_company_changes[i][2])):
        if sp_company_changes[i][2][k] == 'UA-C': 
            holder = sp_company_changes[i][2][k]
            sp_company_changes[i][2][k] = holder.replace("-C", '')
    

#put current tickers into their own list
currentTickers = []
for i in sp_current_companies: 
    currentTickers.append(i[0])


#create list of tickers that was active during each date range, starting
# with the current ticker list
add_list, remove_list, ticker_list = [], [], []
for i in sp_company_changes:
    if i[0][0] > datetime.today().strftime('%Y-%m-%d'):
        i.append([])
    elif i[0][0] <= datetime.today().strftime('%Y-%m-%d') <= i[0][1]:
        i.append([])
        for a in currentTickers: 
            i[3].append(a)
        for j in i[1]: 
            remove_list.append(j)
        for k in i[2]: 
            add_list.append(k)
        for ticker in i[3]: 
            ticker_list.append(ticker)
        i[3].sort()
    elif i[0][0] <= i[0][1] < datetime.today().strftime('%Y-%m-%d'):
        for j in remove_list:
            if j in ticker_list: 
                ticker_list.remove(j)
            else: 
                continue
        for k in add_list:
            ticker_list.append(k)
        i.append([])
        for a in ticker_list:
            i[3].append(a)
        i[3].sort()
        add_list, remove_list, ticker_list = [], [], []
        for j in i[1]: 
            remove_list.append(j)
        for k in i[2]: 
            add_list.append(k)
        for ticker in i[3]:
            ticker_list.append(ticker)
            
            
FTM.SafeSaveData('/Users/Shared/Models/SP500.csv', sp_current_companies)
FTM.SafeSaveData('/Users/Shared/USMarkets/SP500/Constituents/'+ datetime.strftime(datetime.now().date(), "%Y%m%d")+'.csv', sp_current_companies)
ticker_list = [line[1] for line in sp_current_companies]

#print(ticker_list)
ticker_file="/Users/Shared/IBBarData/BarDataTickers.csv"
curTickers = []
if os.path.isfile(ticker_file):
    try:
        with open(ticker_file, 'r') as csvfile:
            tickerlines = [tuple(line) for line in csv.reader(csvfile)]
            tickerlines = [tuple(line) for line in tickerlines if ("_" not in line[0])]
            curTickers = list(set([line[0] for line in tickerlines]))
    except Exception as e:
        print(e)

    cur_ticker_len = len(curTickers)
    curTickers = sorted(list(set(curTickers + ticker_list)))
    new_ticker_len = len(curTickers)
    print(len(curTickers))
    if (new_ticker_len > cur_ticker_len):
        outputlist = []
        for ticker in curTickers:
            outputlist += [(ticker,)]
        FTM.SafeSaveData(ticker_file, outputlist)


#FTM.SafeAddData('/Users/Shared/IBBarData/BarDataTickers.csv', ticker_list)
        


