##for phillip trade

#daily setting
#today; yesterday; day before yesterday; day after tommorrow
library("stringr")
library("readr")
library("pdftools")
library('bizdays')

today<-Sys.Date();today
fnm_t<-format(today,format="%Y%m%d");fnm_t
fnm5<-paste(str_sub(fnm_t,7,8),str_sub(fnm_t,5,6),str_sub(fnm_t,1,4),sep="");fnm5

#change today-2 back to today-1 after done debugging -- MT 2021.06.12 
#this is because i am debugging on a saturday 
#but ideally either use business days or scan new and un-processed contract notes file
#last trading day
if(weekdays(today)=="Monday"){
	fnm_y<-format(today-3,format="%Y%m%d");fnm_y
	} else {
		fnm_y<-format(today-2,format="%Y%m%d");fnm_y
		}
last_t<-paste(str_sub(fnm_y,1,4),str_sub(fnm_y,5,6),str_sub(fnm_y,7,8),sep="_");last_t


#reading data from pdf
#path<-paste('Z:/DayEnd Report/Phillip Daily Report/Contract note for Margin Account - 2065959',sep="")
path<-paste('Z:/',sep="")
file<-paste('Contract note for Margin Account - 2065959-',last_t,'.pdf',sep='')
php_text<-pdf_text(pdf=paste(path,'/',file,sep=''))%>%
	readr::read_lines()

print(php_text)
#data cleaning
title_start<-which(str_detect(php_text,'STATEMENT'))
title_end<-which(str_detect(php_text,'STATEMENT'))+5
title_list<-NULL
for (i in 1:length(title_start)){
	title<-seq(title_start[i],title_end[i],1)
	title_list<-c(title_list,title)	
	}
title_list

note_start<-which(str_detect(php_text,'Important Notes'))
note_end<-which(str_detect(php_text,'Important Notes'))+12
note_list<-NULL
for (i in 1:length(note_start)){
	title<-seq(note_start[i],note_end[i],1)
	note_list<-c(note_list,title)	
	}
note_list

page_start<-which(str_detect(php_text,'Page'))
page_end<-which(str_detect(php_text,'Page'))+1
page_list<-NULL
for (i in 1:length(page_start)){
	title<-seq(page_start[i],page_end[i],1)
	page_list<-c(page_list,title)	
	}
page_list

php_text<-php_text[-c(title_list,note_list,page_list)]

#remove contracts; trans;cd;date etc
php_text<-php_text[-c(which(str_detect(php_text,'CONTRACT|TRANS|CD|DATE|Comm|TOTAL|SETTLEMENT')))]

#remove corp action
corp_act<-php_text[c(which(str_detect(php_text,'CORPORATE ACTION')):length(php_text))]
corp_act<-corp_act[-c(1)]

corp_act_a<-corp_act[seq(2,length(corp_act),2)]%>%str_squish()
corp_dt<-corp_act_a[seq(1,length(corp_act_a),2)]%>%str_sub(1,8)%>%as.Date('%d/%m/%y')

corp_act_b<-corp_act_a[seq(1,length(corp_act_a),2)]
corp_act_b%>%str_sub(9)%>%str_squish()%>%
%>%str_detect('[A-Za-z]')

php_text<-php_text[-c(which(str_detect(php_text,'CORPORATE ACTION')):length(php_text))]

#remove security movements
corp_act2<-php_text[c(which(str_detect(php_text,'SECURITY MOVEMENT')):length(php_text))]

php_text<-php_text[-c(which(str_detect(php_text,'SECURITY MOVEMENT')):length(php_text))]


#format trade data
#part1: 
php_text_a<-php_text[seq(1,length(php_text),2)]%>%
	str_split('BUY|SELL')%>%
	unlist()%>%
	matrix(ncol=2,byrow=T)
php_text_aa<-php_text_a[,1]%>%
	str_split(' ')%>%
	unlist()%>%
	matrix(ncol=5,byrow=T)
colnames(php_text_aa)<-c('CD','Trans_date','Due_date','Cont_no','bs')
CD<-php_text_aa[,'CD']

#part2: got stk code; buy_sell
php_text_a[,2]<-str_squish(php_text_a[,2])
php_stk<-NULL
for (i in 1:length(php_text_a[,2])){
	start=1
	end=if_else(CD[i]=='CN',str_locate(php_text_a[,2],'[0-9]')[i,1]+5,str_locate(php_text_a[,2],'[0-9]')[i,1]-1)
	php_stk[i]<-str_sub(php_text_a[i,2],start,end)
	}
php_stk<-php_stk%>%
	str_split(',')%>%
	unlist()%>%
	matrix(ncol=2,byrow=T)%>%
	data.frame()%>%
	rename(stock_name=X1,stock_code=X2)%>%
	mutate(stock_code=str_replace_all(stock_code,' ',''))%>%
	mutate(stock_code=if_else(CD=='CN',paste(stock_code,'CN',sep=':'),paste(stock_code,'US',sep=':')))

bs<-str_extract(php_text[seq(1,length(php_text),2)],'BUY|SELL')

#part3:
php_text_ab<-php_text_a[,2]%>%
	str_sub(str_locate(php_text_a[,2],'[0-9]')[,1],-1L)
php_text_ab[CD=='CN']<-str_sub(php_text_ab[CD=='CN'],7)
php_text_ab<-php_text_ab%>%
	str_squish()%>%
	str_split(' ')%>%
	unlist()%>%
	matrix(ncol=5,byrow=T)
colnames(php_text_ab)<-c('Quantity','Trd_ccy','Price','Gross_amt','Net_amt')

#part 4:
php_text_b<-php_text[seq(2,length(php_text),2)]%>%
	str_squish()%>%
	str_split(' ')%>%
	unlist()%>%
	matrix(ncol=5,byrow=T)
colnames(php_text_b)<-c('comm','other_fee','Foreign_fee','Tax','Ex_rate')


#combine trade data together
trade_php<-cbind(php_text_aa[,1:4],bs,php_stk,php_text_ab,php_text_b)%>%data.frame()
col<-c('cd','trans_date','due_date','cont_no','buy_sell','security_name','stock_code','quantity','currency',
	'price','gross_amount','net_amount','comm','other_fee','foreign_fee','tax','ex_rate')

colnames(trade_php)<-col


#trade data format
#date
trade_php$trans_date<-as.Date(trade_php$trans_date,'%d/%m/%y')
trade_php$due_date<-as.Date(trade_php$due_date,'%d/%m/%y')

for (i in c(8,10:15)){
	trade_php[,i]<-trade_php[,i]%>%
	str_replace(',','')%>%
	as.numeric()
	}
trade_php%>%head()

#buy: setl_amount*-1; sel setl_amount+1;qty*-1
trade_php<-trade_php%>%
	mutate(qty2=if_else(buy_sell=='SELL',-1*quantity,quantity),
		amt2=if_else(buy_sell=='BUY',-1*net_amount,net_amount))


fout_php<-paste("Z:/Daily Operation/6_Daily Order/",fnm_t,"/phillip_trade_",fnm_y,".csv",sep="")
write.csv(trade_php, file = fout_php,row.names=F)

#summary
trade_php%>%group_by(stock_code)%>%
	summarise(turnover=sum(gross_amount),net_qty=sum(qty2),net_position=sum(amt2))%>%
	data.frame()

trade_php%>%group_by(stock_code)%>%
	summarise(turnover=sum(gross_amount),net_qty=sum(qty2),net_position=sum(amt2))%>%
	data.frame()%>%
	summarise(turnover=sum(turnover),net_amount=sum(net_position))


#read in history trade and put in tableau
tout_php<-paste('Z:/Tableau/hist_php_trade.csv')
php_hist<-read.csv(tout_php,header=T)
php_hist%>%head()
php_hist%>%tail()

php_hist<-php_hist%>%filter(str_detect(trans_date,'-'))
php_hist$trans_date<-as.Date(php_hist$trans_date)
php_hist$due_date<-as.Date(php_hist$due_date)

hist_php<-rbind(php_hist,trade_php)
hist_php<-hist_php[order(hist_php[,2],hist_php[,6]),]
hist_php<-hist_php[!duplicated(hist_php),]
write.csv(hist_php,tout_php,row.names=F)

holding<-hist_php%>%group_by(cd,stock_code)%>%
	summarise(qty=sum(qty2),net_amt=sum(amt2))%>%
	data.frame()%>%
	arrange(cd,qty,stock_code)%>%
	mutate(avg_price=net_amt/qty)

#corp_act

fout_php<-paste("Z:/Daily Operation/6_Daily Order/",fnm_t,"/hist_php_trade",".xlsx",sep="")
data_php <- list("trade" = hist_php,'position'=holding)
write.xlsx(data_php, file = fout_php)

#here copy php history unmatch info
file.copy("C:/Rdata/b2b_unmatch_info.txt",
	paste("Z:/Daily Operation/6_Daily Order/",fnm_t,"/b2b_unmatch_info",".txt",sep=""))

#here output php cn holding stocks
hist_php<-php_hist
hist_php%>%head()
hist_php%>%group_by(cd,stock_code)%>%
	summarise(qty=sum(qty2),net_amt=sum(amt2))%>%
	data.frame()%>%
	filter(qty!=0,cd=='CN')%>%
	transmute(stock_code)%>%
	mutate(stock_code=str_replace(stock_code,':CN','.SS'))%>%
	data.frame()%>%
	write.table(paste("Z:/Daily Operation/6_Daily Order/",fnm_t,"/hist_php_position",fnm_y,".txt",sep=""),
	row.names=F,fileEncoding="GBK")

#read in php hist trade from bos
#tin_php<-paste('Z:/Tableau/hist_php_trade_bos.csv')
#php_hist_cl<-read.csv(tin_php,header=T)%>%data.frame()

#php_hist_cl<-php_hist_cl%>%
#	transmute(cd='',trans_date=tran_date,due_date=value_date,cont_no=tran_id,
#			buy_sell=if_else(bs_flag=='B','BUY','SELL'),
#			security_name=product_name,stock_code=product_id,
#			quantity=qty,currency=quote_ccy,price=avg_price,gross_amount=gross_amt,
#			net_amount=net_amt1,comm=commission,other_fee=0,foreign_fee=charge,
#			tax=0,ex_rate=ex_rate)%>%
#	mutate(qty2=if_else(buy_sell=='SELL',-1*quantity,quantity),
#		 amt2=if_else(buy_sell=='BUY',-1*net_amount,net_amount))%>%
#	mutate(trans_date=as.Date(trans_date,'%Y-%m-%d'),
#		due_date=as.Date(due_date,'%Y-%m-%d'))%>%
#	filter(trans_date<'2020-08-10')
#php_hist_cl$cd<-php_hist_cl$stock_code%>%str_split(':')%>%
#	unlist()%>%matrix(ncol=2,byrow=T)%>%data.frame()%>%
#	transmute(cd=X2)%>%unlist()
#write.csv(php_hist_cl,'C:/Users/sma/Desktop/tableau/hist_php_trade_bos.csv',row.names=F)
#tout_php<-paste('Z:/Tableau/hist_php_trade.csv')
#write.csv(hist_php_a,tout_php,row.names=F)

#merge with broker trade from 2020-08-10
#hist_php_a<-rbind(php_hist_cl,hist_php)%>%
#	arrange(trans_date,stock_code,buy_sell)%>%
#	unique()