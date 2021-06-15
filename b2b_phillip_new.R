##for phillip trade
#daily setting
#today; yesterday; day before yesterday; day after tommorrow

library("stringr")
library("readr")
library("pdftools")
library('bizdays')

#set the statement date and format it to contract notes naming convention
#use the existing code although it is probably better to consider using business days instead of using a fixed offset
#in addition, it might be better to consider scanning for downloaded/existing contract notes 
statement_dt <- function() { 
	if(weekdays(today)=="Monday"){
		fnm_y<-format(today-3,format="%Y%m%d");fnm_y
		} 
	else {
		fnm_y<-format(today-2,format="%Y%m%d");fnm_y
		}
	last_t<-paste(str_sub(fnm_y,1,4),str_sub(fnm_y,5,6),str_sub(fnm_y,7,8),sep="_");last_t
	last_t
}

#read the contents from the contract note PDF file
#input : dt format to the specific contract note filename convention
#output: the contents of the contract note as an array of strings  
contract_note_contents <- function(dt) {
	#path<-paste('Z:/DayEnd Report/Phillip Daily Report/Contract note for Margin Account - 2065959',sep="")
	path<-paste('Z:/',sep="")
	file<-paste('Contract note for Margin Account - 2065959-', dt,'.pdf',sep='')
	php_text<-pdf_text(pdf=paste(path,'/',file,sep=''))%>% readr::read_lines()
}


#data cleaning
cleaned_contract_note_contents <- function(php_text) {
	#This code is poentially dangerous because it removes lines containing certain strings
	#The problem is that if an underlying happens to contain the target phrases used herein 
	#It would remove the lines that contain transaction information and cause all kinds of other problems
	
	#remove lines (strings) containing: 
	# STATEMENT (beginning of CN), Important Notes (footnote at each page), Page(top of each page)
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
	tmpx <- which(str_detect(php_text,'CORPORATE ACTION'))
	if (length(tmpx)>0) {
		#remove corp action
		corp_act<-php_text[c(which(str_detect(php_text,'CORPORATE ACTION')):length(php_text))]
		corp_act<-corp_act[-c(1)]
	
		corp_act_a<-corp_act[seq(2,length(corp_act),2)]%>%str_squish()
		corp_dt<-corp_act_a[seq(1,length(corp_act_a),2)]%>%str_sub(1,8)%>%as.Date('%d/%m/%y')

		corp_act_b<-corp_act_a[seq(1,length(corp_act_a),2)]
		corp_act_b%>%str_sub(9)%>%str_squish()%>% str_detect('[A-Za-z]')

		php_text<-php_text[-c(which(str_detect(php_text,'CORPORATE ACTION')):length(php_text))]
	}

	#remove security movements
	tmpx <- which(str_detect(php_text,'SECURITY MOVEMENT'))
	if (length(tmpx)>0) { 
		corp_act2<-php_text[c(which(str_detect(php_text,'SECURITY MOVEMENT')):length(php_text))]
		php_text<-php_text[-c(which(str_detect(php_text,'SECURITY MOVEMENT')):length(php_text))]
	}
	php_text
}

clean_white_space <- function(php_text) {
	trimmed_php <- c() 
	for (line in php_text) {
		trimmed_php <- c(trimmed_php, str_squish(str_trim(line)))
		#tmp <- strsplit(str_squish(str_trim(line)), ' ')
	}
	trimmed_php
}

fix_cn_tickers <- function(php_text) {
	#this function tries to fix the situation when the ticker of a CN underlying is 
	tmp <- which(str_detect(php_text, "^CN "))
	if (length(tmp) >=1) {
		cn_list <-  which(str_detect(php_text, "^CN "))
		remove_indices <- c()
		for (i in 1:length(cn_list)) {
			print(i)
			print(cn_list[i])
			line_idx <- cn_list[i]
			next_line_idx <- line_idx + 1 
			if (next_line_idx < length(php_text)){
				next_line <- php_text[next_line_idx]
				print(next_line)
				print(nchar(next_line))
				if (nchar(next_line) == 6) {
					#if the next line has a length of 6 it is most likely an A-share ticker
					digit_test <- grepl("^[0-9]+$", next_line, perl=T)
					if (digit_test) {
						#next line is of length 6 and contains only digits, so it's likely a ticker for A-shares 
						ticker <- next_line; 
						ticker_idx <- next_line_idx; 
						print('here')

						#process the transaction line 
						tmp <- str_split(php_text[line_idx], ' ')
						tmp <- tmp[[1]]
						print(tmp)

						if (tmp[length(tmp)-3] == 'CNY') { 
							last_co_name <- tmp[length(tmp)-5]
							if (substr(last_co_name, nchar(last_co_name), nchar(last_co_name)) == ",") {
								last_co_name = paste(last_co_name, next_line, sep=" ")
							
								tmp[length(tmp)-5] <- last_co_name
								new_line <- tmp[1]
								for (i in 2:length(tmp)) { 
									new_line <- paste(new_line, tmp[i], sep=" ")
								}  
								php_text[line_idx] <- str_squish(str_trim(new_line))
								remove_indices <- c(remove_indices, ticker_idx)
							}
						}
					}
				}
			}
		}
	}
	php_text <- php_text[-remove_indices]
}



		

php_text <- cleaned_contract_note_contents(contract_note_contents(statement_dt()))
php_text <- clean_white_space(php_text)

print('here')
php_text <- fix_cn_tickers(php_text)

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
