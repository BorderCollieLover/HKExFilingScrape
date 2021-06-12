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
		fnm_y<-format(today-3,format="%Y%m%d");fnm_y
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
