### Libraries ###
require(data.table)
require(tidyverse)
require(plyr)
require(bigrquery)


UploadBigQuery <- function(data, bqtable, bqdataset = "Test", bqproject = "867965574458", overwrite = FALSE) {
  # The default project is Advans
  message(paste("Upload data to table:", bqtable, "Data set:", bqdataset,  "overwrite =", overwrite))
  if (overwrite == TRUE) {
    message(paste("Deleting table", bqtable, "in", bqdataset))
    tryCatch(delete_table(bqproject, bqdataset, bqtable), 
             error = function(e) {message("Cannot delete table", e)})
  }
  job <- insert_upload_job(bqproject, bqdataset, bqtable, data)
  wait_for(job)
}

options("httr_oob_default" = TRUE)
project <- "867965574458" # put your project ID here


### Parameters ###
FoldData   <-'C:/Users/Giorgio/Documents/Rubyx/Advans/Amret/Data/'
FoldResult <-'C:/Users/Giorgio/Documents/Rubyx/Advans/Amret/Data/Analysis'

setwd("C:/Users/Giorgio/Documents/Rubyx/Advans/Amret/Data")

############ December Events (25/03)
##### Load data: 415,722
{
events <-data.table(read.csv(paste0(FoldData,'Event_Sample_NEW.csv'),
                                stringsAsFactors = FALSE))						

setnames(events, "ï..CUSID", "CUSID")
setnames(events, "PAYMENT_DEU_DATE", "PAYMENT_DUE_DATE")	
						
events$CUSID <- as.character(events$CUSID)		
events$EVENT_DATE<-as.Date(as.character(events$EVENT_DATE),format="%Y%m%d")
events$TRNDATE<-as.Date(as.character(events$TRNDATE),format="%Y%m%d")
events$PAYMENT_DUE_DATE<-as.Date(as.character(events$PAYMENT_DUE_DATE),format="%Y%m%d")					
								
}

############ December Schedule (25/03)
##### Load data: 171,221
{
schedule <-data.table(read.csv(paste0(FoldData,'LOAN_SCHEDULE_DEC_19.csv'),
                                stringsAsFactors = FALSE))		
								
setnames(schedule, "ï..ACC", "ACC")
schedule$DUEDATE<-as.Date(as.character(schedule$DUEDATE),format="%Y%m%d")
schedule$REPORTDATE<-as.Date(as.character(schedule$REPORTDATE),format="%Y%m%d")		

schedule_events <- merge(schedule,events, by.x="ACC",by.y="CONTRACT_ID",all.x=T)

schedule_events$PRIPAID <- ifelse(schedule_events$PAYMENT_TYPE=="Principal",schedule_events$EVENT_PAID_AMOUNT,0)
schedule_events$INTPAID <- ifelse(schedule_events$PAYMENT_TYPE=="Interest",schedule_events$EVENT_PAID_AMOUNT,0)

schedule_events <- subset(schedule_events, schedule_events$EVENT_DATE>=schedule_events$DUEDATE)

schedule_events <- schedule_events[order(ACC,DUEDATE,EVENT_DATE,PAYMENT_TYPE)]

schedule_events2 <- data.table(ddply(schedule_events, .(ACC, DUEDATE, PRIAMT, INTAMT), summarize, 
                                               PRIPAID = sum(PRIPAID),
											   INTPAID = sum(INTPAID),
											   LASTPAYMENTDATE = max(EVENT_DATE)
))

schedule_events2$flag_principal <- ifelse(schedule_events2$PRIPAID==schedule_events2$PRIAMT,0,
										ifelse(schedule_events2$PRIPAID<schedule_events2$PRIAMT,-1,
										  ifelse(schedule_events2$PRIPAID>schedule_events2$PRIAMT,1,2)))

mytable <- with(schedule_events2, table(flag_principal))
prop.table(mytable)
										  
schedule_events2$flag_interest <- ifelse(schedule_events2$INTPAID==schedule_events2$INTAMT,0,
										ifelse(schedule_events2$INTPAID<schedule_events2$INTAMT,-1,
										  ifelse(schedule_events2$INTPAID>schedule_events2$INTAMT,1,2)))
										  
mytable <- with(schedule_events2, table(flag_interest))
prop.table(mytable)

schedule_events2$TOTAMT <- schedule_events2$PRIAMT+schedule_events2$INTAMT
schedule_events2$TOTPAID <- schedule_events2$PRIPAID+schedule_events2$INTPAID	

schedule_events2$flag_total <- ifelse(schedule_events2$TOTPAID==schedule_events2$TOTAMT,0,
										ifelse(schedule_events2$TOTPAID<schedule_events2$TOTAMT,-1,
										  ifelse(schedule_events2$TOTPAID>schedule_events2$TOTAMT,1,2)))	

mytable <- with(schedule_events2, table(flag_total))

prop.table(mytable)

schedule_events2$TOTPASTDUE <- schedule_events2$TOTAMT-schedule_events2$TOTPAID
schedule_events2$PRIPASTDUE <- schedule_events2$PRIAMT-schedule_events2$PRIPAID
schedule_events2$INTPASTDUE <- schedule_events2$INTAMT-schedule_events2$INTPAID

schedule_events2$INTPASTDUE <- round(schedule_events2$INTPASTDUE, digits=2)
}
############ December Loan Balance (25/03)
##### Load data: 415,433

loan_balance <-data.table(read.csv(paste0(FoldData,'Loan_Balance_Sample.csv'),
                                stringsAsFactors = FALSE))		

setnames(loan_balance, "ï..Customer_ID", "Customer_ID")		

loan_balance$Update_Date<-as.Date(as.character(loan_balance$Update_Date),format="%Y%m%d")
loan_balance$Date_Due<-as.Date(as.character(loan_balance$Date_Due),format="%Y%m%d")		

lastdate <- ddply(loan_balance,~Contract_ID,summarise,Last_Date=max(Update_Date))

loan_balance2 <- subset(loan_balance, select=c("Contract_ID","Update_Date","Principal_Past_Due","Interests_Past_Due"))

lastbalance <- merge(lastdate, loan_balance2, by.x=c("Contract_ID","Last_Date"),by.y=c("Contract_ID","Update_Date"),all.x=T)


check <- data.table(ddply(last_balance, .(Contract_ID), summarize, 
                                               count = length(Contract_ID)
))
						
					
schedule_events3 <- merge(schedule_events2,lastbalance, by.x="ACC", by.y="Contract_ID", all.x=T)

schedule_events3$Principal_Past_Due <- ifelse(is.na(schedule_events3$Principal_Past_Due),0,schedule_events3$Principal_Past_Due)
schedule_events3$Interests_Past_Due <- ifelse(is.na(schedule_events3$Interests_Past_Due),0,schedule_events3$Interests_Past_Due)

schedule_events3$Total_Past_Due <- schedule_events3$Principal_Past_Due+schedule_events3$Interests_Past_Due
schedule_events3$TOTPASTDUE <- round(schedule_events3$TOTPASTDUE, digits=2)

schedule_events3$Flag_OK <- ifelse(schedule_events3$TOTPASTDUE==schedule_events3$Total_PPast_Due,0,1)

mytable <- with(schedule_events3, table(Flag_OK))
prop.table(mytable)

contractstobechecked <- subset(schedule_events3, schedule_events3$Flag_OK==1)

write.csv(contractstobechecked, "contractstobechecked.csv", row.names=F)