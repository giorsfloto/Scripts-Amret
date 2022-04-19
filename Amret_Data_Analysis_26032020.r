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
##### Load data: 449,408
{
events <-data.table(read.csv(paste0(FoldData,'Event_Sample_20200326.csv'),
                                stringsAsFactors = FALSE))						

setnames(events, "ï..CUSID", "CUSID")
setnames(events, "PAYMENT_DEU_DATE", "PAYMENT_DUE_DATE")	
						
events$CUSID <- as.character(events$CUSID)		
events$EVENT_DATE<-as.Date(as.character(events$EVENT_DATE),format="%Y%m%d")
events$TRNDATE<-as.Date(as.character(events$TRNDATE),format="%Y%m%d")
events$PAYMENT_DUE_DATE<-as.Date(as.character(events$PAYMENT_DUE_DATE),format="%Y%m%d")		

sum(events$EVENT=='R' & events$EVENT_DATE>events$PAYMENT_DUE_DATE) ### 35,508 rows with Event='R' and Event_Date>Payment_Due_Date
sum(events$EVENT_PAID_AMOUNT>events$AMOUNT_DUE) ### 1,506 rows with EVENT_PAID_AMOUNT>AMOUNT_DUE (0.34%). Is AMOUNT_DUE reliable?
											
}

############ December Schedule (25/03)
##### Load data: 171,221
{
schedule <-data.table(read.csv(paste0(FoldData,'LOAN_SCHEDULE_DEC_19.csv'),
                                stringsAsFactors = FALSE))		
								
setnames(schedule, "ï..ACC", "ACC")
schedule$DUEDATE<-as.Date(as.character(schedule$DUEDATE),format="%Y%m%d")
schedule$REPORTDATE<-as.Date(as.character(schedule$REPORTDATE),format="%Y%m%d")		

schedule_events <- merge(schedule,events, by.x="ACC",by.y="CONTRACT_ID",all.x=T) ## 376,650

sum(schedule_events$DUEDATE != schedule_events$PAYMENT_DUE_DATE, na.rm=T) ### 1,531 with DUEDATE(schedule)<>PAYMENT_DUE_DATE (0.4%). Most of them are contracts with 2 instalements during the month

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

sum(!(events$CONTRACT_ID %in% (schedule$ACC))) # 74,161 rows with Contract_ID not in schedule (43.3%)
###### this happens becuase in the first extraction (LOAN_SCHEDULE_DEC_19.csv) all instalments with capital due=0 were excluded
###### with the second schedule extraction (LOAN_SCHEDULE_20200327), no contracts in schedule can't be found in event
schedule2 <-data.table(read.csv(paste0('LOAN_SCHEDULE_20200327.csv'),
                                stringsAsFactors = FALSE))		
								
setnames(schedule2, "ï..Contract_ID", "Contract_ID")
sum(!(events$CONTRACT_ID %in% (schedule2$Contract_ID))) #### 0: OK!!

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

schedule_events3$Flag_OK <- ifelse(schedule_events3$TOTPASTDUE==schedule_events3$Total_Past_Due,0,1)

mytable <- with(schedule_events3, table(Flag_OK))
prop.table(mytable)

contractstobechecked <- subset(schedule_events3, schedule_events3$Flag_OK==1)

write.csv(contractstobechecked, "contractstobechecked.csv", row.names=F)

############ December Loan (25/03)
##### Load data: 329,984

loan <-data.table(read.csv(paste0(FoldData,'Loan_Sample.csv'),
                                stringsAsFactors = FALSE))	
								
length(unique(loan$Contract_ID)) ### 329,984 unique contracts: OK!

setnames(loan, "ï..Contract_ID", "Contract_ID")
setnames(loan, "Account_Type", "Repayment_Account_ID")
setnames(loan, "LOAN_PURPOSE", "Loan_Purpose_Code")

loan$Contract_ID <- as.character(loan$Contract_ID)
loan$Customer_ID <- as.character(loan$Customer_ID)
loan$Loan_Code <- as.character(loan$Loan_Code)
loan$Portfolio_Manager_ID <- as.character(loan$Portfolio_Manager_ID)
loan$Update_Date<-as.Date(as.character(substr(loan$Update_Date,1,10),format="%Y%m%d"))
loan$Disbursement_Date<-as.Date(as.character(loan$Disbursement_Date),format="%Y%m%d")
loan$Maturity_Date<-as.Date(as.character(loan$Maturity_Date),format="%Y%m%d")

schedule_events4 <- merge(schedule_events3,loan, by.x="ACC",by.y="Contract_ID", all.x=T)
sum(is.na(schedule_events4$Repayment_Account_ID)) #### 0 loans without repayment account: OK!!!!!!

table(loan$Loan_Code)
summary(loan$Disbursement_Date)
summary(loan$Maturity_Date)
summary(loan$Disbursement_Amount)
table(loan$Instalments_Freq) # 100% M
summary(loan$Instalments_Number) # Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 0.00   12.00   24.00   28.56   48.00  120.00 
summary(loan$Cycle) #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's  1.000   1.000   2.000   4.073   5.000  27.000       9 
summary(loan$Interest_Rate) #   Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 0.00   17.40   18.00   17.23   18.00   36.00 
table(loan$Payment_Type_Code) #   ""    ANNUITY      BALOON   DECLINING    FLEXIBLE SEMI BALOON  144796        3334         950      164335       15782         787
table(loan$Restructuration_Flag) # NO YES 291156  38828
table(loan$Department) # 159 branches!
table(loan$Portfolio_Manager_ID) # 2,261 PMs
table(loan$Currency) #  KHR    THB    USD  195,784   8,630 125,570 
table(loan$Written_Off_Flag) # N Y 329118    866 
table(loan$Loan_Purpose_Code)
table(loan$Update_Date)

sum(!(events$CONTRACT_ID %in% (loan$Contract_ID)))  # all contracts in events are in loan
sum(!(schedule$ACC %in% (loan$Contract_ID)))  # all contracts in schedule are in loan

############ December Account_Balance (25/03)
##### Load data: 1,991,398

account_balance <-data.table(read.csv(paste0(FoldData,'Account_Balance.csv'),
                                stringsAsFactors = FALSE))	
							
account_balance$Customer_ID <- as.character(account_balance$Customer_ID)
account_balance$Account_ID <- as.character(account_balance$Account_ID)
account_balance$Account_Code <- as.character(account_balance$Account_Code)
account_balance$Update_Date<-as.Date(as.character(account_balance$Update_Date),format="%Y%m%d")

length(unique(paste0(account_balance$Account_ID,account_balance$Update_Date,sep=''))) ### 1,991,398: OK!!

table(account_balance$Account_Type)
table(account_balance$Account_Code)
table(paste0(account_balance$Account_Code,account_balance$Account_Type,sep='-'))
table(account_balance$Currency)
table(account_balance$Update_Date)
summary(account_balance$Update_Date)
								
############ December Customer (25/03)
##### Load data: 274,265

customer <-data.table(read.csv(paste0(FoldData,'Customer_Sample.csv'),
                                stringsAsFactors = FALSE))	
								
setnames(customer, "ï..Customer_ID", "Customer_ID")

length(unique(customer$Customer_ID)) ### 274,265 unique customers: OK!

customer$Customer_Area <- as.character(customer$Customer_Area)

customer$Customer_Area <- as.character(customer$Customer_Area)
customer$Creation_Date<-as.Date(as.character(customer$Creation_Date),format="%Y%m%d")
customer$Update_Date<-as.Date(as.character(substr(customer$Update_Date,1,10),format="%Y%m%d"))

table(customer$Department)
sum(is.na(customer$Department)) ## 0 rows with missing Department
table(customer$Customer_Area)
sum(is.na(customer$Customer_Area)) ## 0 rows with missing Customer_Area
length(unique(customer$Customer_Area)) ## 12,046 unique Customer Areas
summary(customer$Creation_Date)
sum(customer$Creation_Date>as.Date('2019-12-31'))
table(customer$Customer_Class)
table(customer$Customer_Type_Code)
table(customer$Customer_Segment)
table(customer$Update_Date)

sum(!(events$CUSID %in% (customer$Customer_ID))) # 89 rows in events with Customer_ID not in customer
sum(!(account_balance$Customer_ID %in% (customer$Customer_ID))) # 1,034,390 in account not in customer
sum(!(events$CUSID %in% (account_balance$Customer_ID))) # 2 rows in events with Customer_ID not in account_balance

############ Loan Applications (25/03)
##### Load data: 16,963

loan_application <-data.table(read.csv(paste0(FoldData,'Loan_Application_Sample.csv'),
                                stringsAsFactors = FALSE))	
								
setnames(loan_application, "ï..Loan_Application_ID", "Loan_Application_ID")
loan_application$Customer_ID <- as.character(loan_application$Customer_ID)
loan_application$Decision <- as.character(loan_application$Decision)
loan_application$Update_Date<-as.Date(as.character(substr(loan_application$Update_Date,1,10),format="%Y%m%d"))

length(unique(loan_application$Loan_Application_ID)) #### 16,963 LID: OK!

sum(substr(loan_application$Loan_ID,1,2)='LD')  ## 11,057 with Loan_ID
sum(is.na(loan_application$Customer_ID)) ## 0
table(loan_application$Decision) ##  1     3     5    90   4010  1893 11057     2 
table(loan_application[substr(loan_application$Loan_ID,1,2)=='LD',Decision]) ## LID with Loan_ID have Decision='5'
quantile(loan_application$Amount_Requested, c(0,.1,.25,.5,.75,.9,1))
summary(loan_application[loan_application$Currency == 'USD', "Amount_Requested" ]) #  Min.   :  149   1st Qu.: 3000   Median : 6000    Mean   : 8769   3rd Qu.:11000   Max.   :95000 
summary(loan_application[loan_application$Currency == 'KHR', "Amount_Requested" ]) # Min.   :  300   1st Qu.: 3000   Median : 10000000    Mean   : 14561118    3rd Qu.: 20000000    Max.   :160000000 
summary(loan_application[loan_application$Currency == 'THB', "Amount_Requested" ]) # Min.   :  8000     1st Qu.: 72750   Median : 120000    Mean   : 165487   3rd Qu.:210000    Max.   :2040000
summary(loan_application$Term_Requested) ##  Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 3.00   24.00   36.00   39.25   48.00   84.00 
sum(is.na(loan_application$Department)) # 0
table(loan_application$Department)
sum(is.na(loan_application$Portfolio_Manager_ID))
table(loan_application$Portfolio_Manager_ID)
table(loan_application$Currency) # KHR   THB   USD 5838   750 10375 

check <- subset(loan_application,!(loan_application$Loan_ID %in% loan$Contract_ID))
table(check$Loan_ID) ## all Loan_ID in LA are in Loan






