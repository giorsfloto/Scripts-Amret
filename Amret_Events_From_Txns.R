### Libraries ###
require(data.table)
require(tidyverse)
require(plyr)
require(bigrquery)
require(readxl)
require(lubridate)
library(googleCloudStorageR)  
setwd("/home/Amret")
options(scipen = 999) # to prevent scientific notation

### functions used
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

# This function converts string type like "yyyymmdd" and "yymmddHHMM" to date and time format
T24.Transform.ConvertToDate <- function(s) {
  sSample <- s[!is.na(s)][1]
  
  if (nchar(sSample) == 8) {
    s <- as.Date(s, "%Y%m%d")
  } else if (nchar(sSample) == 10 && substr(sSample, 1, 1) != "9") {
    s <- as.POSIXct(strptime(paste0("20", s), "%Y%m%d%H%M", "UTC"))
  } else if (nchar(sSample) == 7 && as.integer(substr(sSample, 5, 7)) <= 366) {
    # For format like yyyyDDD, 2015002 => 2015-1-1
    firstDayYear <- as.Date(paste0(substr(s, 1, 4), "-1-1"))
    s <- firstDayYear + as.integer(substr(s, 5, 7)) - 1
  }
  
  s
}

convertToNumeric <- function(x) {
  r <- as.numeric(x)
  r[is.na(r)] <- 0
  r
}

### Parameters ###
FoldData   <-'/home/Amret/Data/'
FoldResult <-'/home/Amret/Data/Analysis'
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/bigquery",
                                        "https://www.googleapis.com/auth/cloud-platform",
                                        "https://www.googleapis.com/auth/devstorage.full_control",
                                        "https://www.googleapis.com/auth/gmail.compose"))
# the key should be uploaded in directory using upload button of Rstudio file explorer
googleAuthR::gar_auth_service(json_file = "amret-mf-kh-f06028117aef.json",
                              scope = getOption("googleAuthR.scopes.selected"))
gcs_global_bucket("amret_dev")
options("httr_oob_default" = TRUE)
project <- "248422820269" # put your project ID here
bq_auth(path = "amret-mf-kh-f06028117aef.json")  ### json to allow bigrquery to query project Grooming

sch1 <-'amret_kh_sch_20201006113826'
sch2 <-'amret_kh_sch_20201006114146'
sch3 <-'amret_kh_sch_20201006114216'
txn <-'amret_kh_txn_20200831090023'
loa <-'amret_kh_loa_20201006115328'
lba <-'amret_kh_lba_20201006133030' 


############ Schedules
##### Load data: 8,254,025 rows (7,353,803+455,495+444,727)
{
gcs_get_object(paste0(sch1,'.csv'), saveToDisk = paste0(FoldData,sch1,'.csv'), overwrite=TRUE)
schedules1 <-data.table(read.csv(paste0(FoldData,sch1,'.csv'),sep=';',
                                   stringsAsFactors = FALSE))
  
gcs_get_object(paste0(sch2,'.csv'), saveToDisk = paste0(FoldData,sch2,'.csv'), overwrite=TRUE)
schedules2 <-data.table(read.csv(paste0(FoldData,sch2,'.csv'),sep=';',
                                   stringsAsFactors = FALSE))
  
gcs_get_object(paste0(sch3,'.csv'), saveToDisk = paste0(FoldData,sch3,'.csv'), overwrite=TRUE)
schedules3 <-data.table(read.csv(paste0(FoldData,sch3,'.csv'),sep=';',
                                   stringsAsFactors = FALSE))
  
  ### 8,254,025 records
schedules <- rbind(schedules1,schedules2,schedules3)
  
schedules$Customer_ID <- as.character(schedules$Customer_ID)
schedules$Payment_Due_Date <- as.Date(schedules$Payment_Due_Date)
schedules$Schedule_Update_Date <- as.Date(schedules$Schedule_Update_Date)
}
##### Descriptive Analysis
{
  ## Categorical variables
  table(schedules$Department_ID)
  sum(is.na(schedules$Department_ID))        ### 0
  sum(is.na(schedules$Customer_ID))          ### 0
  sum(is.na(schedules$Contract_ID))          ### 0
  table(schedules$Contract_Type)             ### 100% LOAN
  table(schedules$Payment_Frequency)         ### 100% RECURRING
  table(schedules$Frequency)                 ### 100% M
  table(schedules$Schedule_Currency_Code)    ### KHR THB USD 2654272   282316 5317437  
  mytable <- table(schedules$Schedule_Currency_Code)
  prop.table(mytable)                        ### KHR THB USD 32.2% 3.4% 64.4%
  table(schedules$Instalment_Status)         ### 
  
  
  ## Numerical/Date variables
  sum(schedules$Total_Due_Amount==0)  ## 1557 rows with Total_Due_Amount 0
  sum(schedules$Capital_Due_Amount==0)  ## 866714 rows with Capital Amount Due 0
  sum(schedules$Interest_Due_Amount==0)  ## 44745 rows with Interest Amount Due 0
  sum(round((schedules$Capital_Due_Amount+schedules$Interest_Due_Amount),3)
      !=round(schedules$Total_Due_Amount,3)) ### ok we have to round to 3 digits, but Captil+Interest=Total Amounts
  summary(schedules$Payment_Due_Date) # min 2013-05-06, max 2029-11-01
  summary(schedules$Schedule_Update_Date) # 
  
  ## Check formats
  sum(grepl('^[L][D][0-9]{10}$',  schedules$Contract_ID))/length(schedules$Contract_ID) ### 100% Contract_IDs are LD0123456789
  sum(grepl('^[0-9]{1,8}$',  schedules$Customer_ID))/length(schedules$Contract_ID)      ### 100% with up to 8 digits
  sum(grepl('^[A-Z]{3}$',  schedules$Department_ID))/length(schedules$Contract_ID)      ### 100% with 3 characters
  sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  schedules$Total_Due_Amount))/length(schedules$Contract_ID) ### 57.1% some are integers
  sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  schedules$Capital_Due_Amount))/length(schedules$Contract_ID) ### 8.0%
  sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  schedules$Interest_Due_Amount))/length(schedules$Contract_ID) ###60.7%
  #length(unique(paste0(schedules$Contract_ID,schedules$Instalment_Number,sep=''))) # 340,223 unique contracts/instalments: OK!
}

## OK: Contract_ID/Payment_Due_Date/Schedule_Update_Date are unique
length(unique(paste0(schedules$Contract_ID,schedules$Payment_Due_Date,schedules$Schedule_Update_Date)))/length(schedules$Contract_ID) 

## split payments by component (Capital and Interest)
capital_schedules <- subset(schedules, select=c("Customer_ID","Contract_ID","Payment_Due_Date",
                           "Schedule_Currency_Code","Capital_Due_Amount"))

## 7,387,311
capital_schedules <- subset(capital_schedules,capital_schedules$Capital_Due_Amount>0)
capital_schedules$Type <-"Capital"
setnames(capital_schedules,"Capital_Due_Amount","Due_Amount")

interest_schedules <- subset(schedules, select=c("Customer_ID","Contract_ID","Payment_Due_Date",
                                                "Schedule_Currency_Code","Interest_Due_Amount"))

### 8,209,280
interest_schedules <- subset(interest_schedules,interest_schedules$Interest_Due_Amount>0)
interest_schedules$Type <-"Interest"
setnames(interest_schedules,"Interest_Due_Amount","Due_Amount")

## 15,596,591
schedules <- rbind(capital_schedules, interest_schedules)
table(schedules$Type)

## clean VM
rm(schedules1,schedules2,schedules3)
file.remove('Data/amret_kh_sch_20201006113826.csv')
file.remove('Data/amret_kh_sch_20201006114146.csv')
file.remove('Data/amret_kh_sch_20201006114216.csv')

############ Loans
##### Load data: 211,198
gcs_get_object(paste0(loa,'.csv'), saveToDisk = paste0(FoldData,loa,'.csv'), overwrite=TRUE)
loans <-data.table(read.csv(paste0(FoldData,loa,'.csv'),sep=';',
                            stringsAsFactors = FALSE))
loans$Loan_ID <- as.character(loans$Loan_ID)	
loans$Customer_ID <- as.character(loans$Customer_ID)
loans$Inputter_ID <- as.character(loans$Inputter_ID)
loans$Authoriser_ID <- as.character(loans$Authoriser_ID)
loans$PortfolioManager_ID <- as.character(loans$PortfolioManager_ID)
loans$Loan_Product_Code <- as.character(loans$Loan_Product_Code)
loans$Loan_Purpose_Code <- as.character(loans$Loan_Purpose_Code)
loans$Loan_Process_Code <- as.character(loans$Loan_Process_Code)
loans$Loan_Update_Date <- as.Date(loans$Loan_Update_Date)
loans$Loan_Start_Date <- as.Date(loans$Loan_Start_Date)
loans$Loan_Maturity_Date <- as.Date(loans$Loan_Maturity_Date)
loans$Loan_End_Date <- as.Date(loans$Loan_End_Date)
loans$Loan_Disbursement_Date <- as.Date(loans$Loan_Disbursement_Date)

length(unique(loans$Loan_ID))/length(loans$Loan_ID)  ### 1: IDs are unique

### loans disbursed since 1/6/2020: 25,593 loans
loans <- subset(loans, loans$Loan_Disbursement_Date>=as.Date('2020-06-01'))

### schedules from loans disbursed since 1/6/2020: 806,519 obs
recent_schedules <- subset(schedules, schedules$Contract_ID %in% (loans$Loan_ID))

### separate recent schedules with only one instalment
### we assume they are early terminations that we will add at the end to event
schedules_count <- data.table(ddply(recent_schedules, .(Contract_ID, Schedule_Update_Date), summarize, 
                                Instalments = length(Contract_ID)
)) 

recent_schedules <- merge(recent_schedules,schedules_count, all.x=T)

early_terminations <- subset(recent_schedules, recent_schedules$Instalments==1)
recent_schedules <- subset(recent_schedules, recent_schedules$Instalments>1)

### calculate Version of instalments

recent_schedules$Key <- paste0(recent_schedules$Contract_ID,
                               as.character(year(recent_schedules$Payment_Due_Date)),
                               as.character(month(recent_schedules$Payment_Due_Date)))

recent_schedules <- recent_schedules[order(recent_schedules$Key,
                                                  -recent_schedules$Schedule_Update_Date)]

recent_schedules[, Counter := cumsum(!(Schedule_Update_Date==Schedule_Update_Date[1])), by = "Key"]

recent_schedules$Key <- NULL
last_recent_schedules <- subset(recent_schedules, recent_schedules$Counter==0)
last_recent_schedules$Counter <- NULL

last_recent_schedules <- rbind(last_recent_schedules,early_terminations)

#recent_schedules$Key <- paste0(recent_schedules$Contract_ID,recent_schedules$Payment_Due_Date)
#recent_schedules <- recent_schedules[order(recent_schedules$Key,
#                                          -recent_schedules$Schedule_Update_Date)]
#recent_schedules$Row <- 1
#recent_schedules$Version <- unlist(tapply(recent_schedules$Row, recent_schedules$Key, cumsum)) ### 
#recent_schedules$Row <- NULL       
#last_recent_schedules <- subset(recent_schedules, recent_schedules$Version==1)

last_recent_schedules$Total_Due_Amount <- convertToNumeric(last_recent_schedules$Total_Due_Amount)
last_recent_schedules$Capital_Due_Amount <- convertToNumeric(last_recent_schedules$Capital_Due_Amount)
last_recent_schedules$Interest_Due_Amount <- convertToNumeric(last_recent_schedules$Interest_Due_Amount)


clean_schedules <- subset(last_recent_schedules, select=c("Contract_ID","Payment_Due_Date","Schedule_Currency_Code",
                                                          "Total_Due_Amount","Capital_Due_Amount","Interest_Due_Amount"))
setnames(clean_schedules, "Contract_ID", "Loan_ID")
clean_schedules$Date <- clean_schedules$Payment_Due_Date  ### Date field to merge with events
############ Transactions
##### Load data: 5,119,470 obs 
gcs_get_object(paste0(txn,'.csv'), saveToDisk = paste0(FoldData,txn,'.csv'), overwrite=TRUE)
transactions <-data.table(read.csv(paste0(FoldData,txn,'.csv'),sep=';',
                                   stringsAsFactors = FALSE))

### subset of transactions that are loan payments: 1,325,964 obs
loan_transactions <- subset(transactions, 
                            (transactions$TRNCODE=='402' & substr(transactions$ACC,1,2)=='LD')
                            | (transactions$TRNCODE %in% c('420','408','423','434','750','751','402','752')
                               & (transactions$PRIAMT>0 | transactions$INTAMT>0 | transactions$PENAMT>0))
                            |(transactions$TRNCODE=='430' & substr(transactions$TRNID,1,1)=='1'))

loan_transactions$TRNDATE <- T24.Transform.ConvertToDate(as.character(loan_transactions$TRNDATE))
loan_transactions$VALUEDATE <- T24.Transform.ConvertToDate(as.character(loan_transactions$VALUEDATE))

loan_payments <- subset(loan_transactions, loan_transactions$TRNCODE %in%
                          c('423','420','434','750','751','752'))

loan_payments$Loan_ID <- ifelse(substr(loan_payments$ACC,1,2)=='PD', 
                                substr(loan_payments$ACC,3,14), substr(loan_payments$ACC,1,12))

loan_payments$TRNAMT <- convertToNumeric(loan_payments$TRNAMT)
loan_payments$PRIAMT <- convertToNumeric(loan_payments$PRIAMT)
loan_payments$INTAMT <- convertToNumeric(loan_payments$INTAMT)
loan_payments$PENAMT <- convertToNumeric(loan_payments$PENAMT)

### 762,239 obs.
loan_events <- data.table(ddply(loan_payments, .(Loan_ID, TRNDATE), summarize, 
                                Total_Paid_Amount = sum(TRNAMT),
                                Capital_Paid_Amount = sum(PRIAMT),
                                Interest_Paid_Amount = sum(INTAMT),
                                Penalty_Paid_Amount = sum(PENAMT)
)) 

setnames(loan_events,"TRNDATE","Date")
loan_events$Event_Date <- loan_events$Date

clean_loan_events <- subset(loan_events, loan_events$Loan_ID %in% (loans$Loan_ID))

sch_eve <- merge(clean_schedules, clean_loan_events, all=T)


sch_eve2 <- subset(sch_eve, select=c("Loan_ID","Date","Payment_Due_Date", "Event_Date",
                                     "Total_Due_Amount",
                                     "Capital_Due_Amount","Interest_Due_Amount",
                                     "Total_Paid_Amount","Capital_Paid_Amount",
                                     "Interest_Paid_Amount","Penalty_Paid_Amount"))

sch_eve2 <- subset(sch_eve2, sch_eve2$Date>='2020-06-01' & sch_eve2$Date<='2020-08-31')

sch_eve2$Total_Due_Amount <- convertToNumeric(sch_eve2$Total_Due_Amount)
sch_eve2$Capital_Due_Amount <- convertToNumeric(sch_eve2$Capital_Due_Amount)
sch_eve2$Interest_Due_Amount <- convertToNumeric(sch_eve2$Interest_Due_Amount)
sch_eve2$Total_Paid_Amount <- convertToNumeric(sch_eve2$Total_Paid_Amount)
sch_eve2$Capital_Paid_Amount <- convertToNumeric(sch_eve2$Capital_Paid_Amount)
sch_eve2$Interest_Paid_Amount <- convertToNumeric(sch_eve2$Interest_Paid_Amount)
sch_eve2$Penalty_Paid_Amount <- convertToNumeric(sch_eve2$Penalty_Paid_Amount)

sch_eve2 <- sch_eve2[order(sch_eve2$Loan_ID,sch_eve2$Date)]
sch_eve2[, Total_Due_Amount_Cum := cumsum(Total_Due_Amount), by="Loan_ID"]
sch_eve2[, Capital_Due_Amount_Cum := cumsum(Capital_Due_Amount), by="Loan_ID"]
sch_eve2[, Interest_Due_Amount_Cum := cumsum(Interest_Due_Amount), by="Loan_ID"]
sch_eve2[, Total_Paid_Amount_Cum := cumsum(Total_Paid_Amount), by="Loan_ID"]
sch_eve2[, Capital_Paid_Amount_Cum := cumsum(Capital_Paid_Amount), by="Loan_ID"]
sch_eve2[, Interest_Paid_Amount_Cum := cumsum(Interest_Paid_Amount), by="Loan_ID"]
sch_eve2[, Penalty_Paid_Amount_Cum := cumsum(Penalty_Paid_Amount), by="Loan_ID"]

sch_eve2 <- subset(sch_eve2, select=c("Loan_ID","Date","Payment_Due_Date","Event_Date",
                                      "Total_Paid_Amount","Capital_Paid_Amount",
                                      "Interest_Paid_Amount","Total_Due_Amount_Cum",
                                      "Capital_Due_Amount_Cum","Interest_Due_Amount_Cum",
                                      "Total_Paid_Amount_Cum","Capital_Paid_Amount_Cum",
                                      "Interest_Paid_Amount_Cum","Penalty_Paid_Amount_Cum"))

sch_eve2 <- sch_eve2[order(sch_eve2$Loan_ID, -sch_eve2$Date)]

sch_eve2$Row <- 1 

sch_eve2$Version <- unlist(tapply(sch_eve2$Row, sch_eve2$Loan_ID, cumsum)) ### counter by Contract increasing with Instalment Number 
sch_eve2$Row <- NULL       

### last version of schedule_event to be compared with overdue from loan balances at 31/8/2020
sch_eve3 <- subset(sch_eve2, sch_eve2$Version==1)
sch_eve3$Total_Overdue_Amount <- sch_eve3$Total_Due_Amount_Cum-sch_eve3$Total_Paid_Amount_Cum
sch_eve3$Capital_Overdue_Amount <- sch_eve3$Capital_Due_Amount_Cum-sch_eve3$Capital_Paid_Amount_Cum
sch_eve3$Interest_Overdue_Amount <- sch_eve3$Interest_Due_Amount_Cum-sch_eve3$Interest_Paid_Amount_Cum

############ Loan Balances at 31/08/2020
##### Load data: 211,198 obs 
gcs_get_object(paste0(lba,'.csv'), saveToDisk = paste0(FoldData,lba,'.csv'), overwrite=TRUE)
loan_balances <-data.table(read.csv(paste0(FoldData,lba,'.csv'),sep=';',
                                   stringsAsFactors = FALSE))

loan_balances$Loan_ID <- as.character(loan_balances$Loan_ID)
loan_balances$Customer_ID <- as.character(loan_balances$Customer_ID)
loan_balances$Loan_Product_Code <- as.character(loan_balances$Loan_Product_Code)
loan_balances$LoanBalance_Currency_Code <- as.character(loan_balances$LoanBalance_Currency_Code)
loan_balances$Event_Code <- as.character(loan_balances$Event_Code)
loan_balances$Loan_Balance_Date <- as.Date(loan_balances$Loan_Balance_Date)

length(unique(loan_balances$Loan_ID))/length(loan_balances$Loan_ID)  ## 1:unique Loan_IDs
length(unique(paste0(loan_balances$Loan_ID,loan_balances$Loan_Balance_Date)))/length(loan_balances$Loan_ID) ## 1

table(loan_balances$Loan_Balance_Date)  ### 2020-08-31

clean_loan_balances <- subset(loan_balances, select=c("Loan_ID", "Principal_Overdue_Amount",
                                                      "Interest_Overdue_Amount"))

setnames(clean_loan_balances, old=c("Principal_Overdue_Amount","Interest_Overdue_Amount"),
        new=c("Principal_Overdue_Amount_31082020","Interest_Overdue_Amount_31082020"))

sch_eve3 <- merge(sch_eve3,clean_loan_balances, all.x=T)
sch_eve3$Capital_Mismatch <- sch_eve3$Capital_Overdue_Amount-sch_eve3$Principal_Overdue_Amount_31082020
sch_eve3$Interest_Mismatch <- sch_eve3$Interest_Overdue_Amount-sch_eve3$Interest_Overdue_Amount_31082020

### mismatch on Principal Overdue: 0.12% (13)
sum(abs(sch_eve3$Capital_Mismatch)>0.001)
sum(abs(sch_eve3$Capital_Mismatch)>0.001)/length(sch_eve3$Loan_ID)*100  
### mismatch on Interest Overdue: 0.18% (20)
sum(abs(sch_eve3$Interest_Mismatch)>0.001)
sum(abs(sch_eve3$Interest_Mismatch)>0.001)/length(sch_eve3$Loan_ID)*100 

check <- subset(sch_eve3, abs(sch_eve3$Capital_Mismatch)>0.001 
                | abs(sch_eve3$Interest_Mismatch)>0.001)

length(check$Loan_ID)/length(sch_eve3$Loan_ID)*100

loan_status <- subset(loans, select=c("Loan_ID","Loan_Status_Name"))
check <- merge(check,loan_status,all.x=T)
table(check$Loan_Status_Name)

#### creation of Previous and Next Schedule Dates
sch_eve2 <- merge(sch_eve2,clean_loan_balances, all.x=T)
sch_eve2$Capital_Mismatch <- sch_eve2$Capital_Overdue_Amount-sch_eve2$Principal_Overdue_Amount_31082020
sch_eve2$Interest_Mismatch <- sch_eve2$Interest_Overdue_Amount-sch_eve2$Interest_Overdue_Amount_31082020

sch_eve2$Total_Overdue_Amount <- round(sch_eve2$Total_Due_Amount_Cum-sch_eve2$Capital_Paid_Amount_Cum-sch_eve2$Interest_Paid_Amount_Cum,3)
sch_eve2$Capital_Overdue_Amount <- sch_eve2$Capital_Due_Amount_Cum-sch_eve2$Capital_Paid_Amount_Cum
sch_eve2$Interest_Overdue_Amount <- sch_eve2$Interest_Due_Amount_Cum-sch_eve2$Interest_Paid_Amount_Cum
sch_eve2 <- sch_eve2[order(sch_eve2$Loan_ID, sch_eve2$Date)]

sch_eve2$Prev_Payment_Due_Date <- sch_eve2$Payment_Due_Date
sch_eve2[,segment:=cumsum(!is.na(Prev_Payment_Due_Date))]
sch_eve2[,Prev_Payment_Due_Date:=Prev_Payment_Due_Date[1],by=.(segment,Loan_ID)]

sch_eve2 <- sch_eve2[order(sch_eve2$Loan_ID, -sch_eve2$Date)]

sch_eve2$Next_Payment_Due_Date <- sch_eve2$Payment_Due_Date
sch_eve2[,segment:=cumsum(!is.na(Next_Payment_Due_Date))]
sch_eve2[,Next_Payment_Due_Date:=Next_Payment_Due_Date[1],by=.(segment,Loan_ID)]

sch_eve2 <- sch_eve2[order(sch_eve2$Loan_ID, sch_eve2$Date)]
### when Payment is in advance (Outstanding <0) Payment Date is the next
### otherwise is the previous one
sch_eve2$Final_Payment_Due_Date <- sch_eve2$Prev_Payment_Due_Date
sch_eve2[Total_Overdue_Amount<0, Final_Payment_Due_Date:=Next_Payment_Due_Date]

sch_eve2$Payment_Due_Date <- NULL
sch_eve2$Prev_Payment_Due_Date <- NULL
sch_eve2$Next_Payment_Due_Date <- NULL
sch_eve2$segment <- NULL

setnames(sch_eve2, "Final_Payment_Due_Date", "Payment_Due_Date")

schedules_amounts <- subset(clean_schedules, select=c("Loan_ID","Payment_Due_Date",
                           "Total_Due_Amount","Capital_Due_Amount","Interest_Due_Amount"))

sch_eve2 <- merge(sch_eve2,schedules_amounts, all.x=T)

sch_eve_final <- subset(sch_eve2, select=c("Loan_ID","Payment_Due_Date","Event_Date",
                        "Total_Due_Amount","Capital_Due_Amount","Interest_Due_Amount",
                        "Total_Paid_Amount","Capital_Paid_Amount","Interest_Paid_Amount",
                        "Total_Due_Amount_Cum","Capital_Due_Amount_Cum","Interest_Due_Amount_Cum",
                        "Total_Paid_Amount_Cum","Capital_Paid_Amount_Cum","Interest_Paid_Amount_Cum",
                        "Capital_Overdue_Amount","Interest_Overdue_Amount"))

write.csv(sch_eve_final,"schedule_events.csv",row.names=F)
