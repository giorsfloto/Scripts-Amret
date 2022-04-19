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
googleAuthR::gar_auth_service(json_file = "amret-mfi-kh-6a0c9db85a56.json",
                              scope = getOption("googleAuthR.scopes.selected"))
gcs_global_bucket("amret_dev")
options("httr_oob_default" = TRUE)
project <- "885266346487" # put your project ID here
bq_auth(path = "amret-mfi-kh-6a0c9db85a56.json")  ### json to allow bigrquery to query project Amret KH

### schedules: 11,300,143 records 
schedules1_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                         DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                         FROM `amret-mfi-kh.sandbox.schedule` 
                         WHERE Payment_Due_Date>='2019-03-01' AND Payment_Due_Date<='2019-12-31'")

schedules1 <- query_exec(schedules1_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules2_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                         DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                         FROM `amret-mfi-kh.sandbox.schedule` 
                         WHERE Payment_Due_Date>='2020-01-01' AND Payment_Due_Date<='2020-09-30'")

schedules2 <- query_exec(schedules2_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules3_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                         DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                         FROM `amret-mfi-kh.sandbox.schedule` 
                         WHERE Payment_Due_Date>='2020-10-01' AND Payment_Due_Date<='2021-12-31'")

schedules3 <- query_exec(schedules3_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules4_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                         DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                         FROM `amret-mfi-kh.sandbox.schedule` 
                         WHERE Payment_Due_Date>='2022-01-01'")

schedules4 <- query_exec(schedules4_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules <- rbind(schedules1,schedules2,schedules3,schedules4)

schedules_target <- rbind(schedules1,schedules2)

### loans with instalments between 1/3/2019 and 30/9/2020: 359,824
loans_target <- data.table(ddply(schedules_target, .(Contract_ID), summarize, 
                                 Capital_Due_Amount_Original=sum(Capital_Due_Amount),
                                 Interest_Due_Amount_Original=sum(Interest_Due_Amount)
)) 

loans_target$Total_Due_Amount_Original <- loans_target$Capital_Due_Amount_Original+loans_target$Interest_Due_Amount_Original
### delete files needed to build schedule
rm(schedules1,schedules2,schedules3,schedules4)

##### Loans from Amret extractions: 376,679
loans_sql <- paste0('SELECT Loan_ID, Loan_Disbursement_Date, Loan_Update_Date, Loan_Maturity_Date, Loan_Restructuration_Flag, 
                    Loan_End_Date, Capital_Disbursed_Amount, Loan_Status_Name, Loan_Currency_Code
                    FROM `amret-mfi-kh.sandbox.loan_20201107`')

loans <- query_exec(loans_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

#####
loans_target <- merge(loans_target, loans, by.x="Contract_ID", by.y="Loan_ID", all.x=T)
rm(loans)
##### Loan payments 1/3/2019 -> 30/9/2020 4,897,512 records
events_sql <- paste0("SELECT Contract_ID, Transaction_DateTime,
                     SUM(CASE WHEN Transaction_Type_Code IN ('420','423','750') THEN Transaction_Amount ELSE 0 END) AS Capital_Paid_Amount,
                     SUM(CASE WHEN Transaction_Type_Code IN ('434','751') THEN Transaction_Amount ELSE 0 END) AS Interest_Paid_Amount,
                     SUM(CASE WHEN Transaction_Type_Code='752' THEN Transaction_Amount ELSE 0 END) AS Penalty_Paid_Amount
                     FROM `amret-mfi-kh.sandbox.transaction`WHERE Transaction_Type_Code IN ('420','423','434','750','751','752')
                     GROUP BY 1, 2")

events <- query_exec(events_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

### loans paid: 371,780
loans_paid <- data.table(ddply(events, .(Contract_ID), summarize, 
                               Capital_Paid_Amount = sum(Capital_Paid_Amount),
                               Interest_Paid_Amount = sum(Interest_Paid_Amount),
                               Penalty_Paid_Amount = sum(Penalty_Paid_Amount)
                               
)) 

loans_paid$Total_Paid_Amount <- loans_paid$Capital_Paid_Amount+loans_paid$Interest_Paid_Amount

loans_target <- merge (loans_target, loans_paid, by.x="Contract_ID", by.y="Contract_ID", all.x=T)

### loan balances
## 2019-02-28: 190,081 loans 
loan_balances_0219_sql <- paste0("SELECT Loan_ID, Loan_Balance_Date, 
                                 Principal_Outstanding_Amount AS Principal_Outstanding_Amount_0219, Principal_Overdue_Amount AS Principal_Overdue_Amount_0219, 
                                 Interest_Outstanding_Amount AS Interest_Outstanding_Amount_0219, Interest_Overdue_Amount AS Interest_Overdue_Amount_0219, 
                                 Penalty_Overdue_Amount AS Penalty_Overdue_Amount_0219 
                                 FROM `amret-mfi-kh.sandbox.loan_balance_20190228`")

loan_balances_0219 <- query_exec(loan_balances_0219_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)
## 2020-09-30: 212,537 loans 
loan_balances_0920_sql <- paste0("SELECT Loan_ID, Loan_Balance_Date, 
                                 Principal_Outstanding_Amount AS Principal_Outstanding_Amount_0920, Principal_Overdue_Amount AS Principal_Overdue_Amount_0920, 
                                 Interest_Outstanding_Amount AS Interest_Outstanding_Amount_0920, Interest_Overdue_Amount AS Interest_Overdue_Amount_0920, 
                                 Penalty_Overdue_Amount AS Penalty_Overdue_Amount_0920 
                                 FROM `amret-mfi-kh.sandbox.loan_balance_20200930`")

loan_balances_0920 <- query_exec(loan_balances_0920_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

loans_target <- merge(loans_target, loan_balances_0219, by.x="Contract_ID", 
                      by.y="Loan_ID", all.x=T)
loans_target <- merge(loans_target, loan_balances_0920, by.x="Contract_ID", 
                      by.y="Loan_ID", all.x=T)

loans_target[, Loan_Balance_Date.x := NULL] 
loans_target[, Loan_Balance_Date.y := NULL] 

## data cleaning
loans_target$Capital_Paid_Amount <- convertToNumeric(loans_target$Capital_Paid_Amount)
loans_target$Interest_Paid_Amount <- convertToNumeric(loans_target$Interest_Paid_Amount)
loans_target$Penalty_Paid_Amount <- convertToNumeric(loans_target$Penalty_Paid_Amount)
loans_target$Total_Paid_Amount <- convertToNumeric(loans_target$Total_Paid_Amount)
loans_target$Principal_Outstanding_Amount_0920 <- convertToNumeric(loans_target$Principal_Outstanding_Amount_0920)
loans_target$Principal_Overdue_Amount_0920 <- convertToNumeric(loans_target$Principal_Overdue_Amount_0920)
loans_target$Interest_Outstanding_Amount_0920 <- convertToNumeric(loans_target$Interest_Outstanding_Amount_0920)
loans_target$Interest_Overdue_Amount_0920 <- convertToNumeric(loans_target$Interest_Overdue_Amount_0920)
loans_target$Penalty_Overdue_Amount_0920 <- convertToNumeric(loans_target$Penalty_Overdue_Amount_0920)
loans_target$Principal_Outstanding_Amount_0219 <- convertToNumeric(loans_target$Principal_Outstanding_Amount_0219)
loans_target$Principal_Overdue_Amount_0219 <- convertToNumeric(loans_target$Principal_Overdue_Amount_0219)
loans_target$Interest_Outstanding_Amount_0219 <- convertToNumeric(loans_target$Interest_Outstanding_Amount_0219)
loans_target$Interest_Overdue_Amount_0219 <- convertToNumeric(loans_target$Interest_Overdue_Amount_0219)
loans_target$Penalty_Overdue_Amount_0219 <- convertToNumeric(loans_target$Penalty_Overdue_Amount_0219)

### Outstanding amount at 1/3/2019: if Principal_Outstanding_Amount_0219>0 else Capital_Disbursed (for loans disbursed later)  
loans_target$Start_Amount <- ifelse(loans_target$Principal_Outstanding_Amount_0219>0,
                                    loans_target$Principal_Outstanding_Amount_0219,
                                    loans_target$Capital_Disbursed_Amount)

loans_target$consistency_flag <- ifelse(
  (loans_target$Loan_Currency_Code=='USD'& 
     abs(loans_target$Start_Amount-loans_target$Principal_Outstanding_Amount_0920-loans_target$Capital_Paid_Amount)>0.1)
  |(loans_target$Loan_Currency_Code=='THB'& 
      abs(loans_target$Start_Amount-loans_target$Principal_Outstanding_Amount_0920-loans_target$Capital_Paid_Amount)>1)
  |(loans_target$Loan_Currency_Code=='KHR'& 
      abs(loans_target$Start_Amount-loans_target$Principal_Outstanding_Amount_0920-loans_target$Capital_Paid_Amount)>100),0,1)

### Cross-check of capital payments on original data
loans_target$Capital_OK0 <- ifelse(
  ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount_Original>0.1))
   |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount_Original>1)) 
   |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount_Original>100))),2,
  ifelse(
    ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount_Original< -0.1))
     |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount_Original< -1)) 
     |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount_Original< -100))),0,1))

table(loans_target$Capital_OK0)
consistent_loans <- subset(loans_target, loans_target$consistency_flag==1)
table(consistent_loans$Capital_OK0)
mytable <- table(consistent_loans$Capital_OK0)
prop.table(mytable)

### Cross-check of interest payments on original data
loans_target$Interest_OK0 <- ifelse(
  ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount_Original>0.1))
   |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount_Original>1)) 
   |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount_Original>100))),2,
  ifelse(
    ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount_Original< -0.1))
     |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount_Original< -1)) 
     |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount_Original< -100))),0,1))

table(loans_target$Interest_OK0)
consistent_loans <- subset(loans_target, loans_target$consistency_flag==1)
table(consistent_loans$Interest_OK0)
mytable <- table(consistent_loans$Interest_OK0)
prop.table(mytable)

### version of schedules
schedules_version_sql <- paste0("SELECT *, 
                                COUNT(*) OVER (PARTITION BY Contract_ID, Payment_Month ORDER BY Schedule_Update_Date DESC) AS Version
                                FROM
                                (
                                SELECT Contract_ID, DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,
                                Schedule_Update_Date
                                FROM `amret-mfi-kh.sandbox.schedule` 
                                WHERE Payment_Due_Date>='2019-03-01'
                                GROUP BY 1, 2, 3)")

schedules_version <- query_exec(schedules_version_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules <- merge(schedules, schedules_version, by=c("Contract_ID", "Payment_Month","Schedule_Update_Date"), all.x=T)

## schedules loans target 1/9/2019: 10,766,049
## we join schedules and loans so to use end and maturity dates and loan statuses to select the schedules
schedules_loans <- merge(schedules,loans_target, by.x="Contract_ID",by.y="Contract_ID", all=FALSE)

### exclude instalments with Payment Due Date <= Disbursement Date
schedules_loans <- subset(schedules_loans, 
                                  schedules_loans$Payment_Due_Date>
                                    schedules_loans$Loan_Disbursement_Date)
## capital instalment with Version<=2 (newest and previous ones)
## 5,285,229: records
schedules_loans_capital <- subset(schedules_loans,
                                  schedules_loans$Version<=2
                                  & (
                                    (schedules_loans$Loan_Status_Name=='CLOSED' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                                    |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date>'2020-09-30' & schedules_loans$Payment_Due_Date<='2020-09-30')
                                    |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date<='2020-09-30' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                                  ))

### last instalments (Version=1): 5,220,628
schedules_loans_capital1 <- subset(schedules_loans_capital, schedules_loans_capital$Version==1)

loans_capital_due <- data.table(ddply(schedules_loans_capital1, .(Contract_ID), summarize, 
                                      Capital_Due_Amount = sum(Capital_Due_Amount)
)) 

loans_target <- merge(loans_target, loans_capital_due, by.x="Contract_ID", by.y="Contract_ID", all.x=T)
loans_target_saved <- loans_target

loans_target$Capital_Due_Amount <- convertToNumeric(loans_target$Capital_Due_Amount)

### marge de tolerance pour definir les echeances OK: 0.1 USD, 1 THB, 100 KHR
loans_target$Capital_OK1 <- ifelse(
  ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount>0.1))
   |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount>1)) 
   |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount>100))),2,
  ifelse(
    ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount< -0.1))
     |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount< -1)) 
     |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Capital_Paid_Amount+loans_target$Principal_Overdue_Amount_0920-loans_target$Principal_Overdue_Amount_0219-loans_target$Capital_Due_Amount< -100))),0,1))

consistent_loans <- subset(loans_target,loans_target$consistency_flag==1)
table(consistent_loans$Capital_OK1)
## 317,348 OK (90.1%) 7,743 with paid < due (2.2%), 26,941 with paid>due (7.7%)
mytable <- table(consistent_loans$Capital_OK1)
prop.table(mytable)

#### subset of schedules for ok loans (317,348)
loans_ok <- subset(consistent_loans, consistent_loans$Capital_OK1==1)
loans_ok <- subset(loans_ok, select=c("Contract_ID"))

schedules_ok1 <- merge(loans_ok, schedules_loans_capital1, by.x="Contract_ID",
                       by.y="Contract_ID", all.x=T)

schedules_ok1 <- subset(schedules_ok1, schedules_ok1$Capital_Due_Amount!=0)

schedules_ok1 <- subset(schedules_ok1,select=c("Contract_ID","Payment_Due_Date",
                                               "Loan_Currency_Code","Capital_Due_Amount"))

### Capital 2nd step
### for instalments with paid>due and where we can trust capital paid from transactions
### (Capital_Paid=Capital_Disbursed_Amount-Principal_Outstanding_Amount)
### we verify if there are some single instalments from older schedules (Version=2) exactly equal to the mismatch between paid and due
### selection of contracts: 26,941 
loans_ko <- subset(loans_target, loans_target$Capital_OK1==2 & loans_target$consistency_flag==1)

### Mismatch between what was paid (OK) and our calculation of due (KO)
loans_ko$Mismatch <- loans_ko$Capital_Paid_Amount-loans_ko$Capital_Due_Amount
loans_ko <- subset(loans_ko, select=c("Contract_ID","Mismatch"))

### we take all schedules for capital with Version 1 & 2 (newest schedules and previous ones)
### subset of schedules of ko loans
loans_capital2 <- merge(loans_ko, schedules_loans_capital, 
                        by.x="Contract_ID", by.y="Contract_ID", all.x=T)

### we keep newest (Version=1) and previous if their amount equal to mismatch
loans_capital2 <- subset(loans_capital2, loans_capital2$Version==1 
                         | 
                           (loans_capital2$Version==2	& 
                              ((loans_capital2$Loan_Currency_Code=='USD' & abs(loans_capital2$Capital_Due_Amount-loans_capital2$Mismatch)<0.1)
                               |(loans_capital2$Loan_Currency_Code=='THB' & abs(loans_capital2$Capital_Due_Amount-loans_capital2$Mismatch)<1)
                               |(loans_capital2$Loan_Currency_Code=='KHR' & abs(loans_capital2$Capital_Due_Amount-loans_capital2$Mismatch)<100))))

loans_ko <- data.table(ddply(loans_capital2, .(Contract_ID, Loan_Currency_Code, Capital_Paid_Amount, Principal_Overdue_Amount_0219, Principal_Overdue_Amount_0920), summarize, 
                             Capital_Due_Amount = sum(Capital_Due_Amount)
)) 
loans_ko$Capital_Paid_Amount <- convertToNumeric(loans_ko$Capital_Paid_Amount)
loans_ko$Principal_Overdue_Amount_0920 <- convertToNumeric(loans_ko$Principal_Overdue_Amount_0920)
loans_ko$Principal_Overdue_Amount_0219 <- convertToNumeric(loans_ko$Principal_Overdue_Amount_0219)
loans_ko$Capital_Due_Amount <- convertToNumeric(loans_ko$Capital_Due_Amount)

loans_ko$Capital_OK2 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Contract_ID","Capital_OK2"))
table(loans_ko$Capital_OK2)
loans_target <- merge(loans_target,loans_ko, all.x=T)
loans_target$Capital_OK2 <- convertToNumeric(loans_target$Capital_OK2)
loans_target$Capital_OK <-ifelse(loans_target$Capital_OK1==1 | loans_target$Capital_OK2==1,1,loans_target$Capital_OK1)
consistent_loans <- subset(loans_target, loans_target$consistency_flag==1)
table(consistent_loans$Capital_OK)
## 188,625 OK (96.1%) 6,442 with paid < due (3.3%), 1,228 with paid>due (0.6%)
mytable <- table(consistent_loans$Capital_OK)
prop.table(mytable)

#### subset of schedules for ok loans (19,066)
loans_ok <- subset(loans_ko, loans_ko$Capital_OK2==1)
loans_ok <- subset(loans_ok, select=c("Contract_ID"))

schedules_ok2 <- merge(loans_ok, loans_capital2, by.x="Contract_ID",
                       by.y="Contract_ID", all.x=T)

schedules_ok2 <- subset(schedules_ok2, schedules_ok2$Capital_Due_Amount!=0)

schedules_ok2 <- subset(schedules_ok2,select=c("Contract_ID","Payment_Due_Date",
                                               "Loan_Currency_Code","Capital_Due_Amount"))


schedules_ok <- rbind(schedules_ok1, schedules_ok2)
### Capital 3rd step
### for instalments with paid<due that were restructured
### (Loan_Restructuration_Flag=TRUE)
### we exclude older instalments with payment due dates after restructuration Date (Loan_Update_Date)
### selection of contracts: 7,159 
loans_ko <- subset(loans_target, loans_target$Capital_OK1==0 & loans_target$Loan_Restructuration_Flag==TRUE
                   & loans_target$consistency_flag==1)

loans_ko <- subset(loans_ko, select=c("Contract_ID"))


schedules_loans_restructured <- merge(loans_ko, schedules_loans_capital1,
                                      by.x="Contract_ID", by.y="Contract_ID", all.x=T)

schedules_loans_restructured <- subset(schedules_loans_restructured,
                                       (schedules_loans_restructured$Schedule_Update_Date<schedules_loans_restructured$Loan_Update_Date
                                        & schedules_loans_restructured$Payment_Due_Date<schedules_loans_restructured$Loan_Update_Date)  
                                       | schedules_loans_restructured$Schedule_Update_Date>=schedules_loans_restructured$Loan_Update_Date)

loans_ko <- data.table(ddply(schedules_loans_restructured, .(Contract_ID, Loan_Currency_Code, Capital_Paid_Amount, Principal_Overdue_Amount_0219, Principal_Overdue_Amount_0920), summarize, 
                             Capital_Due_Amount = sum(Capital_Due_Amount)
))                    

loans_ko$Capital_Paid_Amount <- convertToNumeric(loans_ko$Capital_Paid_Amount)
loans_ko$Principal_Overdue_Amount_0920 <- convertToNumeric(loans_ko$Principal_Overdue_Amount_0920)
loans_ko$Principal_Overdue_Amount_0219 <- convertToNumeric(loans_ko$Principal_Overdue_Amount_0219)
loans_ko$Capital_Due_Amount <- convertToNumeric(loans_ko$Capital_Due_Amount)

loans_ko$Capital_OK3 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount_0920-loans_ko$Principal_Overdue_Amount_0219-loans_ko$Capital_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Contract_ID","Capital_OK3"))
table(loans_ko$Capital_OK3)
loans_target <- merge(loans_target,loans_ko, all.x=T)
loans_target$Capital_OK3 <- convertToNumeric(loans_target$Capital_OK3)
loans_target$Capital_OK <-ifelse(loans_target$Capital_OK1==1 | loans_target$Capital_OK2==1 | loans_target$Capital_OK3==1,
                                 1,loans_target$Capital_OK1)

consistent_loans <- subset(loans_target, loans_target$consistency_flag==1)
table(consistent_loans$Capital_OK)
## 188,625 OK (96.1%) 6,442 with paid < due (3.3%), 1,228 with paid>due (0.6%)
mytable <- table(consistent_loans$Capital_OK)
prop.table(mytable)

#### subset of schedules for ok loans (6,187)
loans_ok <- subset(loans_ko, loans_ko$Capital_OK3==1)
loans_ok <- subset(loans_ok, select=c("Contract_ID"))

schedules_ok3 <- merge(loans_ok, schedules_loans_restructured, by.x="Contract_ID",
                       by.y="Contract_ID", all.x=T)

schedules_ok3 <- subset(schedules_ok3, schedules_ok3$Capital_Due_Amount!=0)

schedules_ok3 <- subset(schedules_ok3,select=c("Contract_ID","Payment_Due_Date",
                                               "Loan_Currency_Code","Capital_Due_Amount"))


schedules_ok <- rbind(schedules_ok1, schedules_ok2, schedules_ok3)
write.csv(schedules_ok,"capital_schedules_ok.csv",row.names=F)
#length(unique(paste0(schedules_ok$Contract_ID,schedules_ok$Payment_Due_Date)))
rm(schedules_ok1,schedules_ok2,schedules_ok3,schedules_version)
### Interest 1st step
### echeances interets V1-V2: on prend les derniers schedules pour chaque mois (Version<=2)
### et pour les credits cloturés on prend toutes les echeances jusqu'a la date de clouture (<> Capital!!!)
### 3,799,172: records
schedules_loans_interest <- subset(schedules_loans,
                                   schedules_loans$Version<=2
                                   & (
                                     (schedules_loans$Loan_Status_Name=='CLOSED' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_End_Date)
                                     |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date>'2020-09-30' & schedules_loans$Payment_Due_Date<='2020-09-30')
                                     |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date<='2020-09-30' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                                     
                                   ))
#### last schedules (Version1): 3,751,390
schedules_loans_interest1 <- subset(schedules_loans_interest, schedules_loans_interest$Version==1)
#### 359,624
loans_interest_due <- data.table(ddply(schedules_loans_interest1, .(Contract_ID), summarize, 
                                       Interest_Due_Amount = sum(Interest_Due_Amount)
)) 

loans_target <- merge(loans_target, loans_interest_due, by.x="Contract_ID", by.y="Contract_ID", all.x=T)
loans_target$Interest_Due_Amount <- convertToNumeric(loans_target$Interest_Due_Amount)


### marge de tolerance pour definir les echeances OK: 0.1 USD, 1 THB, 100 KHR
loans_target$Interest_OK1 <- ifelse(
  ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount>0.1))
   |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount>1)) 
   |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount>100))),2,
  ifelse(
    ((loans_target$Loan_Currency_Code=='USD'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount< -0.1))
     |(loans_target$Loan_Currency_Code=='THB'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount< -1)) 
     |(loans_target$Loan_Currency_Code=='KHR'& (loans_target$Interest_Paid_Amount+loans_target$Interest_Overdue_Amount_0920-loans_target$Interest_Overdue_Amount_0219-loans_target$Interest_Due_Amount< -100))),0,1))

consistent_loans <- subset(loans_target,loans_target$consistency_flag==1)
table(consistent_loans$Interest_OK1)
## 165,562 OK (84.3%) 6,921 with paid < due (3.5%), 23,812 with paid>due (12.1%)
mytable <- table(consistent_loans$Interest_OK1)
prop.table(mytable)

#### subset of schedules for ok loans (244,424)
loans_ok <- subset(consistent_loans, consistent_loans$Interest_OK1==1)
loans_ok <- subset(loans_ok, select=c("Contract_ID"))

schedules_ok1 <- merge(loans_ok, schedules_loans_interest1, by.x="Contract_ID",
                       by.y="Contract_ID", all.x=T)

schedules_ok1 <- subset(schedules_ok1, schedules_ok1$Interest_Due_Amount!=0)

schedules_ok1 <- subset(schedules_ok1,select=c("Contract_ID","Payment_Due_Date",
                                               "Loan_Currency_Code","Interest_Due_Amount"))

### Interest 2nd step
### we verify if there are some single instalments from older schedules (Version=2) exactly equal to the mismatch between paid and due
### selection of contracts: 97,261
loans_ko <- subset(loans_target, loans_target$Interest_OK1==2 & loans_target$consistency_flag==1)

loans_ko$Mismatch <- loans_ko$Interest_Paid_Amount-loans_ko$Interest_Due_Amount
loans_ko <- subset(loans_ko, select=c("Contract_ID","Mismatch"))

loans_interest2 <- merge(loans_ko, schedules_loans_interest, 
                         by.x="Contract_ID", by.y="Contract_ID", all.x=T)


loans_interest2 <- subset(loans_interest2, loans_interest2$Version==1 
                          | 
                            (loans_interest2$Version==2	& 
                               ((loans_interest2$Loan_Currency_Code=='USD' & abs(loans_interest2$Interest_Due_Amount-loans_interest2$Mismatch)<0.1)
                                |(loans_interest2$Loan_Currency_Code=='THB' & abs(loans_interest2$Interest_Due_Amount-loans_interest2$Mismatch)<1)
                                |(loans_interest2$Loan_Currency_Code=='KHR' & abs(loans_interest2$Interest_Due_Amount-loans_interest2$Mismatch)<100))))

loans_ko <- data.table(ddply(loans_interest2, .(Contract_ID, Loan_Currency_Code, Interest_Paid_Amount, Interest_Overdue_Amount_0219,Interest_Overdue_Amount_0920), summarize, 
                             Interest_Due_Amount = sum(Interest_Due_Amount)
)) 

loans_ko$Interest_Paid_Amount <- convertToNumeric(loans_ko$Interest_Paid_Amount)
loans_ko$Interest_Overdue_Amount_0920 <- convertToNumeric(loans_ko$Interest_Overdue_Amount_0920)
loans_ko$Interest_Overdue_Amount_0219 <- convertToNumeric(loans_ko$Interest_Overdue_Amount_0219)
loans_ko$Interest_Due_Amount <- convertToNumeric(loans_ko$Interest_Due_Amount)

loans_ko$Interest_OK2 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Contract_ID","Interest_OK2"))
table(loans_ko$Interest_OK2)
loans_target <- merge(loans_target,loans_ko, all.x=T)
loans_target$Interest_OK2 <- convertToNumeric(loans_target$Interest_OK2)
loans_target$Interest_OK <-ifelse(loans_target$Interest_OK1==1 | loans_target$Interest_OK2==1,1,loans_target$Interest_OK1)

consistent_loans <- subset(loans_target, loans_target$consistency_flag==1)
table(consistent_loans$Interest_OK)
## 266,459 OK (75.7%) 10,347 with paid < due (2.9%), 75,136 with paid>due (21.3%)
mytable <- table(consistent_loans$Interest_OK)
prop.table(mytable)

#### subset of schedules for ok loans (22,125)
loans_ok <- subset(loans_ko, loans_ko$Interest_OK2==1)
loans_ok <- subset(loans_ok, select=c("Contract_ID"))

schedules_ok2 <- merge(loans_ok, loans_interest2, by.x="Contract_ID",
                       by.y="Contract_ID", all.x=T)

schedules_ok2 <- subset(schedules_ok2, schedules_ok2$Interest_Due_Amount!=0)

schedules_ok2 <- subset(schedules_ok2,select=c("Contract_ID","Payment_Due_Date",
                                               "Loan_Currency_Code","Interest_Due_Amount"))


schedules_interest_ok <- rbind(schedules_ok1, schedules_ok2)
#length(unique(schedules_interest_ok$Contract_ID,schedules_interest_ok$Payment_Due_Date))
### Interest 3rd step
### for instalments with paid<due that were restructured
### (Loan_Restructuration_Flag=TRUE)
### we exclude older instalments with payment due dates after restructuration Date (Loan_Update_Date)
### selection of contracts: 7,512 
loans_ko <- subset(loans_target, loans_target$Interest_OK1==0 
                   & loans_target$Loan_Restructuration_Flag==TRUE & loans_target$consistency_flag==1)

loans_ko <- subset(loans_ko, select=c("Contract_ID"))

schedules_loans_restructured <- merge(loans_ko, schedules_loans_interest1,
                                      by.x="Contract_ID", by.y="Contract_ID", all.x=T)

schedules_loans_restructured <- subset(schedules_loans_restructured,
                                       (schedules_loans_restructured$Schedule_Update_Date<schedules_loans_restructured$Loan_Update_Date
                                        & schedules_loans_restructured$Payment_Due_Date<schedules_loans_restructured$Loan_Update_Date)  
                                       | schedules_loans_restructured$Schedule_Update_Date>=schedules_loans_restructured$Loan_Update_Date)

loans_ko <- data.table(ddply(schedules_loans_restructured, .(Contract_ID, Loan_Currency_Code, Interest_Paid_Amount, Interest_Overdue_Amount_0219, Interest_Overdue_Amount_0920), summarize, 
                             Interest_Due_Amount = sum(Interest_Due_Amount)
))                    

loans_ko$Interest_Paid_Amount <- convertToNumeric(loans_ko$Interest_Paid_Amount)
loans_ko$Interest_Overdue_Amount_0920 <- convertToNumeric(loans_ko$Interest_Overdue_Amount_0920)
loans_ko$Interest_Overdue_Amount_0219 <- convertToNumeric(loans_ko$Interest_Overdue_Amount_0219)
loans_ko$Interest_Due_Amount <- convertToNumeric(loans_ko$Interest_Due_Amount)

loans_ko$Interest_OK3 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount_0920-loans_ko$Interest_Overdue_Amount_0219-loans_ko$Interest_Due_Amount< -100))),0,1))
table(loans_ko$Interest_OK3)
loans_ko <- subset(loans_ko, select=c("Contract_ID","Interest_OK3"))

loans_target <- merge(loans_target,loans_ko, all.x=T)
loans_target$Interest_OK3 <- convertToNumeric(loans_target$Interest_OK3)
loans_target$Interest_OK <-ifelse(loans_target$Interest_OK1==1 | loans_target$Interest_OK2==1 | loans_target$Interest_OK3==1,
                                  1,loans_target$Interest_OK1)
consistent_loans <- subset(loans_target,loans_target$consistency_flag==1)
table(consistent_loans$Interest_OK)
## 171,729 OK (87.5%) 4,359 with paid < due (2.2%), 20,207 with paid>due (10.3%)
mytable <- table(consistent_loans$Interest_OK)
prop.table(mytable)

#### subset of schedules for ok loans (3,711)
loans_ok <- subset(loans_ko, loans_ko$Interest_OK3==1)
loans_ok <- subset(loans_ok, select=c("Contract_ID"))

schedules_ok3 <- merge(loans_ok, schedules_loans_restructured, by.x="Contract_ID",
                       by.y="Contract_ID", all.x=T)

schedules_ok3 <- subset(schedules_ok3, schedules_ok3$Interest_Due_Amount!=0)

schedules_ok3 <- subset(schedules_ok3,select=c("Contract_ID","Payment_Due_Date",
                                               "Loan_Currency_Code","Interest_Due_Amount"))


schedules_interest_ok <- rbind(schedules_ok1, schedules_ok2, schedules_ok3)
### for interests ko, we take schedules according to version 1
loans_ko <- subset(loans_target, loans_target$consistency_flag==1
                   &loans_target$Interest_OK!=1)

loans_ko <- subset(loans_ko, select=c("Contract_ID"))

schedules_ko <- merge(loans_ko, schedules_loans_interest1, by.x="Contract_ID",
                      by.y="Contract_ID", all.x=T)

schedules_ko <- subset(schedules_ko, schedules_ko$Interest_Due_Amount!=0)

schedules_ko <- subset(schedules_ko,select=c("Contract_ID","Payment_Due_Date",
                                             "Loan_Currency_Code","Interest_Due_Amount"))


schedules_interest <- rbind(schedules_ok1, schedules_ok2, schedules_ok3,
                            schedules_ko)

schedules_interest <- subset(schedules_interest, 
                             schedules_interest$Contract_ID %in% schedules_ok$Contract_ID)

write.csv(schedules_interest,"interest_schedules.csv",row.names=F)

schedules_final_ok <- merge(schedules_ok,schedules_interest, 
                            by.x=c("Contract_ID","Payment_Due_Date","Loan_Currency_Code"),
                            by.y=c("Contract_ID","Payment_Due_Date","Loan_Currency_Code"),
                            all=TRUE)

UploadBigQuery(schedules_final_ok, "schedules_ok", "sandbox", project, overwrite = TRUE)
#### export of inconsistent loans
inconsistent_loans <- subset(loans_target, loans_target$consistency_flag==0)

inconsistent_loans <- subset(inconsistent_loans,
                             select=c("Contract_ID","Capital_Disbursed_Amount", "Loan_Currency_Code",
                                      "Principal_Outstanding_Amount_0219","Principal_Outstanding_Amount_0920",
                                      "Capital_Paid_Amount"))

write.csv(incosistent_loans,"inconsistent_loans.csv", row.names = F)

#### export of mismatched loans
mismatched_loans <- subset(loans_target, loans_target$consistency_flag==1 
                           & loans_target$Capital_OK!=1)

mismatched_loans <- subset(mismatched_loans,
                           select=c("Contract_ID","Capital_Due_Amount","Capital_Paid_Amount"))

write.csv(mismatched_loans,"mismatched_loans.csv", row.names = F)

