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

##### Loans disbursed after 1/3/2019: 196,295
loans_sql <- paste0('SELECT Loan_ID, Loan_Disbursement_Date, Loan_Update_Date, Loan_Maturity_Date, Loan_Restructuration_Flag, 
						                Loan_End_Date, Capital_Disbursed_Amount, Loan_Status_Name, Loan_Currency_Code
					           FROM `amret-mfi-kh.sandbox.loan_20201107` 
					           WHERE Loan_Disbursement_Date>="2019-03-01"')

loans <- query_exec(loans_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)


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

### loan balances: 212,537 loans (2020-09-30)
loan_balances_sql <- paste0("SELECT Loan_ID, Loan_Balance_Date, Principal_Outstanding_Amount, Principal_Overdue_Amount, 
                                Interest_Outstanding_Amount, Interest_Overdue_Amount, Penalty_Overdue_Amount 
                              FROM `amret-mfi-kh.sandbox.loan_balance`")

loan_balances <- query_exec(loan_balances_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

### schedules 
schedules1_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                            DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                          FROM `amret-mfi-kh.sandbox.schedule` 
                          WHERE Payment_Due_Date>='2019-03-01' AND Payment_Due_Date<='2019-06-30'")

schedules1 <- query_exec(schedules1_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules2_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                            DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                          FROM `amret-mfi-kh.sandbox.schedule` 
                          WHERE Payment_Due_Date>='2019-07-01' AND Payment_Due_Date<='2019-12-31'")

schedules2 <- query_exec(schedules2_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules3_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                            DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                          FROM `amret-mfi-kh.sandbox.schedule` 
                          WHERE Payment_Due_Date>='2020-01-01' AND Payment_Due_Date<='2020-12-31'")

schedules3 <- query_exec(schedules3_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules4_sql <- paste0("SELECT Contract_ID, Payment_Due_Date, Capital_Due_Amount, Interest_Due_Amount,
                            DATETIME_TRUNC(Payment_Due_Date, MONTH) AS Payment_Month,Schedule_Update_Date
                          FROM `amret-mfi-kh.sandbox.schedule` 
                          WHERE Payment_Due_Date>='2021-01-01'")

schedules4 <- query_exec(schedules4_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

schedules <- rbind(schedules1,schedules2,schedules3,schedules4)

### delete files needed to build schedule
rm(schedules1,schedules2,schedules3,schedules4)

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

## schedules loans disbursed since 1/9/2019: 6,578,459
schedules_loans <- merge(schedules,loans, by.x="Contract_ID",by.y="Loan_ID", all=FALSE)

## echeances capital V1: on prend les derniers schedules pour chaque mois (Version=1)
## et pour les credits cloturés on prend toutes les echeances jusqu'a la date de maturité
## 2,068,931: records
schedules_loans_capital1 <- subset(schedules_loans,
                              schedules_loans$Version==1
                              & (
                               (schedules_loans$Loan_Status_Name=='CLOSED' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                              |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date>'2020-09-30' & schedules_loans$Payment_Due_Date<='2020-09-30')
                              |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date<='2020-09-30' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                              ))
loans_capital_due <- data.table(ddply(schedules_loans_capital1, .(Contract_ID), summarize, 
                               Capital_Due_Amount = sum(Capital_Due_Amount)
)) 


loans <- merge(loans, loans_paid, by.x="Loan_ID", by.y="Contract_ID", all.x=T)
loans <- merge(loans, loan_balances, all.x=T)
loans <- merge(loans, loans_capital_due, by.x="Loan_ID", by.y="Contract_ID", all.x=T)

loans$Capital_Paid_Amount <- convertToNumeric(loans$Capital_Paid_Amount)
loans$Interest_Paid_Amount <- convertToNumeric(loans$Interest_Paid_Amount)
loans$Penalty_Paid_Amount <- convertToNumeric(loans$Penalty_Paid_Amount)
loans$Total_Paid_Amount <- convertToNumeric(loans$Total_Paid_Amount)
loans$Principal_Outstanding_Amount <- convertToNumeric(loans$Principal_Outstanding_Amount)
loans$Principal_Overdue_Amount <- convertToNumeric(loans$Principal_Overdue_Amount)
loans$Interest_Outstanding_Amount <- convertToNumeric(loans$Interest_Outstanding_Amount)
loans$Interest_Overdue_Amount <- convertToNumeric(loans$Interest_Overdue_Amount)
loans$Penalty_Overdue_Amount <- convertToNumeric(loans$Penalty_Overdue_Amount)
loans$Capital_Due_Amount <- convertToNumeric(loans$Capital_Due_Amount)

### marge de tolerance pour definir les echeances OK: 0.1 USD, 1 THB, 100 KHR
loans$Capital_OK1 <- ifelse(
  ((loans$Loan_Currency_Code=='USD'& (loans$Capital_Paid_Amount+loans$Principal_Overdue_Amount-loans$Capital_Due_Amount>0.1))
   |(loans$Loan_Currency_Code=='THB'& (loans$Capital_Paid_Amount+loans$Principal_Overdue_Amount-loans$Capital_Due_Amount>1)) 
   |(loans$Loan_Currency_Code=='KHR'& (loans$Capital_Paid_Amount+loans$Principal_Overdue_Amount-loans$Capital_Due_Amount>100))),2,
  ifelse(
    ((loans$Loan_Currency_Code=='USD'& (loans$Capital_Paid_Amount+loans$Principal_Overdue_Amount-loans$Capital_Due_Amount< -0.1))
     |(loans$Loan_Currency_Code=='THB'& (loans$Capital_Paid_Amount+loans$Principal_Overdue_Amount-loans$Capital_Due_Amount< -1)) 
     |(loans$Loan_Currency_Code=='KHR'& (loans$Capital_Paid_Amount+loans$Principal_Overdue_Amount-loans$Capital_Due_Amount< -100))),0,1))

table(loans$Capital_OK1)

## 183,901 OK (93.7%) 6,442 with paid < due (3.3%), 5,952 with paid>due (3.0%)
mytable <- table(loans$Capital_OK1)
prop.table(mytable)

### Capital 2nd step
### for instalments with paid>due and where we can trust capital paid from transactions
### (Capital_Paid=Capital_Disbursed_Amount-Principal_Outstanding_Amount)
### we verify if there are some single instlalment from older schedules (Version=2) exactly equal to the mismatch between paid and due
### selection of contracts: 5,669 
loans_ko <- subset(loans, loans$Capital_OK1==2 
                   & loans$Capital_Disbursed_Amount-loans$Principal_Outstanding_Amount-loans$Capital_Paid_Amount==0)

loans_ko$Mismatch <- loans_ko$Capital_Paid_Amount-loans_ko$Capital_Due_Amount
loans_ko <- subset(loans_ko, select=c("Loan_ID","Capital_Paid_Amount"
                                      ,"Principal_Overdue_Amount","Mismatch"))

schedules_loans_capital12 <- subset(schedules_loans,
                                   schedules_loans$Version<=2
                                   & (
                                     (schedules_loans$Loan_Status_Name=='CLOSED' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                                     |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date>'2020-09-30' & schedules_loans$Payment_Due_Date<='2020-09-30')
                                     |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date<='2020-09-30' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                                   ))
loans_capital2 <- merge(loans_ko, schedules_loans_capital12, 
                        by.x="Loan_ID", by.y="Contract_ID", all.x=T)


loans_capital2 <- subset(loans_capital2, loans_capital2$Version==1 
                         | loans_capital2$Capital_Due_Amount==loans_capital2$Mismatch)

loans_ko <- data.table(ddply(loans_capital2, .(Loan_ID, Loan_Currency_Code, Capital_Paid_Amount, Principal_Overdue_Amount), summarize, 
                                        Capital_Due_Amount = sum(Capital_Due_Amount)
)) 

loans_ko$Capital_OK2 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Loan_ID","Capital_OK2"))

loans <- merge(loans,loans_ko, all.x=T)
loans$Capital_OK2 <- convertToNumeric(loans$Capital_OK2)
loans$Capital_OK <-ifelse(loans$Capital_OK1==1 | loans$Capital_OK2==1,1,loans$Capital_OK1)

## 188,625 OK (96.1%) 6,442 with paid < due (3.3%), 1,228 with paid>due (0.6%)
mytable <- table(loans$Capital_OK)
prop.table(mytable)

### Capital 3rd step
### for instalments with paid<due that were restructured
### (Loan_Restructuration_Flag=TRUE)
### we exclude older instalments with payment due dates after restructuration Date (Loan_Update_Date)
### selection of contracts: 5,956 
loans_ko <- subset(loans, loans$Capital_OK1==0 & loans$Loan_Restructuration_Flag==TRUE)

loans_ko$Mismatch <- loans_ko$Capital_Paid_Amount-loans_ko$Capital_Due_Amount
loans_ko <- subset(loans_ko, select=c("Loan_ID","Capital_Paid_Amount"
                                      ,"Principal_Overdue_Amount","Mismatch"))


schedules_loans_restructured <- merge(loans_ko, schedules_loans_capital1,
                                      by.x="Loan_ID", by.y="Contract_ID", all.x=T)

schedules_loans_restructured <- subset(schedules_loans_restructured,
                     (schedules_loans_restructured$Schedule_Update_Date<schedules_loans_restructured$Loan_Update_Date
                      & schedules_loans_restructured$Payment_Due_Date<schedules_loans_restructured$Loan_Update_Date)  
                    | schedules_loans_restructured$Schedule_Update_Date>=schedules_loans_restructured$Loan_Update_Date)
                      
loans_ko <- data.table(ddply(schedules_loans_restructured, .(Loan_ID, Loan_Currency_Code, Capital_Paid_Amount, Principal_Overdue_Amount), summarize, 
                             Capital_Due_Amount = sum(Capital_Due_Amount)
))                    

loans_ko$Capital_OK3 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Capital_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Capital_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Loan_ID","Capital_OK3"))

loans <- merge(loans,loans_ko, all.x=T)
loans$Capital_OK3 <- convertToNumeric(loans$Capital_OK3)
loans$Capital_OK <-ifelse(loans$Capital_OK1==1 | loans$Capital_OK2==1 | loans$Capital_OK3==1,
                          1,loans$Capital_OK1)

## 193,761 OK (98.7%) 1,306 with paid < due (0.7%), 1,228 with paid>due (0.6%)
mytable <- table(loans$Capital_OK)
prop.table(mytable)


### Capital 1st step
### echeances interets V1-V2: on prend les derniers schedules pour chaque mois (Version<=2)
### et pour les credits cloturés on prend toutes les echeances jusqu'a la date de clouture (<> Capital!!!)
### 1,668,501: records
schedules_loans_interest <- subset(schedules_loans,
                                   schedules_loans$Version<=2
                                   & (
                                     (schedules_loans$Loan_Status_Name=='CLOSED' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_End_Date)
                                     |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date>'2020-09-30' & schedules_loans$Payment_Due_Date<='2020-09-30')
                                     |(schedules_loans$Loan_Status_Name!='CLOSED' & schedules_loans$Loan_Maturity_Date<='2020-09-30' & schedules_loans$Payment_Due_Date<=schedules_loans$Loan_Maturity_Date)
                                     ))
schedules_loans_interest1 <- subset(schedules_loans_interest, schedules_loans_interest$Version==1)
#### 180,007
loans_interest_due <- data.table(ddply(schedules_loans_interest1, .(Contract_ID), summarize, 
                                       Interest_Due_Amount = sum(Interest_Due_Amount)
)) 

loans <- merge(loans, loans_interest_due, by.x="Loan_ID", by.y="Contract_ID", all.x=T)
loans$Interest_Due_Amount <- convertToNumeric(loans$Interest_Due_Amount)


### marge de tolerance pour definir les echeances OK: 0.1 USD, 1 THB, 100 KHR
loans$Interest_OK1 <- ifelse(
  ((loans$Loan_Currency_Code=='USD'& (loans$Interest_Paid_Amount+loans$Interest_Overdue_Amount-loans$Interest_Due_Amount>0.1))
   |(loans$Loan_Currency_Code=='THB'& (loans$Interest_Paid_Amount+loans$Interest_Overdue_Amount-loans$Interest_Due_Amount>1)) 
   |(loans$Loan_Currency_Code=='KHR'& (loans$Interest_Paid_Amount+loans$Interest_Overdue_Amount-loans$Interest_Due_Amount>100))),2,
  ifelse(
    ((loans$Loan_Currency_Code=='USD'& (loans$Interest_Paid_Amount+loans$Interest_Overdue_Amount-loans$Interest_Due_Amount< -0.1))
     |(loans$Loan_Currency_Code=='THB'& (loans$Interest_Paid_Amount+loans$Interest_Overdue_Amount-loans$Interest_Due_Amount< -1)) 
     |(loans$Loan_Currency_Code=='KHR'& (loans$Interest_Paid_Amount+loans$Interest_Overdue_Amount-loans$Interest_Due_Amount< -100))),0,1))

table(loans$Interest_OK1)

## 165,562 OK (84.3%) 6,921 with paid < due (3.5%), 23,812 with paid>due (12.1%)
mytable <- table(loans$Interest_OK1)
prop.table(mytable)



### Interest 2nd step
### we verify if there are some single instalments from older schedules (Version=2) exactly equal to the mismatch between paid and due
### selection of contracts: 26,533
loans_ko <- subset(loans, loans$Interest_OK1==2)

loans_ko$Mismatch <- loans_ko$Interest_Paid_Amount-loans_ko$Interest_Due_Amount
loans_ko <- subset(loans_ko, select=c("Loan_ID","Interest_Paid_Amount"
                                      ,"Interest_Overdue_Amount","Mismatch"))

loans_interest2 <- merge(loans_ko, schedules_loans_interest, 
                         by.x="Loan_ID", by.y="Contract_ID", all.x=T)


loans_interest2 <- subset(loans_interest2, loans_interest2$Version==1 
                          | loans_interest2$Interest_Due_Amount==loans_interest2$Mismatch)

loans_ko <- data.table(ddply(loans_interest2, .(Loan_ID, Loan_Currency_Code, Interest_Paid_Amount, Interest_Overdue_Amount), summarize, 
                             Interest_Due_Amount = sum(Interest_Due_Amount)
)) 

loans_ko$Interest_OK2 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount-loans_ko$Interest_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount-loans_ko$Interest_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount-loans_ko$Interest_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount-loans_ko$Interest_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount-loans_ko$Interest_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Interest_Overdue_Amount-loans_ko$Interest_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Loan_ID","Interest_OK2"))

loans <- merge(loans,loans_ko, all.x=T)
loans$Interest_OK2 <- convertToNumeric(loans$Interest_OK2)
loans$Interest_OK <-ifelse(loans$Interest_OK1==1 | loans$Interest_OK2==1,1,loans$Interest_OK1)
table(loans$Interest_OK)
## 169,167 OK (86.2%) 6,921 with paid < due (3.5%), 20,207 with paid>due (10.3%)
mytable <- table(loans$Interest_OK)
prop.table(mytable)

### Interest 3rd step
### for instalments with paid<due that were restructured
### (Loan_Restructuration_Flag=TRUE)
### we exclude older instalments with payment due dates after restructuration Date (Loan_Update_Date)
### selection of contracts: 6,391 
loans_ko <- subset(loans, loans$Interest_OK1==0 & loans$Loan_Restructuration_Flag==TRUE)

loans_ko <- subset(loans_ko, select=c("Loan_ID","Interest_Paid_Amount","Principal_Overdue_Amount"))


schedules_loans_restructured <- merge(loans_ko, schedules_loans_interest1,
                                      by.x="Loan_ID", by.y="Contract_ID", all.x=T)

schedules_loans_restructured <- subset(schedules_loans_restructured,
                                       (schedules_loans_restructured$Schedule_Update_Date<schedules_loans_restructured$Loan_Update_Date
                                        & schedules_loans_restructured$Payment_Due_Date<schedules_loans_restructured$Loan_Update_Date)  
                                       | schedules_loans_restructured$Schedule_Update_Date>=schedules_loans_restructured$Loan_Update_Date)

loans_ko <- data.table(ddply(schedules_loans_restructured, .(Loan_ID, Loan_Currency_Code, Interest_Paid_Amount, Principal_Overdue_Amount), summarize, 
                             Interest_Due_Amount = sum(Interest_Due_Amount)
))                    

loans_ko$Interest_OK3 <- ifelse(
  ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Interest_Due_Amount>0.1))
   |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Interest_Due_Amount>1)) 
   |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Interest_Due_Amount>100))),2,
  ifelse(
    ((loans_ko$Loan_Currency_Code=='USD'& (loans_ko$Interest_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Interest_Due_Amount< -0.1))
     |(loans_ko$Loan_Currency_Code=='THB'& (loans_ko$Interest_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Interest_Due_Amount< -1)) 
     |(loans_ko$Loan_Currency_Code=='KHR'& (loans_ko$Interest_Paid_Amount+loans_ko$Principal_Overdue_Amount-loans_ko$Interest_Due_Amount< -100))),0,1))

loans_ko <- subset(loans_ko, select=c("Loan_ID","Interest_OK3"))

loans <- merge(loans,loans_ko, all.x=T)
loans$Interest_OK3 <- convertToNumeric(loans$Interest_OK3)
loans$Interest_OK <-ifelse(loans$Interest_OK1==1 | loans$Interest_OK2==1 | loans$Interest_OK3==1,
                           1,loans$Interest_OK1)
table(loans$Interest_OK)
## 171,729 OK (87.5%) 4,359 with paid < due (2.2%), 20,207 with paid>due (10.3%)
mytable <- table(loans$Interest_OK)
prop.table(mytable)
