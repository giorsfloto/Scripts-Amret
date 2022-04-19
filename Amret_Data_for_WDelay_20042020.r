### Libraries ###
require(data.table)
require(tidyverse)
require(plyr)
require(bigrquery)
require(lubridate)
require(googleCloudStorageR)

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
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/bigquery",
                                        "https://www.googleapis.com/auth/cloud-platform",
                                        "https://www.googleapis.com/auth/devstorage.full_control",
                                        "https://www.googleapis.com/auth/gmail.compose"))
# the key should be uploaded in directory using upload button of Rstudio file explorer
googleAuthR::gar_auth_service(json_file = "amret-mf-kh-f06028117aef.json",


                                                                                          scope = getOption("googleAuthR.scopes.selected"))
gcs_global_bucket("amret_dev")
options("httr_oob_default" = TRUE)
project <- "248422820269" # put your project ID here (Amret)
bq_auth(path = "amret-mf-kh-f06028117aef.json")  ### json to allow bigrquery to query project Amret with 
#setwd("C:/Users/Giorgio/Documents/Rubyx/Advans/Amret/Data")
DateRun <- as.Date('2020-02-28')
nDaysAdv <- 20

### retrieve EOM exchange rates from GCS
gcs_get_object("Exchange_Rate_EOM.csv", saveToDisk = "Exchange_Rate_EOM.csv", overwrite=TRUE)
exchange_rate <- read.csv("Exchange_Rate_EOM.csv", header=T, stringsAsFactors=FALSE)
setnames(exchange_rate,old=c("ccyType","ReportDate"), new=c("Currency","Date"))
exchange_rate$Date<-as.Date(as.character(exchange_rate$Date),format="%Y%m%d")
## Year and Month to join data with sch and sav tables
exchange_rate$Year <- year(exchange_rate$Date)
exchange_rate$Month <- month(exchange_rate$Date)
## I delete obs double for year/month
exchange_rate <- subset(exchange_rate, !(exchange_rate$Date==as.Date('2019-04-01') |
                                         exchange_rate$Date==as.Date('2019-06-03')))

exchange_rate <- subset(exchange_rate, select=c(Currency, Year, Month, Mid_Reval_Rate))
### extract schedule and events from BQ (creating a fake Instalment_Number)
sch_eve_sql <- paste0('SELECT * EXCEPT (CID, P_D_Date)
FROM
(SELECT *
FROM `amret-mf-kh.amret_kh.schedule_instalments`) A
LEFT JOIN
(SELECT *, COUNT(*) OVER (PARTITION BY CID ORDER BY P_D_Date) AS Instalment_Number
FROM
(
SELECT Contract_ID AS CID, Payment_Due_Date P_D_Date
FROM `amret-mf-kh.amret_kh.schedule_instalments`
GROUP BY 1, 2)) B
ON A.Contract_ID=B.CID AND A.Payment_Due_Date=B.P_D_Date')

sch_eve <- query_exec(sch_eve_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)
setnames(sch_eve,"Instalment_Paid","Paid_Amount")

# Date format and other cleaning --------------------------------------------------
sch_eve <- as.data.table(sch_eve)
sch_eve[,Payment_Due_Date:=as.Date(Payment_Due_Date)]
sch_eve[,Event_Date:=as.Date(Event_Date)]


# Count how many days the event took place after or before the due date
sch_eve[,dDays:=as.numeric(Event_Date-Payment_Due_Date)] 

# Compute total payment up to this date
sch_eve[,tPaid_Amount:=cumsum(Paid_Amount),
      by=.(Customer_ID,Contract_ID,Currency,Payment_Due_Date,Instalment_Number,
           Amount_Due)]
# Compute the left to be paid and its lag
sch_eve[,cRemaining_Amount:=Amount_Due-tPaid_Amount]
sch_eve[,lRemaining_Amount:=shift(cRemaining_Amount),
      by=.(Customer_ID,Contract_ID,Currency, Instalment_Number)]
sch_eve[tPaid_Amount>Amount_Due, tPaid_Amount:=Amount_Due] 
# Compute partial weighted delays
sch_eve[,wDelay:=dDays*Paid_Amount/Amount_Due]
# Compute the delay for unpaid fraction till now
sch_eve[cRemaining_Amount>0,wDelay2:=wDelay+
           as.numeric(DateRun-Payment_Due_Date)*cRemaining_Amount/Amount_Due,
      by=.(Contract_ID,Instalment_Number)]
sch_eve[,one:=1]
sch_eve[order(Event_Date),EventNumber:=cumsum(one),
     by=.(Contract_ID,Instalment_Number)]
sch_eve[order(Event_Date),EventMax:=max(EventNumber),
     by=.(Contract_ID,Instalment_Number)]
sch_eve[EventNumber==EventMax & cRemaining_Amount>0,wDelay:=wDelay2]
# Trunked delay
sch_eve[,twDelay:=wDelay]
sch_eve[wDelay<0,twDelay:=0]

# Instalment aggregation ----------------------------------------------------------
sch1<-sch_eve[,.(wDelay=sum(twDelay)),
           by=.(Customer_ID,Contract_ID,Currency, Instalment_Number,Payment_Due_Date, 
                Amount_Due)]

sch1$wDelay <- round(sch1$wDelay,digits=4)				
				
# Atribute delay to instalments with no payment
sch1[is.na(wDelay) & Payment_Due_Date<DateRun,
     wDelay:=as.numeric((DateRun)-Payment_Due_Date)]

# Compute advance window
sch1[,Due_Date0:=Payment_Due_Date-nDaysAdv]
# Compute trunked delay
sch1[,twDelay:=wDelay]
sch1[wDelay<0,twDelay:=0]

# Compute weighted Advances ----------------------------------------------------------
### extract  last balance of the day for Account Type CA since 31/08/2018
sav_sql <- paste0("SELECT *
FROM
(
SELECT Customer_ID, Account_ID, Account_Type, Principal_Balance, Currency, 
Update_Date as Date
FROM `amret-mf-kh.amret_kh.account_balance` 
UNION ALL 
SELECT Customer_ID, Account_ID, Account_Type, Principal_Balance, Currency, 
CAST(Update_Date AS TIMESTAMP) as Date
FROM `amret-mf-kh.amret_kh.account_balance_20180831`)
WHERE Account_Type='CA'")

sav <- query_exec(sav_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

# Select customers/contract/instalment with no delay in Sav1
# tmp has instalments with no delay
tmp <-subset(sch1, sch1$wDelay==0)
tmp <- subset(tmp, select=c("Customer_ID","Contract_ID","Instalment_Number","Currency","Payment_Due_Date","Amount_Due","Due_Date0"))

# Get Savings from customers with instalments with no delay
sav <- subset(sav, sav$Customer_ID %in% tmp$Customer_ID)
sav <- as.data.table(sav)
sav[,Date:= as.Date(Date)]


# Create DT with account balances by date -----------------------
Dates<-seq.Date(from=as.Date('2018-08-31'),to=DateRun,by='day') ### we don't have august 2018 account balances so we can't calculate advances for September 2018 schedules
Accounts<-get("sav")[,unique(Account_ID)]

savDate<-data.table(Account_ID=rep(Accounts,each=length(Dates)),Date=rep(Dates,length(Accounts)))

savDate<-merge(savDate,get("sav")[,.(Customer_ID,Account_ID,Date, Currency, 
                                                   Principal_Balance)],
               by=c('Account_ID','Date'),all.x=T)
# Fill empty dates
savDate[,segment:=cumsum(!is.na(Principal_Balance))]
savDate[,Principal_Balance:=Principal_Balance[1],by=.(segment,Account_ID)]
savDate[, Customer_ID:= Customer_ID[1],by=.(segment,Account_ID)]
savDate[, Currency:= Currency[1],by=.(segment,Account_ID)]
### here we need to convert all balances in the same currency
savDate$Year=year(savDate$Date)
savDate$Month=month(savDate$Date)

savDate <- merge(savDate,exchange_rate,by=c("Currency","Year","Month"),all.x=T)
savDate <- subset(savDate, !is.na(savDate$Currency))
savDate$Principal_Balance_KHR <- ifelse(savDate$Currency=='KHR',savDate$Principal_Balance,
                                        savDate$Principal_Balance/savDate$Mid_Reval_Rate)

savDate<-savDate[,.(Acc_Balance=sum(Principal_Balance_KHR)),by=.(Customer_ID,Date)]
savDate$Acc_Balance <- ifelse(is.na(savDate$Acc_Balance),0,savDate$Acc_Balance)
# Create DT for savings balances and amount dues for future instalments
savd<-tmp[,.(Date=seq.Date(from=min(Due_Date0),to=max(Payment_Due_Date),by='day')),
          by=.(Customer_ID,Contract_ID,Instalment_Number,Currency, Due_Date0,Payment_Due_Date,Amount_Due)]
### here we need to convert all amount dues in the same currency		  
savd$Year=year(savd$Date)
savd$Month=month(savd$Date)
### conversion to KHR
savd <- merge(savd,exchange_rate,by=c("Currency","Year","Month"),all.x=T)
savd$Amount_Due_KHR <- ifelse(savd$Currency=='KHR',savd$Amount_Due,
                              savd$Amount_Due/savd$Mid_Reval_Rate)		  
savd[order(Payment_Due_Date,-Amount_Due_KHR),priority:=cumsum(!is.na(Amount_Due_KHR)),by=.(Date,Customer_ID)]

savd<-merge(savd,savDate,by.x=c('Date','Customer_ID'),by.y=c('Date','Customer_ID'),all.x=T)
savd<-savd[order(Customer_ID,Date)]
savd[is.na(Acc_Balance),Acc_Balance:=0]

savd[order(priority),cAmount_Due:=cumsum(Amount_Due_KHR),by=.(Customer_ID,Date)]
savd[order(Customer_ID,Date,priority),
     Acc_Balance_residual:=Acc_Balance-shift(cAmount_Due),by=.(Customer_ID,Date)]
savd[is.na(Acc_Balance_residual),Acc_Balance_residual:=Acc_Balance]
savd[Acc_Balance_residual<0,Acc_Balance_residual:=0]

savd[,pcDueCover:= Acc_Balance_residual/Amount_Due_KHR]
savd[pcDueCover<0,pcDueCover:=0]
savd[pcDueCover>1,pcDueCover:=1]
savd<-savd[Date-Due_Date0<=nDaysAdv,.(wAdvance=sum(pcDueCover)),by=.(Customer_ID,Due_Date0)]
savd[wAdvance>nDaysAdv,wAdvance:=nDaysAdv]

assign("sch1",merge(get("sch1"),
                                 savd,by=c('Customer_ID','Due_Date0'),all.x=T))
get("sch1")[!is.na(wAdvance) & wDelay==0,wDelay:=wDelay-wAdvance]
get("sch1")[wDelay>30,wDelay:=30]
get("sch1")[wDelay<(-nDaysAdv),wDelay:=-nDaysAdv]
get("sch1")[,twDelay:=wDelay]
get("sch1")[wDelay<0,twDelay:=0]
