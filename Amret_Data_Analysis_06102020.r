### Libraries ###
require(data.table)
require(tidyverse)
require(plyr)
require(bigrquery)
require(readxl)
require(lubridate)
library(googleCloudStorageR)  
library(Hmisc)
setwd("C:/Users/Giorgio/Documents/Rubyx/Advans/Amret")
options(scipen = 999) # to prevent scientific notation

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

### Parameters ###
FoldData   <-'C:/Users/Giorgio/Documents/Rubyx/Advans/Amret/Data/'
FoldResult <-'C:/Users/Giorgio/Documents/Rubyx/Advans/Amret/Data/Analysis'
options(googleAuthR.scopes.selected = c("https://www.googleapis.com/auth/bigquery",
                                        "https://www.googleapis.com/auth/cloud-platform",
                                        "https://www.googleapis.com/auth/devstorage.full_control",
                                        "https://www.googleapis.com/auth/gmail.compose"))
# the key should be uploaded in directory using upload button of Rstudio file explorer
googleAuthR::gar_auth_service(json_file = "amret-mfi-kh-6a0c9db85a56.json",
                                                                          scope = getOption("googleAuthR.scopes.selected"))
gcs_global_bucket("amret_kh_dev")
options("httr_oob_default" = TRUE)
project <- "885266346487" # put your project ID here
bq_auth(path = "amret-mfi-kh-6a0c9db85a56.json")  ### json to allow bigrquery to query project Amret KH

cus <-'amret_kh_cus_20200831090023'
acc <-'amret_kh_acc_20201012173640'
aba <-'amret_kh_aba_20200831090023'
loa <-'amret_kh_loa_20201019163514'
lba <-'amret_kh_lba_20200831090023'
sch <-'amret_kh_sch_20200831090023'
eve <-'amret_kh_eve_20200831090023'
txn <-'amret_kh_txn_20201008162223'
laa <-'amret_kh_laa_20201120140502'
lap <-'amret_kh_lap_20201120140248'
pfm <-'amret_kh_pfm_20201203171647'
dic <-'amret_kh_dic_20201203171647'
cur <-'amret_kh_cur_20201202154020'

############ Customers (2/10/2020)
##### Load data: 17,437 rows
{
#gcs_get_object(paste0('cus/',cus,'.csv'), saveToDisk = paste0(FoldData,cus,'.csv'), overwrite=TRUE)
customers <-data.table(read.csv(paste0(FoldData,cus,'.csv'),sep=';',
                                stringsAsFactors = FALSE))

length(unique(customers$Customer_ID))/length(customers$Customer_ID)  #### 1, IDs are unique: OK!
#length(unique(paste0(customers$Customer_ID,customers$Customer_Update_Date))) ### 1,244, not even ID/Update_Date are unique
#customer_ids <- data.table(ddply(customers, .(Customer_ID), summarize, 
#                                 rows = length(Customer_ID)
# )) 
#double_customer_ids <- subset(customer_ids, customer_ids$rows>1)


customers$Customer_Update_Date <- as.Date(customers$Customer_Update_Date)
customers$Customer_Creation_Date <- as.Date(customers$Customer_Creation_Date)
customers$Customer_ID <- as.character(customers$Customer_ID)
customers$Customer_Inputter_ID <- as.character(customers$Customer_Inputter_ID)
customers$PortfolioManager_ID <- as.character(customers$PortfolioManager_ID)
customers$Customer_Area_Code <- as.character(customers$Customer_Area_Code)
customers$Sector_Code <- as.character(customers$Sector_Code)
customers$Subsector_Code <- as.character(customers$Subsector_Code)

}
##### Descriptive Analysis
{
## Categorical variables

table(customers$Department_ID)             ### 
sum(is.na(customers$Department_ID))        ### 0 missing
length(unique(customers$Department_ID))    ### 145 different Departments
table(customers$Customer_Inputter_ID)           ### 
sum(is.na(customers$Customer_Inputter_ID)) ### 0 missing
length(unique(customers$Customer_Inputter_ID)) ### 722 different Customer_Inputter_IDs
table(customers$PortfolioManager_ID)           ### 
sum(is.na(customers$PortfolioManager_ID)) ### 0 missing
length(unique(customers$PortfolioManager_ID)) ### 305 different Customer_Inputter_IDs
table(customers$Customer_Area_Code)     ###  1  10  11  12  13  14  15  17  19   2  20  23  24  25  26  27  28  29   3  30  31  32  33  35   4   5 7 8
                                        ### 85  41 148  14  48  44  36  41  33  91  16  59  76 268  63  56 246  52   1 122 167  25  68  29  36  81 6 4  
table(customers$Gender_Code)                 ###  F    M 1987 2
table(customers$Sector_Code)            ### 100% 3000   
sum(is.na(customers$Sector_Code))       ### 0 
table(customers$Subsector_Code)         ### 1  10  11  12  13  14  15   2   3   4   5   6   7   8   9
                                        ### 4   8   4   1  27 146   2 979 312 356  99   3  38   6
sum(is.na(customers$Sector_Code))       ### 0 
table(customers$Marital_Status_Name)         ### DIVORCED   MARRIED     OTHER SEPARATED    SINGLE     WIDOW   7      1851         2         3        91        35  
table(customers$Customer_Category_Code)         
table(customers$Customer_Type_Code)     ###  I 2 NI 1987
table(customers$Customer_Status_Name)        ### Active Closed  Lapse 1919     34     36 
#table(customers$Banked_Previously_Flag)### 100% empty 
summary(customers$Customer_Birth_Year)  ### Min. 1st Qu.  Median    Mean 3rd Qu.    Max.
                                        ### 1959    1978    1983    1983    1989    2001
table(customers$Customer_Birth_Month)   ###   1   2   3   4   5   6   7   8   9  10  11  12
                                        ### 227 168 167 176 215 191 137 206 150 149  93 110

## Numerical/Date variables
summary(customers$Customer_Creation_Date) ###         Min.      1st Qu.       Median         Mean      3rd Qu.         Max.
                                          ### "2020-09-07" "2020-09-07" "2020-09-07" "2020-09-07" "2020-09-08" "2020-09-08"
summary(customers$Customer_Update_Date)   ###         Min.      1st Qu.       Median         Mean      3rd Qu.         Max.
                                          ### "2020-09-07" "2020-09-07" "2020-09-08" "2020-09-07" "2020-09-08" "2020-09-09"
sum(customers$Customer_Update_Date<customers$Creation_Date) ### 0
sum(customers$Customer_Birth_Year>=1915 & customers$Customer_Birth_Year<=as.integer(format(Sys.Date(), "%Y")))/length(customers$Customer_ID) 

## Check formats
sum(grepl('^[0-9]{1,7}$',  customers$Customer_ID))/length(customers$Customer_ID)         ### Customer_ID have up to 7 digits
#sum(grepl('^[0-9]{1,4}$',  customers$Inputter_ID))/sum(!(is.na(customers$Inputter_ID)))  ### Inputter_ID have up to 4 digits
sum(grepl('^[0-9]{1,4}$',  customers$PortfolioManager_ID))/length(customers$Customer_ID) ### PM_ID have up to 4 digits
sum(grepl('^[A-Z]{3}$',  customers$Department_ID))/length(customers$Customer_ID)  
sum(grepl('^[0-9]{1,8}$',  customers$Customer_Area_Code))/length(customers$Customer_ID)  
## Check coherency with other tables
#sum(!(events$Customer_ID %in% customers$Customer_ID))  ### 0: all customers in Schedules are in Customer
#sum(!(schedules$Customer_ID %in% customers$Customer_ID))  ### 0: all customers in Schedules are in Customer
#sum(!(loan_balances$Customer_ID %in% customers$Customer_ID))  ### all customers in Loan Balances are in Customer
#sum(!(accounts$Customer_ID %in% customers$Customer_ID))  ### all customers in Accounts are in Customer
#sum(!(account_balances$Customer_ID %in% customers$Customer_ID))  ### all customers in Account Balances are in Customer
}							

############ Accounts as of 12/10/2020
##### Load data: 1,000
{
gcs_get_object(paste0('acc/',acc,'.csv'), saveToDisk = paste0(FoldData,acc,'.csv'), overwrite=TRUE)
accounts <-data.table(read.csv(paste0(FoldData,acc,'.csv'),sep=';',
                                  stringsAsFactors = FALSE))
  
								
#setnames(accounts, old=c("Client.ID","DEPARTMENT.ID","ACCOUNT.INPUTTER.ID","PORTFOLIO.MANAGER.ID","ACCOUNT.CODE","ACCOUNT.CURRENCY.CODE",
#                         "ACCOUNT.ID","ACCOUNT.UPDATE.DATE","TRANSACTION.FIRST.DATE.TIME","ACCOUNT.START.DATE","ACCOUNT.END.DATE",
#						 "ACCOUNT.STATUS.NAME","ACCOUNT.CODE.TYPE"), 
#				   new=c("Customer_ID","Department_ID","Account_Inputter_ID","PortfolioManager_ID","Account_Code","Account_Currency_Code",
#                         "Account_ID","Account_Update_Date","Transaction_First_DateTime","Account_Start_Date","Account_End_Date",
#						 "Account_Status_Name","Account_Code_Type"))

accounts$Customer_ID <- as.character(accounts$Customer_ID)
accounts$Account_ID <- as.character(accounts$Account_ID)
accounts$Account_Inputter_ID <- as.character(accounts$Account_Inputter_ID)
accounts$PortfolioManager_ID <- as.character(accounts$PortfolioManager_ID)
accounts$Account_Code <- as.character(accounts$Account_Code)
accounts$Account_Update_Date <- as.Date(accounts$Account_Update_Date)
accounts$Account_Start_Date <- as.Date(accounts$Account_Start_Date)
accounts$Account_End_Date <- as.Date(accounts$Account_End_Date, format = "%Y-%m-%d")
accounts$Account_Maturity_Date <- as.Date(accounts$Account_Maturity_Date, format = "%Y-%m-%d")
#accounts$Transaction_First_DateTime <- parse_date_time(accounts$Transaction_First_DateTime,  orders="ymd HMS")

length(unique(accounts$Account_ID))/length(accounts$Account_ID)  #### #### 1: OK
#length(unique(paste0(accounts$Account_ID,accounts$Account_Code))) ### 17,190 not even by Account_ID/Account_Code
#length(unique(paste0(accounts$Account_ID,accounts$Account_Update_Date)))  ### 17,201, not even ID/Update_Date are unique

### count records by accounts
#account_counts <- data.table(ddply(accounts, .(Account_ID), summarize, 
#                                records = length(Account_ID)
#)) 
### accounts with several records
#accounts_duplicated <- subset(accounts,accounts$records>1) ## 
#sum(is.na(accounts$Account_ID))

#sum(!(accounts$Account_ID %in% account_balances$Account_ID))  ### 0 Account_IDs in accounts are not in account_balances: OK!!
}
##### Descriptive Analysis
{
## Categorical variables
table(accounts$Department_ID)      ###
sum(is.na(accounts$Department_ID))        ### 0 missing
length(unique(accounts$Department_ID))    ### 95 different Departments  
table(accounts$Account_Inputter_ID)  ###  
sum(is.na(accounts$Account_Inputter_ID))        ### 0 missing
length(unique(accounts$Account_Inputter_ID))  ### 612 different Inputter_IDs
table(accounts$Account_Channel_Name)  ### 100% BRANCH
table(accounts$PortfolioManager_ID)  
sum(is.na(accounts$PortfolioManager_ID))        ### 0 missing
length(unique(accounts$PortfolioManager_ID))  ### 481 different PortfolioManager_IDs
sum(accounts$Account_Inputter_ID==accounts$PortfolioManager_ID) ### 1,991 Account_Inputter_ID and Portfolio_Manager_ID are always equal
table(accounts$Account_Code)                 ### 6000 6100 6400 6403 6602 6700 
                                             ###  488    6  100  389   12    5 
table(accounts$Account_Currency_Code)        ### KHR THB USD
                                             ### 546   8 446
table(accounts$Account_Status_Name)          ###  ACTIVE  CLOSED DORMANT  
                                             ###    555       1     444
table(accounts$Account_Type_Code)            ###  CA  SA  TD
											 ### 877 106  17                                     
table(accounts$Account_Interest_Rate)        ### 0 0.01 0.03 0.04 0.06 0.07 
											 ### 488 389  75   40    4    4

#sum(accounts$Status=="Closed" & is.na(accounts$Closing_Date)) # 0 Closed accounts with Closing_Date empty

## Numerical/Date variables
summary(accounts$Account_Update_Date) # min 2020-09-30, max 2020-09-30
summary(accounts$Account_Start_Date) # min 2009-10-26, max 2020-09-30
sum(is.na(accounts$Account_Start_Date)) # 0
summary(accounts$Account_End_Date) # 99.9% NAS
summary(accounts$Account_Maturity_Date)# min 2020-11-28, max 2035-11-14, NA's 984
#summary(accounts$Transaction_First_DateTime)  # min 2020-09-07 00:40:01, max 2020-09-08 23:36:51
#sum(accounts$Account_Start_Date==as.Date(accounts$Transaction_First_DateTime)) ### Transaction_First_DateTime is always same date than Account_Start_Date
sum(is.na(accounts$Account_End_Date) & accounts$Account_Status_Name=="CLOSED") ### 0: OK
sum(is.na(accounts$Account_Maturity_Date) & accounts$Account_Type_Code=="TD") ### 1 over 17

## Check formats
sum(grepl('^[0-9]{1,8}$',  accounts$Account_ID))/length(accounts$Account_ID) ### Inputter_IDs have just up to 8 digits
#sum(grepl('^[0-9]{1,4}$',  accounts$Account_Inputter_ID))/length(accounts$Account_Inputter_ID) ### Inputter_IDs have just up to 4 digits
sum(grepl('^[0-9]{1,4}$',  accounts$PortfolioManager_ID))/length(accounts$PortfolioManager_ID) ### Portfolio_Manager_IDs have just up to 4 digits
sum(grepl('^[0-9]{1,7}$',  accounts$Customer_ID))/length(accounts$PortfolioManager_ID) ### Customer_IDs have up to 7 digits
sum(grepl('^[A-Z]{3}$',  accounts$Department_ID))/length(accounts$Account_ID)### Department_ID have 3 characters
sum(grepl('^[0-9]{4}$',  accounts$Account_Code))/length(accounts$Account_ID)### Account_Codes have 4 digits
sum(grepl('^[0][.][0-9]{1,4}$',  accounts$Account_Interest_Rate))/length(accounts$Account_ID)### Account_Codes are not float 0.1234
}

## Merging accounts and account_balances
{
#account_balances2 <- subset(account_balances, select=c(Account_ID, Principal_Balance, Interest_Balance, Unpaidfee_Balance, Real_Balance, Overdraft_Amount))

#accounts <- merge(accounts,account_balances2,by.x='Account_ID',by.y='Account_ID',all.x=T)
}

############ Account Balances
##### 2/10/2020 1,000 rows
{
#gcs_get_object(paste0('aba/',aba,'.csv'), saveToDisk = paste0(FoldData,aba,'.csv'), overwrite=TRUE)
account_balances <-data.table(read.csv(paste0(FoldData,aba,'.csv'),sep=';',
                                 stringsAsFactors = FALSE))

#setnames(account_balances, old=c("CUSTOMER_ID","ACCOUNT_CODE","ACCOUNT_ID","ACCOUNT_CURRENCY_CODE","ACCOUNT_BALANCE_DATE","CAPITAL_BALANCE_AMOUNT",
#                         "REAL_BALANCE_AMOUNT"), 
#				   new=c("Customer_ID","Account_Code","Account_ID","Account_Currency_Code","Account_Balance_Date","Capital_Balance_Amount",
#                         "Available_Balance_Amount"))
								
account_balances$Customer_ID <- as.character(account_balances$Customer_ID)
account_balances$Account_ID <- as.character(account_balances$Account_ID)
account_balances$Account_Code <- as.character(account_balances$Account_Code)
account_balances$Account_Balance_Date <- as.Date(account_balances$Account_Balance_Date)


length(unique(account_balances$Account_ID))/length(account_balances$Account_ID)  #### #### 1: OK!
length(unique(paste0(account_balances$Account_ID,account_balances$Account_Balance_Date)))  ### 1,263, not even ID/Update_Date are unique


### check if double ids excluding blanks
#account_balances_noblank <- subset(account_balances, !is.na(account_balances$Account_ID))
#print(paste0("% of unique non empty IDs in Account_Balance: ",length(unique(account_balances_noblank$Account_ID))/length(account_balances_noblank$Account_ID)*100,"%"))
### accounts with several records
#account_balances_duplicated <- subset(account_balances_counts,account_balances_counts$records>1) ## 
sum(is.na(account_balances$Account_ID)) ### 0:OK
sum(account_balances$Account_ID==account_balances$Customer_ID, na.rm=T) ## 0 Account IDs and Customer IDs are different:OK
length(unique(account_balances$Customer_ID)) ### 997 differents Customers
sum(is.na(account_balances$Customer_ID)) ### no missing customers
							
}
##### Descriptive Analysis
## Categorical variables
{
table(account_balances$Account_Code)           ###   3110   3111   3112   3113   3114   3115   3116   3126   3130
                                               ### 440518   8778   3494    493      1   3585      1     54      1
table(account_balances$Account_Currency_Code)  ### 100%  NGN

## Numerical/Date variables

summary(account_balances$Account_Balance_Date) # min 2020-09-01 00:00:00, max 2020-09-17 00:00:0, 0 NAs

summary(account_balances$Capital_Balance_Amount) # Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 0   11088   17616   20505   27156  998652 
quantile(account_balances$Capital_Balance_Amount, c(0,.1,.25,.5,.75,.9,1)) #  0.00   6000.00  11088.00  17615.96  27155.96  38097.26 998651.69 
print(paste0("% of 0 available amount balances in Account_Balance: ",sum(account_balances$Capital_Balance_Amount==0)/length(account_balances$Account_ID)*100,"%"))
summary(account_balances$Available_Balance_Amount) # Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 0    5174   11792   14681   20513  995826
quantile(account_balances$Available_Balance_Amount, c(0,.1,.25,.5,.75,.9,1)) # 0.00    704.00   5174.00  11792.04  20513.12  30938.79 995825.69 
print(paste0("% of 0 available amount balances in Account_Balance: ",sum(account_balances$Available_Balance_Amount==0)/length(account_balances$Account_ID)*100,"%"))
print(paste0("% of Accounts where Principal Balance = Available Balance: ",sum(account_balances$Capital_Balance_Amount==account_balances$Available_Balance_Amount)/length(account_balances$Account_ID)*100,"%")) # 85.7% Capital and Available Balance Amounts are always equal
account_balances$Diff_Balances <- account_balances$Capital_Balance_Amount-account_balances$Available_Balance_Amount
quantile(account_balances$Diff_Balances, c(0,.1,.25,.5,.75,.9,1)) #  0      0   2087   5217   8348  11478 500000 
sum(account_balances$Capital_Balance_Amount==account_balances$Available_Balance_Amount)/length(account_balances$Account_ID) ## 1 Available_Balance_Amount always equal to Capital_Balance_Amount
summary(account_balances$Interest_Balance_Amount)
summary(account_balances$Unpaidfee_Balance_Amount)

## Check formats
sum(grepl('^[0-9]{1,8}$',  account_balances$Account_ID))/length(account_balances$Account_ID) ### Account_ID have up to 8 digits
#sum(substr(account_balances$Account_ID,1,4)==account_balances$Account_Code)/length(account_balances$Account_ID) # first four digits of Account_ID are the Account_Code
sum(grepl('^[0-9]{1,7}$',  account_balances$Customer_ID))/length(account_balances$Account_ID) ## customers have up to 7 digits format
sum(grepl('^[0-9]{4}$',  account_balances$Account_Code))/length(account_balances$Account_ID) ## account codes have 4-digits format
sum(grepl('^[0-9]{1,8}[.][0-9]{2}$',  account_balances$Capital_Balance_Amount))/length(account_balances$Account_ID) ## 36% are actually float
sum(grepl('^[0-9]{1,8}[.][0-9]{2}$',  account_balances$Available_Balance_Amount))/length(account_balances$Account_ID) ## 36% are actually float
}

############ Loans (19/10/2020)
##### Load data: 1,000 rows
{
gcs_get_object(paste0('loa/',loa,'.csv'), saveToDisk = paste0(FoldData,loa,'.csv'), overwrite=TRUE)
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
loans$Restructuration_Covid_Flag <- as.character(loans$Restructuration_Covid_Flag)

length(unique(loans$Loan_ID))/length(loans$Loan_ID)  ### 1: IDs are unique
#length(unique(paste0(loans$Loan_ID,loans$Loan_Update_Date))) ## 801: not even the combinations ID/Update_Date are unique

#loans$Loan_Purpose_Code <- as.character(loans$Loan_Purpose_Code)
#loans$Loan_Process_Code <- as.character(loans$Loan_Process_Code)

loans$Loan_Update_Date <- as.Date(loans$Loan_Update_Date)
loans$Loan_Start_Date <- as.Date(loans$Loan_Start_Date)
loans$Loan_Maturity_Date <- as.Date(loans$Loan_Maturity_Date)
loans$Loan_End_Date <- as.Date(loans$Loan_End_Date, format = "%Y-%m-%d")
loans$Loan_Disbursement_Date <- as.Date(loans$Loan_Disbursement_Date)

#loans$Disbursement_Date <- parse_date_time(loans$Disbursement_Date,  orders="dmy HMS")
#loans$Maturity_Date <- parse_date_time(loans$Maturity_Date,  orders="dmy HMS")
#loans$Update_Date <- parse_date_time(loans$Update_Date,  orders="dmy HMS")
}
##### Descriptive Analysis
{
## Categorical variables
        ### 
sum(is.na(loans$Customer_ID))       ### No missing
table(loans$Department_ID)          ### 
sum(is.na(loans$Department_ID))        ### 0 missing
length(unique(loans$Department_ID))    ### 153 different Departments 
table(loans$Inputter_ID)             ### 
sum(is.na(loans$Inputter_ID))        ### 0 missing
print(paste0("% of Inputter IDs missing in Loan: ",round(sum(is.na(loans$Inputter_ID)) /length(loans$Loan_ID)*100,1),"%"))
length(unique(loans$Inputter_ID))    ### 119 unique
table(loans$Authoriser_ID)           ### 
sum(is.na(loans$Authoriser_ID))      ### 0 missing
print(paste0("% of Authoriser IDs missing in Loan: ",round(sum(is.na(loans$Authoriser_ID)) /length(loans$Loan_ID)*100,1),"%"))
length(unique(loans$Authoriser_ID))  ### 833 unique
print(paste0("% of Authoriser IDs=Inputter_ID in Loan: ",round(sum(loans$Inputter_ID==loans$Authoriser_ID, na.rm=T) /length(loans$Loan_ID)*100,1),"%"))
table(loans$Channel_Name)           ### 100% BRANCH
table(loans$PortfolioManager_ID)             ### 
sum(is.na(loans$PortfolioManager_ID))        ### 4 missing
print(paste0("% of PM IDs missing in Loan: ",round(sum(is.na(loans$PortfolioManager_ID)) /length(loans$Loan_ID)*100,1),"%"))
length(unique(loans$PortfolioManager_ID)) ### 725 unique PM_IDs
table(loans$Loan_Product_Code)        ### 21052 21053 21054 21056 21058 21059 21060    
                                      ###   317   107   205    42    52    81   196
table(loans$Loan_Purpose_Code)       
table(loans$Loan_Process_Code)       ### 100% 1
table(loans$Loan_Currency_Code)       ### KHR THB USD 228  32 740
table(loans$Installment_Frequency_Code)###  100% M
table(loans$Payment_Type_Name)        ###       ANNUITY   DECLINING    FLEXIBLE SEMI BALOON 
                                      ###   43       14         832         108           3    
table(loans$Loan_Restructuration_Flag)### 0.2% TRUE
table(loans$Restructuration_Covid_Flag) ### 0.2% TRUE
table(loans$Repayment_Account_ID)
sum(is.na(loans$Repayment_Account_ID)) ## 0
table(loans$Loan_Type_Code)           ###  100% LI
table(loans$Loan_Status_Name)        ###  ACTIVE 946  CLOSED 54
table(loans$Guarantors_IDs)
sum(loans$Guarantors_IDs=="") ## 52.5% missing
sum(is.na(loans$Guarantors_Number))  ## 52.5% missing
sum(is.na(loans$Guarantors_Number) & loans$Guarantor_IDs!="") ## 0 Guarantors_Number is missing so it is Guarantor_ID

## Numerical/Date variables
#summary(loans$Net_Disbursed_Amount) ##  Min. 1st Qu.  Median    Mean 3rd Qu.    Max. NA 4320   80000  100000  109279  120000 3500000    7022
#quantile(loans$Net_Disbursed_Amount, c(0,.1,.25,.5,.75,.9,1),na.rm=T) ####   4320   60000   80000  100000  120000  160000 3500000 
#print(paste0("% of missing Net Disbursement Amount in Loan: ",round(sum(is.na(loans$Net_Disbursed_Amount))/length(loans$Loan_ID)*100,1),"%"))
summary(loans$Capital_Disbursed_Amount) ##  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.  NA  3827   92000  115000  125375  138000 4200000    3750       12
quantile(loans$Capital_Disbursed_Amount, c(0,.1,.25,.5,.75,.9,1), na.rm=T) #### 3827   69000   92000  115000  138000  184000 4200000  
print(paste0("% of missing Capital Disbursement Amount in Loan: ",round(sum(is.na(loans$Capital_Disbursed_Amount))/length(loans$Loan_ID)*100,1),"%"))
#nocapital_amount <- subset(loans, is.na(loans$Capital_Disbursed_Amount))
#table(nocapital_amount$Loan_Status_Name)
#sum(loans$Capital_Disbursed_Amount==loans$Net_Disbursed_Amount, na.rm=T) ## 0
#sum(loans$Net_Disbursed_Amount>loans$Capital_Disbursed_Amount, na.rm=T) ## 0: OK
summary(loans$Instalments_Total_Number) #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.   18.00   36.00   48.00   47.37   48.00   84.00
sum(loans$Instalments_Total_Number==0) # 0
summary(loans$Loan_Cycle_Number)  #  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.  1.000   1.000   2.000   2.461   3.000   9.000
sum(loans$Loan_Cycle_Number==0) # 0
sum(loans$Loan_Cycle_Number>=1000) # 0
summary(loans$Loan_Interest_Rate) # Min. 1st Qu.  Median    Mean 3rd Qu.    Max.  0.0000  0.1400  0.1600  0.1536  0.1600  0.1800 
table(loans$Loan_Interest_Rate)      
sum(is.na(loans$Loan_Interest_Rate))
#table(loans$Effective_Interest_Rate)  ### 0.08  0.12  0.13  0.15  0.16  0.18   0.2  0.23  0.25  0.26  0.27   0.3  0.31
                                      ###  33  2691   379 49632     2     1    19     1   110     6     1    40     3
#sum(loans$Loan_Interest_Rate==loans$Effective_Interest_Rate) ### 41 equals
#sum(loans$Loan_Interest_Rate>=loans$Effective_Interest_Rate)
#print(paste0("% of nominal rates > to nominal rates in Loan: ",round(sum(loans$Loan_Interest_Rate>loans$Effective_Interest_Rate) /length(loans$Loan_ID)*100,1),"%"))

summary(loans$Loan_Disbursement_Date) # min 2019-03-28, max 2019-05-31
summary(loans$Loan_Maturity_Date) # min 2020-01-06, max 2026-06-25 
summary(loans$Loan_Update_Date)  # min. 2020-09-30
summary(loans$Loan_Start_Date)  # min 2019-03-28, max 2019-05-31
sum(loans$Loan_Disbursement_Date<loans$Loan_Start_Date) # 0: OK
loans$App_Days <- loans$Loan_Disbursement_Date-loans$Loan_Start_Date
table(loans$App_Days) ###   100% 0  
table(loans$Loan_End_Date) # min. 2020-09-04 max.2020-10-16 52,913 NAsprint(paste0("% of Closed with Loan End Date in Loan: ",round(sum(loans$Loan_Status_Name=='Closed') /sum(!(is.na(loans$Loan_End_Date)))*100,1),"%"))
sum(loans$Loan_Status=='CLOSED' & is.na(loans$Loan_End_Date))
summary(loans$Guarantors_Number)  ###  Min. 1st Qu.  Median    Mean 3rd Qu.    Max.    NA's 
                                  ###   1       1       1       1       1       1     648 
sum(loans$Loan_ID==loans$Customer_ID) # 0: OK
## Check formats
sum(grepl('^[L][D][0-9]{10}$',  loans$Loan_ID))/length(loans$Loan_ID) ## Loan_ID is LD123456790
sum(grepl('^[0-9]{1,7}$',  loans$Customer_ID))/length(loans$Loan_ID) ### Customer_IDs have just up to 5 digits
sum(grepl('^[A-Z]{3}$',  loans$Department_ID))/length(loans$Loan_ID) ### Department_IDs have format with 3 characters
sum(grepl('^[0-9]{1,4}$',  loans$PortfolioManager_ID))/length(loans$Loan_ID) ### PM_IDs have just up to 4 digits
sum(grepl('^[0-9]{1,6}$',  loans$Loan_Purpose_Code))/length(loans$Loan_ID) ### Loan_Purpose_Code have up to 6 digits
sum(grepl('^[0-9]{1,8}[.][0-9]{2}$',  loans$Capital_Disbursement_Amount))/length(loans$Loan_ID) ### Loan_Purpose_Code have up to 6 digits
sum(grepl('^[0-9]{1,8}$',  loans$Repayment_Account_ID))/length(loans$Loan_ID) ### Account_ID have format 123-1234567
}

############ Loans Balances 2/10/2020
##### Load data: 1,000
{
#gcs_get_object(paste0('lba/',lba,'.csv'), saveToDisk = paste0(FoldData,lba,'.csv'), overwrite=TRUE)
loan_balances <-data.table(read.csv(paste0(FoldData,lba,'.csv'),sep=';',
                              stringsAsFactors = FALSE))
								
loan_balances$Loan_ID <- as.character(loan_balances$Loan_ID)
loan_balances$Customer_ID <- as.character(loan_balances$Customer_ID)
loan_balances$Event_Currency_Code <- as.character(loan_balances$Event_Currency_Code)
loan_balances$Event_Code <- as.character(loan_balances$Event_Currency_Code)
loan_balances$Loan_Balance_Date <- as.Date(loan_balances$Loan_Balance_Date)

length(unique(loan_balances$Loan_ID))/length(loan_balances$Loan_ID)  ## 1:unique Loan_IDs
length(unique(paste0(loan_balances$Loan_ID,loan_balances$Loan_Balance_Date)))/length(loan_balances$Loan_ID) ## 1
### check dobule id/dates
#loan_dates <- data.table(ddply(loan_balances, .(Loan_ID, Loan_Balance_Date), summarize, 
#                                 rows = length(Loan_ID)
#)) 
#double_loan_dates <- subset(loan_dates, loan_dates$rows>1)
#double_loan_dates <- merge(double_loan_dates,loan_balances,all.x=T)
}
##### Descriptive Analysis
{
## Categorical variables
sum(is.na(loan_balances$Customer_ID))      ### 0
table(loan_balances$Loan_Product_Code)     ### 100% 3110
table(loan_balances$LoanBalance_Currency_Code)   ### 100% NGN
table(loan_balances$Event_Code) 
table(loan_balances$Loan_Status_Name)      ### 100% Active
summary(loan_balances$Days_Overdue_Number, na.rm=T)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       3       7       9.12       17.25       22 
sum(loan_balances$Principal_Overdue_Amount>0 & loan_balances$Days_Overdue_Number==0) # 0 
sum(loan_balances$Principal_Overdue_Amount==0 & loan_balances$Interest_Overdue_Amount==0 
    & loan_balances$Penalty_Overdue_Amount==0 & loan_balances$Days_Overdue_Number>0) ## 0

 # Numerical/Date variables
active <- subset(loan_balances, loan_balances$Loan_Status=="ACTIVE")
summary(active$Principal_Outstanding_Amount) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  33150   61200   80750   74120   85000  110500
summary(active$Principal_Overdue_Amount)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       0       0       0       0 
summary(active$Interest_Outstanding_Amount) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  5850   10800   14250   13080   15000   19500 
summary(active$Interest_Overdue_Amount)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       0       0       0       0
summary(active$Penalty_Overdue_Amount)           #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       0       0       0       0
summary(active$Days_Overdue_Number)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       0       0       0       0 
quantile(active$Days_Overdue_Number, c(0,.1,.25,.5,.75,.9,1), na.rm=T) #### 0    0    0    0    0    0  208

closed <- subset(loan_balances, loan_balances$Loan_Status=="CLOSE")
summary(closed$Principal_Outstanding_Amount, na.rm=T) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.   8500   22950   63750   55675   86913   97750
summary(closed$Principal_Overdue_Amount, na.rm=T)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.   0       0    8500    8986   14450   25500
summary(closed$Interest_Outstanding_Amount, na.rm=T) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  1500    4050   11250    9825   15338   17250 
summary(closed$Interest_Overdue_Amount, na.rm=T)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0    1500    1586    2550    4500 
summary(closed$Penalty_Overdue_Amount, na.rm=T)           #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       0       0       0       0 
summary(closed$Days_Overdue_Number, na.rm=T)         #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       2       2       3       6

summary(loan_balances$Loan_Balance_Date)
}

## Check formats
sum(grepl('^[L][D][0-9]{10}$',  loan_balances$Loan_ID))/length(loan_balances$Loan_ID) ## Loan_ID is LD123456790
sum(grepl('^[0-9]{5}$',  loan_balances$Loan_Product_Code))/length(loan_balances$Loan_ID)
sum(grepl('^[0-9]{1,7}$',  loan_balances$Customer_ID))/length(loan_balances$Loan_ID) ### Loan_IDs have format with 17 digits
#### check loans vs. schedules vs. events
{

sum(!(events$Contract_ID %in% loans$Loan_ID)) ### all loans in Events are in Loans..
sum(!(schedules$Contract_ID %in% loans$Loan_ID)) ### ... but 7,781 loans in Schedules are not in Loans
sum(!(loan_balances$Contract_ID %in% loans$Loan_ID)) ### all loans in Loan Balances are in Loans

#check <- subset(schedules,!(schedules$Contract_ID %in% loans$Loan_ID))
#sum((ko4$Contract_ID %in% check$Contract_ID)) ### 1,956 not reconciled instalments are from contracts in loans extraction (1.1%)
}

### check of Repayment_Account_ID
{
sum(!(loans$Repayment_Account_ID %in% accounts$Account_ID)) ### all repayments accounts are in Accounts
sum(!(loans$Repayment_Account_ID %in% account_balances$Account_ID))  ### all repayments accounts are in Account_Balances
repayment_accounts <- as.data.frame(unique(loans$Repayment_Account_ID))
setnames(repayment_accounts, old=c("unique(loans$Repayment_Account_ID)"), new=c("Account_ID"))
repayment_accounts$Account_ID <- as.character(repayment_accounts$Account_ID)

repayment_accounts <- merge(repayment_accounts, accounts, by.x='Account_ID',by.y='Account_ID',all.x=T)
table(repayment_accounts$Account_Type) #### repayment accounts are almost all CA  CA    SA 32093     1 
}

############ Schedules 2/10
##### Load data: 340,223 rows

{
#gcs_get_object(paste0('sch/',sch,'.csv'), saveToDisk = paste0(FoldData,sch,'.csv'), overwrite=TRUE)
schedules <-data.table(read.csv(paste0(FoldData,sch,'.csv'),sep=';',
                                      stringsAsFactors = FALSE))
schedules$Customer_ID <- as.character(schedules$Customer_ID)
##### patch to fix wrong date formats since 9/6 extraction
schedules$Payment_Due_Date <- as.Date(schedules$Payment_Due_Date)
schedules$Schedule_Update_Date <- as.Date(schedules$Schedule_Update_Date)
}

##### Descriptive Analysis
{
## Categorical variables
table(schedules$Department_ID)
sum(is.na(schedules$Customer_ID))          ### 0
sum(is.na(schedules$Contract_ID))          ### 0
table(schedules$Contract_Type)           ### 100% LOAN
table(schedules$Payment_Frequency)         ### 100% RECURRING
table(schedules$Frequency)                 ### 100% M
table(schedules$Schedule_Currency_Code)                  ### KHR THB USD 270  35 695 
table(schedules$Instalment_Status)         ### Satisfied     Unsatisfied 317198       23025  


## Numerical/Date variables
summary(schedules$Total_Due_Amount)  # Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  
quantile(schedules$Total_Due_Amount, c(0,.1,.25,.5,.75,.9,1))  #  
sum(schedules$Total_Due_Amount==0)  ## 0 rows with Total_Due_Amount 0
summary(schedules$Capital_Due_Amount) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 
quantile(schedules$Capital_Due_Amount, c(0,.1,.25,.5,.75,.9,1), na.rm=T) #   
sum(schedules$Capital_Due_Amount==0)  ## 73 rows with Capital Amount Due 0
summary(schedules$Interest_Due_Amount)#  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  
quantile(schedules$Interest_Due_Amount, c(0,.1,.25,.5,.75,.9,1), na.rm=T) #   
sum(schedules$Interest_Due_Amount==0)  ## 6 rows with Interest Amount Due 0
#summary(schedules$Instalment_Sequende_Number) # Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
#hist(schedules$Instalment_Sequende_Number, col="green")
sum(round((schedules$Capital_Due_Amount+schedules$Interest_Due_Amount),3)
    !=round(schedules$Total_Due_Amount,3)) ### ok we have to round to 3 digits, but Captil+Interest=Total Amounts
summary(schedules$Payment_Due_Date) # min 2017-04-10, max 2025-12-23
summary(schedules$Schedule_Update_Date) # 100% 2020-03-31

## Check formats
sum(grepl('^[L][D][0-9]{10}$',  schedules$Contract_ID))/length(schedules$Contract_ID) ### 100% Contract_IDs are LD0123456789
sum(grepl('^[0-9]{1,7}$',  schedules$Customer_ID))/length(schedules$Contract_ID)      ### 100% with up to 7 digits
sum(grepl('^[A-Z]{3}$',  schedules$Department_ID))/length(schedules$Contract_ID)      ### 100% with 3 characters
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  schedules$Total_Due_Amount))/length(schedules$Contract_ID) ### 61.5% some are integers
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  schedules$Capital_Due_Amount))/length(schedules$Contract_ID) ### 8.3%
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  schedules$Interest_Due_Amount))/length(schedules$Contract_ID) ###65.8%
#length(unique(paste0(schedules$Contract_ID,schedules$Instalment_Number,sep=''))) # 340,223 unique contracts/instalments: OK!
}
##### Check for missing instalments
{
#schedules <- schedules[order(schedules$Contract_ID, schedules$Instalment_Number)]
#schedules$Rows <- 1
#schedules$Counter <- unlist(tapply(schedules$Rows, schedules$Contract_ID, cumsum)) ### counter by Contract increasing with Instalment Number 
#schedules <- subset(schedules, select=-Rows)                                       ### drop 1 variable used to calculate the counter

#firstinstalments <- ddply(schedules,~Contract_ID,summarise,First_Instalment=min(Instalment_Number)) ### for each contract I calculate the first Installment number

#schedules <- merge(schedules,firstinstalments,by.x='Contract_ID',by.y='Contract_ID',all.x=T)
#schedules$Instalment_Counter <- schedules$First_Instalment+schedules$Counter-1     ### if there are no missing instalments, Instalment Number must be = First Instalment + counter -1
#sum(schedules$Instalment_Number!=schedules$Instalment_Counter)                     ### 21 
#sum(abs(schedules$Amount_Due-schedules$Capital_Amount_Due-schedules$Interest_Amount_Due)>=0.1) ## OK: Amount_Due=Capital_Amount_Due+Interest_Amount_Due
#schedules <- subset(schedules, select=-c(Counter, Instalment_Counter,First_Instalment))
}


############ Events (2/10)
##### Load data: 1,000
{
gcs_get_object(paste0('eve/',eve,'.csv'), saveToDisk = paste0(FoldData,eve,'.csv'), overwrite=TRUE)
events <-data.table(read.csv(paste0(FoldData,eve,'.csv'),sep=';',
                                  stringsAsFactors = FALSE))
								
##### patch to fix wrong date formats on 29/05 extraction
events$Payment_Due_Date <- as.Date(events$Payment_Due_Date)
events$Event_DateTime <- parse_date_time(events$Event_DateTime,  orders="ymd HMS")
events$Department_ID <- as.character(events$Department)
events$Customer_ID <- as.character(events$Customer_ID)
events$Transaction_ID <- as.character(events$Transaction_ID)
}
								
##### Descriptive Analysis
{
table(events$Department_ID)                ### No empty values
table(events$Contract_Type)             ### 100% LOAN
table(events$Event_Code)      
table(events$Event_Currency_Code)                  	        ### Capital  Interest Penalties 271377    283165     86084  (0.4236122 0.4420130 0.1343748)
table(events$Early_Termination_Flag)         ### N Y 448298 192328     (0.6997812 0.3002188)
sum(is.na(events$Transaction_ID))
length(unique(events$Transaction_ID))  ### 862 some transactions are on several lines
sum(is.na(events$Total_Paid_Amount))   # 0
sum(is.na(events$Capital_Paid_Amount)) # 0
sum(is.na(events$Interest_Paid_Amount))# 0
sum(is.na(events$Penalty_Paid_Amount)) # 0
sum(round((events$Capital_Paid_Amount+events$Interest_Paid_Amount+events$Penalty_Paid_Amount),3)
    !=round(events$Total_Paid_Amount,3)) # 0

#sum(events$Event=="EP" & events$Payment_Due_Date<events$Event_Date) # 0 all early payments happen before payment date
#sum(events$Remaining_Amount>0 & events$Event=="EP" & events$Payment_Due_Date>=events$Event_Date) # 0 early payments on instalments still to be fully paid
length(unique(events$Contract_ID)) # 891
length(unique(paste0(events$Contract_ID,events$Transaction_ID))) # 28,474 unique contracts/Early_Termination OK
#length(unique(events$Contract_ID[events$Early_Termination=="Y"]))/length(unique(events$Contract_ID)) ## 24.2% of contracts are early terminations
#length(unique(paste0(events$Contract_ID,events$Instalment_Number,events$Payment_Type,events$Transaction_ID,sep=' '))) ## unique key is Contract_ID/Instalment_Number/Payment_Type/Transaction_ID
#sum(events$Event=='R' & events$Event_Date!=events$Payment_Due_Date) # 0 all regular payments have Event_Date=Payment_Due_Date
#sum(events$Event=='EP' & events$Event_Date>=events$Payment_Due_Date) # 0 early payments on time have Event_Date>=Payment_Due_Date
#sum(events$Event=='LP' & events$Event_Date<=events$Payment_Due_Date) # 0 late payments on time have Event_Date<=Payment_Due_Date

## Numerical/Date variables
#summary(events$Payment_Due_Date)    #  Min. 2015-08-03 00:00:00, Max 2021-09-20 00:00:00 
#early <- subset(events, events$Payment_Due_Date>events$Event_Date)
#table(early$Payment_Type)           #  Capital  Interest Penalties   28630     28090        10 How do you pay penalties in advance?
#table(early$Event)                  # 
summary(events$Event_DateTime)          #  Min. 2020-08-31, Max 2020-08-31
summary(events$Amount_Due)          # Min.   1st Qu.    Median      Mean   3rd Qu.      Max. 0.004   293.939   422.882   814.927   715.696 29174.471 
quantile(events$Amount_Due, c(0,.1,.25,.5,.75,.9,1)) #  0.004   196.062   293.939   422.882   715.696  1410.211 29174.471 
#sum(events$Amount_Due==0)           # 0 
#summary(events$Total_Paid_Amount)   # Min.  1st Qu.   Median     Mean  3rd Qu.     Max. 0.00    17.95   119.63   204.61   277.98 18933.42 
#quantile(events$Total_Paid_Amount, c(0,.1,.25,.5,.75,.9,1)) # 0% 10% 25% 50% 75% 90% 100%    0.000     0.546    17.952   119.628   277.978   476.112 18933.422 
#sum(events$Total_Paid_Amount==0)    # 37,681 (5.9%) rows with Event_Paid_Amount 0 !!
#summary(events$Remaining_Amount)    # 
#quantile(events$Remaining_Amount, c(0,.1,.25,.5,.75,.9,1)) # 0% 10% 25% 50% 75% 90% 100% 0.000    0.000    0.000    0.000    0.000    0.000 6142.018 
#sum(events$Remaining_Amount==0)     # 636,207 rows with Remaining_Amount 0 (98.5%)

## Check formats
sum(grepl('^[L][D][0-9]{10}$',  events$Contract_ID))/length(events$Contract_ID) ### 100% Contract_IDs are LD0123456789
sum(grepl('^[0-9]{1,7}$',  events$Customer_ID))/length(events$Contract_ID)      ### 100% with up to 7 digits
sum(grepl('^[A-Z]{3}$',  events$Department_ID))/length(events$Contract_ID)      ### 100% with 3 characters
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  events$Total_Paid_Amount))/length(events$Contract_ID) ### 61.5% some are integers
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  events$Capital_Paid_Amount))/length(events$Contract_ID) ### 8.3%
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  events$Interest_Paid_Amount))/length(events$Contract_ID) ###65.8%
sum(grepl('^[0-9]{1,12}[.][0-9]{2}$',  events$Penalty_Paid_Amount))/length(events$Contract_ID)
## repeated transactions
events_txns <- data.table(ddply(events, .(Transaction_ID), summarize, 
                                records = length(Contract_ID)
)) 

repeated_txns <- subset(events_txns, events_txns$records>1)

check <- subset(events, events$Transaction_ID %in% repeated_txns$Transaction_ID)
}

##### Sum of payments by instalment
{
##### check of payments vs. schedules between 2018-10-01 and 2020-03-31
sch <- subset(schedules, schedules$Payment_Due_Date>='2018-10-01' & schedules$Payment_Due_Date<='2020-03-31') ### instalments Oct 2018-Mar 2020: 262,454
### 262,454 contacts/instalments
sch <- subset(sch, select=c("Contract_ID","Instalment_Number","Payment_Due_Date","Amount_Due","Capital_Amount_Due","Interest_Amount_Due","Frequency"))

events_subset <- subset(events, events$Payment_Due_Date>=as.Date('2018-10-01') 
					& events$Payment_Due_Date<=as.Date('2020-03-31')) ### payments with due date 1st quarter 2020: 620,420
					
### check payments not in schedules
sch_eve_full<-merge(events_subset[,.(Contract_ID,Instalment_Number,Payment_Due_Date,Event_Date, Payment_Type, Early_Termination)],
							sch[,.(Contract_ID,Instalment_Number,Payment_Due_Date,Frequency)],
								by=c('Contract_ID','Instalment_Number'),all=T)					
								
### 6631 payments not in schedules (1.07%)
payments_wo_schedules <- sch_eve_full[is.na(Frequency),]

#### 100% Penalties et sauf 149 Early_Termination='Y'
table(payments_wo_schedules$Payment_Type,payments_wo_schedules$Early_Termination)

payments_wo_schedules <- subset(payments_wo_schedules, select=-Payment_Due_Date.y)
setnames(payments_wo_schedules, "Payment_Due_Date.x", "Payment_Due_Date")
write.csv(payments_wo_schedules, "6631paymentsnotinschedules.csv", row.names=F)

#### 640,652 payments
eve <- subset(events, events$Event_Date>='2018-10-01' & events$Event_Date<'2020-04-01') 
eve$Event_Date <- as.Date(eve$Event_Date)
#### 322,963 rows
events_date <- data.table(ddply(eve, .(Contract_ID, Instalment_Number,Event_Date), summarize, 
                                               Capital_Paid = sum(Event_Paid_Amount[Payment_Type == "Capital"]),
											   Interest_Paid = sum(Event_Paid_Amount[Payment_Type == "Interest"])
)) 
#### 324,512 rows
sch_eve <- merge(sch,events_date, by=c("Contract_ID", "Instalment_Number"),all.x=T)

sch <- subset(loan_balances, select=c("Loan_ID","Principal_Due","Interests_Due","Loan_Status","Days_Overdue"))

setnames(sch, old=c("Loan_ID","Principal_Due","Interests_Due"), new=c("Contract_ID","Capital_Past_Due","Interest_Past_Due"))

sch_eve_sch <- merge(sch_eve,sch, by=c("Contract_ID"),all.x=T)


#### cleaning of Contract_ID because of two trailing empty spaces in events extraction
#events_instalments$Contract_ID <- substr(events_instalments$Contract_ID,1,13)
## we assume that if an interest instalment is paid early and the loan is closed at 31/3, paid Amount_Due is equal to due Amount_Due.
## this assumption can make calculations of interests paid wrong for few cases of several instalment payments of which at least one in advance (around 65 cases)

sch_eve_sch$Interest_Paid <- ifelse(sch_eve_sch$Event_Date<sch_eve_sch$Payment_Due_Date 
                                    & sch_eve_sch$Interest_Paid<sch_eve_sch$Interest_Amount_Due
                                     & sch_eve_sch$Loan_Status=='Closed', 
                                       sch_eve_sch$Interest_Amount_Due, sch_eve_sch$Interest_Paid)

sch_eve_sch$Capital_Paid <- ifelse(is.na(sch_eve_sch$Capital_Paid),0,sch_eve_sch$Capital_Paid)
sch_eve_sch$Interest_Paid <- ifelse(is.na(sch_eve_sch$Interest_Paid),0,sch_eve_sch$Interest_Paid)
sch_eve_sch$Capital_Past_Due <- ifelse(is.na(sch_eve_sch$Capital_Past_Due),0,sch_eve_sch$Capital_Past_Due)
sch_eve_sch$Interest_Past_Due <- ifelse(is.na(sch_eve_sch$Interest_Past_Due),0,sch_eve_sch$Interest_Past_Due)
sch_eve_sch$Amount_Paid <- sch_eve_sch$Capital_Paid+sch_eve_sch$Interest_Paid
sch_eve_sch$Total_Past_Due <- sch_eve_sch$Capital_Past_Due+sch_eve_sch$Interest_Past_Due

sch_pay_sch <- data.table(ddply(sch_eve_sch, .(Contract_ID, Instalment_Number,Payment_Due_Date, Amount_Due, Total_Past_Due, Loan_Status, Days_Overdue), summarize, 
                                               Amount_Paid = sum(Amount_Paid)))
			
sch_pay_sch$paid <- ifelse(abs(sch_pay_sch$Amount_Due-sch_pay_sch$Amount_Paid)>0.1,0,1)
sum(sch_pay_sch$paid) # 239,400 installments fully paid
sum(sch_pay_sch$paid)/length(sch_pay_sch$Contract_ID) # 91.2%

sch_not_paid <- subset(sch_pay_sch, sch_pay_sch$paid==0) ## 23,054 installments not full paid

contracts_not_paid <- data.table(ddply(sch_not_paid, .(Contract_ID, Loan_Status, Total_Past_Due), summarize, 
                                               Amount_Due  = sum(Amount_Due),
											   Amount_Paid = sum(Amount_Paid)))  ## for 8,659 contracts

contracts_not_paid$contract_ok <- ifelse(abs(contracts_not_paid$Amount_Due-contracts_not_paid$Amount_Paid-contracts_not_paid$Total_Past_Due)>0.1,0,1)	
## 6,216 are contracts in past due

contracts_not_paid <- subset(contracts_not_paid, select=c("Contract_ID","contract_ok"))

sch_pay_sch <- merge(sch_pay_sch,contracts_not_paid, all.x=T)							   
											   
sch_pay_sch$pay_ok <- ifelse(sch_pay_sch$paid==1 | sch_pay_sch$contract_ok==1, 1,0)
							  
sum(sch_pay_sch$pay_ok) # 249,970 installments with total due = total paid + total past due(contract)
sum(sch_pay_sch$pay_ok)/length(sch_pay_sch$Contract_ID) # 95.2%

sch_pay_sch_ko <- subset(sch_pay_sch, sch_pay_sch$pay_ok==0)

table(sch_pay_sch_ko$Loan_Status)							  

sum(!(events$Contract_ID %in% schedules$Contract_ID))
			
}
###### check coherency between loan_balances, schedules and events
sch_03 <- subset(loan_balances, select=c(Loan_ID, Loan_Status, Principal_Outstanding, Principal_Due, Interests_Outstanding, Interests_Due, Days_Overdue))
sch_02 <- subset(loan_balances_feb, select=c(Loan_ID, Loan_Status, Principal_Outstanding, Principal_Due, Interests_Outstanding, Interests_Due, Days_Overdue))
setnames(sch_02, old=c("Loan_Status","Principal_Outstanding","Principal_Due","Interests_Outstanding","Interests_Due","Days_Overdue"), 
new=c("Loan_Status_Start","Principal_Outstanding_Start","Principal_Due_Start","Interests_Outstanding_Start","Interests_Due_Start","Days_Overdue_Start"))
sch_02$Total_Outstanding_Start <- sch_02$Principal_Outstanding_Start+sch_02$Interests_Outstanding_Start
sch_03$Total_Outstanding <- sch_03$Principal_Outstanding+sch_03$Interests_Outstanding

sch <- merge(sch_02,sch_03, all=TRUE)
sch$Total_Outstanding_Start <- ifelse(is.na(sch$Total_Outstanding_Start),0,sch$Total_Outstanding_Start)
sch$Total_Outstanding <- ifelse(is.na(sch$Total_Outstanding),0,sch$Total_Outstanding)


sch_03 <- subset(schedules, as.Date(schedules$Payment_Due_Date)>=as.Date('2020-03-01'))
sch_03 <- subset(sch_03, select=c(Contract_ID, Amount_Due, Capital_Amount_Due, Interest_Amount_Due))
setnames(sch_03, old=c("Contract_ID"), new=c("Loan_ID"))

sch <- merge(sch, sch_03, by.x="Loan_ID", by.y="Loan_ID", all=T)
sch$Amount_Due <- ifelse(is.na(sch$Amount_Due),0,sch$Amount_Due)

eve_03 <- subset(events, events$Event_Date>='2020-03-01' & events$Event_Date<='2020-03-31')

eve_03 <- data.table(ddply(eve_03, .(Contract_ID), summarize, 
                                               Capital_Paid = sum(Event_Paid_Amount[Payment_Type == "Capital"]),
											   Interest_Paid = sum(Event_Paid_Amount[Payment_Type == "Interest"])))
											   
eve_03$Amount_Paid <- eve_03$Capital_Paid+eve_03$Interest_Paid											 
											 
setnames(eve_03, old=c("Contract_ID"), new=c("Loan_ID"))											   

sch <- merge(sch, eve_03, by.x="Loan_ID", by.y="Loan_ID", all=T)
sch$Amount_Paid <- ifelse(is.na(sch$Amount_Paid),0,sch$Amount_Paid)

disb_date <- subset(loans, select=c(Loan_ID, Disbursement_Date))
sch <- merge(sch,disb_date, all.x=TRUE)

sch$check <- sch$Total_Outstanding_Start-sch$Amount_Paid-sch$Total_Outstanding
### 361 contracts disbursed before 1/3/2020 where C+I Outstanding End of Feb - March Payments <> C+I Outstanding End of March
sum(abs(sch$check)>0.1 & sch$Disbursement_Date<as.Date('2020-03-01'), na.rm=T)  
sum(abs(sch$check)>0.1 & sch$Disbursement_Date<as.Date('2020-03-01'), na.rm=T)/sum(sch$Disbursement_Date<as.Date('2020-03-01'), na.rm=T)

check <- subset(sch, abs(sch$check)>0.1 & sch$Disbursement_Date<as.Date('2020-03-01'))
write.csv(check, "361_mismatches_loan_balances_events.csv")


############ Transactions (8/10)
##### Load data: 1,048,574 rows
{
gcs_get_object(paste0('txn/',txn,'.csv'), saveToDisk = paste0(FoldData,txn,'.csv'), overwrite=TRUE)
transactions <-data.table(read.csv(paste0(FoldData,txn,'.csv'),sep=';',
                               stringsAsFactors = FALSE))
								
##### patch to fix wrong date formats since 29/05 extraction
transactions$Transaction_DateTime <- parse_date_time(transactions$Transaction_DateTime,  orders="ymd HMS")
transactions$Value_Date <- as.Date(transactions$Value_Date)
transactions$Department_ID <- as.character(transactions$Department_ID)
transactions$Customer_ID <- as.character(transactions$Customer_ID)
transactions$Customer_Account_ID <- as.character(transactions$Customer_Account_ID)
transactions$Transaction_ID <- as.character(transactions$Transaction_ID)
transactions$Transaction_Type_Code <- as.character(transactions$Transaction_Type_Code)
transactions$Counterpart_ID <- as.character(transactions$Counterpart_ID)
transactions$Counterpart_Account_ID <- as.character(transactions$Counterpart_Account_ID)
transactions$Terminal_ID <- as.character(transactions$Terminal_ID)


length(unique(transactions$Transaction_ID))/length(transactions$Transaction_ID)  ## Transaction_IDs is unique: OK!
transaction_ids <- data.table(ddply(transactions, .(Transaction_ID), summarize, 
                                                                 records = length(Transaction_ID)
                                  )) 
repeated_transactions <- subset(transaction_ids, transaction_ids$records>1)
repeated_transactions <- merge(repeated_transactions, transactions, all.x=T)

}
##### Descriptive Analysis
{
## Categorical variables
table(transactions$Department_ID)          ### 100      110    120    130    140    150    160    170    210    310    320    410   420    510      520    530 
                                        ###369712  95536  89537  86403  46143  59079  49700   2908  90748  66192   4132  33071  28062  11231   11348   4772
sum(is.na(transactions$Department_ID))    ### 0 NAs
sum(is.na(transactions$Customer_ID))    ### 0 NAs
sum(is.na(transactions$Customer_Account_ID))      ### 0 NAs  
table(transactions$Operator_ID)         ### 100% OTC
table(transactions$Channel_Name)        ### ATM EWLT   MB  MSO  OTC
                                        ### 51    98  276    4  568

table(transactions$Reversal_Flag)       ###  N      Y  949694  98880   (9.4%)
table(transactions$Transaction_Type_Code)    ###                                        
table(transactions$DbCr_Code)               ###  Cr    Db 309 (30.9%) 691 (69.1%)
table(transactions$Success_Code)              ###  C      F  949694  98880   (9.4%)
table(transactions$Transaction_Currency_Code) ### KHR THB USD 516   4 480
sum(transactions$Counterpart_ID=="" | is.na(transactions$Counterpart_ID))/length(transactions$Transaction_ID) ## 55.2% rows with [NULL]
sum(transactions$Counterpart_Account_ID=="" | is.na(transactions$Counterpart_Account_ID))/length(transactions$Transaction_ID) ## 50.2% rows with [NULL]
sum(is.na(transactions$Operator_ID) | transactions$Operator_ID=="[NULL]")  ### 0         
table(transactions$User_Name)
sum(is.na(transactions$User_Name) | transactions$User_Name=="[NULL]")/length(transactions$Transaction_ID)     ### 0% empty
table(transactions$Location_Name)
sum(is.na(transactions$Location_Name) | transactions$Location_Name=="[NULL]")/length(transactions$Transaction_ID)   ### 0% empty
sum(is.na(transactions$Terminal_ID) | transactions$Terminal_ID=="[NULL]")/length(transactions$Transaction_ID)    ###  100% NAs


## Numerical/Date variables
sum(is.na(transactions$Transaction_Amount)) # 0 NAs
sum(transactions$Transaction_Amount==0) # 0 txns with Transaction_Amount=0 
summary(transactions$Transaction_Amount) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  -41250000    -12075       -58    -74232       100  40000000 
quantile(transactions$Transaction_Amount, c(0,.1,.25,.5,.75,.9,1)) #### -41250000.000   -281090.000    -12075.000       -58.105       100.000     55000.000  40000000.000

sum(is.na(transactions$Transaction_Fee_Amount)) # 502 NAs
sum(transactions$Transaction_Fee_Amount==0) # NA
summary(transactions$Transaction_Fee_Amount) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max. NA's  0.000    0.000    0.000    8.039    0.000 4000.000      502 
quantile(transactions$Transaction_Fee_Amount, c(0,.1,.25,.5,.75,.9,1)) #### 0    0    0    0    0    0    0

sum(is.na(transactions$Commission_Amount)) # 0 NAs
summary(transactions$Commission_Amount) #  Min.   1st Qu.    Median      Mean   3rd Qu.      Max.  0       0       0       0       0       0 
quantile(transactions$Commission_Amount, c(0,.1,.25,.5,.75,.9,1)) ####0    0    0    0    0    0    0

summary(transactions$Customer_Account_Balance)  ## 100% 0
summary(transactions$Float_Account_Balance)     ## 100% 0

summary(transactions$Transaction_DateTime)  # min. 2020-08-31, max. 2020-08-31
summary(transactions$Value_Date) # min.2020-08-25, max. 2020-08-31

sum(is.na(transactions$Transaction_DateTime)) # 0 NAs
sum(transactions$Date!=transactions$Transaction_DateTime)/length(transactions$Transaction_ID) ## 8.6% txns with Date <> Value_Date

## Check formats
sum(grepl('^[0-9]{15}$',  transactions$Transaction_ID))/length(transactions$Transaction_ID) ### Transaction_IDs have 15 digits
sum(grepl('^[0-9]{1,7}$',  transactions$Customer_ID))/length(transactions$Transaction_ID) ### Customer_IDs have just up to 7 digits
sum(grepl('^[0-9]{1,7}$',  transactions$Counterpart_ID))/sum(!(is.na(transactions$Counterpart_ID) | transactions$Counterpart_ID=="[NULL]")) ### Counterpart_IDs have just up to 7 digits
sum(grepl('^[0-9]{15}$',  transactions$Transaction_ID))/sum(!(is.na(transactions$Transaction_ID))) ### Transaction_IDs have just 15 digits
sum(grepl('^[0-9]{1,8}$',  transactions$Customer_Account_ID),na.rm=T)/sum(!((is.na(transactions$Customer_Account_ID) | transactions$Customer_Account_ID=="[NULL]")))  ### Account_ID have up to 8 digits
sum(grepl('^[0-9]{1,8}$',  transactions$Counterpart_Account_ID),na.rm=T)/sum(!((is.na(transactions$Counterpart_Account_ID)| transactions$Counterpart_Account_ID==""))) ### 89.96% Counterpart_Account_IDs have up to 8 digits
sum(grepl('^[A-Z]{3}$',  transactions$Department_ID))/length(transactions$Transaction_ID) ### Department_IDs have 3 characters
sum(grepl('^[0-9]{1,3}$',  transactions$Transaction_Type_Code))/length(transactions$Transaction_ID) ### Transaction_Type_Codes have up to 3 digits
}

##### Controls over Operator_ID, User_Name, Location_Name
{
#operators_locations <- data.table(ddply(transactions,
#                                .(Channel, Operator_ID), plyr::summarise, 
#                                locations = length(unique(Location_Name)))) ### Operator_ID:Location_Name 1:n
								
#operators_usernames  <- data.table(ddply(transactions,
#                                .(Channel, Operator_ID), plyr::summarise, 
#                                users = length(unique(User_Name))))  ### Operator_ID:User_Name 1:1
								
#operators_usernames  <- data.table(ddply(transactions,
#                                .(Channel, Operator_ID, User_Name), plyr::summarise,                                 users = length(unique(User_Name))))
								
#operators_locations_usernames  <- data.table(ddply(transactions,
#                                .(Channel, Operator_ID, User_Name, Location_Name), plyr::summarise, 
#                                txns = length(Transaction_ID),
#								amount = sum(as.numeric(Amount))
#								))								
}			
sum(transactions$Customer_Account_Number %in% account_balances$Account_ID)
sum(transactions$Customer_Account_Number %in% accounts$Account_ID)		

## Coherency checks between account balances and transactions.
amounts <- data.table(ddply(transactions[Date<as.Date('2020-04-01'), ], .(Customer_Account_Number, Db_Cr), plyr::summarise,
                                 txns = length(Transaction_ID),
                                 Amount = sum(Amount)))
amounts$Movmt <- ifelse(amounts$Db_Cr=='Db', -amounts$Amount, amounts$Amount)
movmts <- data.table(ddply(amounts, .(Customer_Account_Number), plyr::summarise,
                                 txns = length(txns),
                                 Movmt = sum(Movmt)))
setnames(movmts, old=c("Customer_Account_Number"), new=c("Account_ID"))								 								 
accbal_movmts<-merge(account_balances_28092018[,.(Account_ID,Principal_Balance,Real_Balance)],
                                    movmts,by='Account_ID',all=T)
setnames(accbal_movmts, old=c("Principal_Balance","Real_Balance"), new=c("Principal_Balance_280918","Real_Balance_280918"))		
accbal_movmts<-merge(accbal_movmts, 
									account_balances[,.(Account_ID,Principal_Balance,Real_Balance)],by='Account_ID',all=T)
### KO: all transactions have a loan id as Customer_Account_Number --> there only transactions done on loans accounts (ex. repayments, payments, payoffs, passages en perte)									
### so I check the coherency using Counterpart_Account_Number for the same txns, which should be the Number of the customer deposit account
amounts <- data.table(ddply(transactions[Date>=as.Date('2018-09-29') & Date<as.Date('2020-04-01') & Result=='C', ],
				.(Customer_Account_Number, Db_Cr), plyr::summarise,
                                 txns = length(Transaction_ID),
                                 Amount = sum(Amount)))
amounts$Movmt <- ifelse(amounts$Db_Cr=='Db', -amounts$Amount, amounts$Amount)
movmts <- data.table(ddply(amounts, .(Customer_Account_Number), plyr::summarise,
                                 txns = sum(txns),
                                 Movmt = sum(Movmt)))
setnames(movmts, old=c("Customer_Account_Number"), new=c("Account_ID"))								 								 
accbal_movmts<-merge(account_balances_28092018[,.(Account_ID,Principal_Balance,Real_Balance)],
                                    movmts,by='Account_ID',all=T)
									
setnames(accbal_movmts, old=c("Principal_Balance","Real_Balance","txns","Movmt"), new=c("Principal_Balance_280918","Real_Balance_280918",
"Txns","Movmt"))		
accbal_movmts<-merge(accbal_movmts, 
									account_balances[,.(Account_ID,Principal_Balance,Real_Balance)],by='Account_ID',all=T)
setnames(accbal_movmts, old=c("Principal_Balance","Real_Balance"), new=c("Principal_Balance_310320","Real_Balance_310320"))				

accbal_movmts$Principal_Balance_280918 <- ifelse(is.na(accbal_movmts$Principal_Balance_280918),0, accbal_movmts$Principal_Balance_280918)
accbal_movmts$Real_Balance_280918 <- ifelse(is.na(accbal_movmts$Real_Balance_280918),0,accbal_movmts$Real_Balance_280918)		
accbal_movmts$Txns <- ifelse(is.na(accbal_movmts$Txns),0, accbal_movmts$Txns)	
accbal_movmts$Movmt <- ifelse(is.na(accbal_movmts$Movmt),0, accbal_movmts$Movmt)				
accbal_movmts$Principal_Balance_310320_Calc <- accbal_movmts$Principal_Balance_280918+accbal_movmts$Movmt									
accbal_movmts$Real_Balance_310320_Calc <- accbal_movmts$Real_Balance_280918+accbal_movmts$Movmt

#### I select only current accounts (801), no loans: 26,252 obs
accbal_movmts <- subset(accbal_movmts, substr(accbal_movmts$Account_ID,1,3)=='801')

sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>0.1) # 9854
sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>0.1)/length(accbal_movmts$Account_ID) # 37.8%
sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>1) # 876
sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>1)/length(accbal_movmts$Account_ID) # 3.4%

kos <- subset(accbal_movmts, abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>1) ### 511 accounts KOs (1.95%)
write.csv(kos,"511accountskos.csv",row.names=F)			
									
### I retry between february and march
				amounts <- data.table(ddply(transactions[Date>=as.Date('2020-03-01') & Date<as.Date('2020-04-01') & Result=='C', ],
				.(Customer_Account_Number, Db_Cr), plyr::summarise,
                                 txns = length(Transaction_ID),
                                 Amount = sum(Amount)))
amounts$Movmt <- ifelse(amounts$Db_Cr=='Db', -amounts$Amount, amounts$Amount)
movmts <- data.table(ddply(amounts, .(Customer_Account_Number), plyr::summarise,
                                 txns = length(txns),
                                 Movmt = sum(Movmt)))
setnames(movmts, old=c("Customer_Account_Number"), new=c("Account_ID"))								 								 
accbal_movmts<-merge(account_balances_29022020[,.(Account_ID,Principal_Balance,Real_Balance)],
                                    movmts,by='Account_ID',all.x=T)
setnames(accbal_movmts, old=c("Principal_Balance","Real_Balance","txns","Movmt"), new=c("Principal_Balance_290220","Real_Balance_290220",
"Txns_0320","Movmt_0320"))		
accbal_movmts<-merge(accbal_movmts, 
									account_balances[,.(Account_ID,Principal_Balance,Real_Balance)],by='Account_ID',all.x=T)
setnames(accbal_movmts, old=c("Principal_Balance","Real_Balance"), new=c("Principal_Balance_310320","Real_Balance_310320"))				

accbal_movmts$Principal_Balance_290220 <- ifelse(is.na(accbal_movmts$Principal_Balance_290220),0, accbal_movmts$Principal_Balance_290220)
accbal_movmts$Real_Balance_290220 <- ifelse(is.na(accbal_movmts$Real_Balance_290220),0,accbal_movmts$Real_Balance_290220)		
accbal_movmts$Txns_0320 <- ifelse(is.na(accbal_movmts$Txns_0320),0, accbal_movmts$Txns_0320)	
accbal_movmts$Movmt_0320 <- ifelse(is.na(accbal_movmts$Movmt_0320),0, accbal_movmts$Movmt_0320)				
accbal_movmts$Principal_Balance_310320_Calc <- accbal_movmts$Principal_Balance_290220+accbal_movmts$Movmt_0320									
accbal_movmts$Real_Balance_310320_Calc <- accbal_movmts$Real_Balance_290220+accbal_movmts$Movmt_0320

sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>0.1) # 9854
sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>0.1)/length(accbal_movmts$Account_ID) # 37.8%
sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>1) # 876
sum(abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>1)/length(accbal_movmts$Account_ID) # 3.4%

kos <- subset(accbal_movmts, abs(accbal_movmts$Principal_Balance_310320_Calc-accbal_movmts$Principal_Balance_310320)>1)
kos$PB_Mismatch <- kos$Principal_Balance_310320_Calc-kos$Principal_Balance_310320
kos$PB_Mismatch_Perc <- kos$PB_Mismatch/kos$Principal_Balance_310320
kos$RB_Mismatch <- kos$Real_Balance_310320_Calc-kos$Real_Balance_310320
kos$RB_Mismatch_Perc <- kos$RB_Mismatch/kos$Real_Balance_310320

summary(kos$PB_Mismatch)
summary(kos$PB_Mismatch_Perc)
summary(kos$PB_Mismatch)
summary(kos$PB_Mismatch)


write.csv(kos,"3570accountskos.csv",row.names=F)						
#### Transaction Codes
#{
#tr_ch_cm_codes <-data.table(read.csv(paste0(FoldData,'Tr_Ch_Cm_Codes.csv'),
#                               stringsAsFactors = FALSE,na.strings='', sep=';'))								
														
#### Transaction Codes
{
#tr_ch_cm_codes <-data.table(read.csv(paste0(FoldData,'Tr_Ch_Cm_Codes.csv'),
#                               stringsAsFactors = FALSE,na.strings='', sep=';'))								
								
#txn_codes <- subset(tr_ch_cm_codes,tr_ch_cm_codes$Type!="Commissions")
#txn_codes$Code <- as.character(txn_codes$Code)
#length(unique(txn_codes$Code))

#txn_codes <- subset(txn_codes,select=c(Code,Description))

#transactions <- merge(transactions, txn_codes, by.x="Transaction_Code",by.y="Code",all.x=T)
}

##### Check of joining of transactions with balances and commissions (11/11/2019)
{

#txns_09 <-data.table(read.csv(paste0(FoldData,'cm_txn_advans_cm_txn_20190901_echantillon.csv'),
#                               stringsAsFactors = FALSE,na.strings=''))
								
#balances <-data.table(read.csv(paste0(FoldData,'cm_txn_advans_cm_txn_Account_Balance_20191107080000.csv'),
#                               stringsAsFactors = FALSE,na.strings=''))
	
#balances$ptid <- as.character(balances$ptid)

#txns_09 <- merge(txns_09, balances, by.x="origin_tracer_no",by.y="ptid",all.x=T)

#commissions <- data.table(read.csv(paste0(FoldData,'cm_txn_advans_cm_txn_commissions_20191107080000.csv'),
#                                stringsAsFactors = FALSE,na.strings=''))

#commissions$TransactionUniqueRef <- as.character(commissions$TransactionUniqueRef)

#commissions <- subset(commissions,!(is.na(commissions$TransactionUniqueRef)))

#txns_09 <- merge(txns_09, commissions, by.x="origin_tracer_no",by.y="TransactionUniqueRef",all.x=T)

#sum(!(is.na(txns_09$avail_bal)))
#sum(!(is.na(txns_09$avail_bal)))/length(txns_09$Transaction_ID)
#sum(!(is.na(txns_09$tfr_avail_bal)))
#sum(!(is.na(txns_09$tfr_avail_bal)))/length(txns_09$Transaction_ID)
#sum(!(is.na(txns_09$CommissionAmount)))
#sum(!(is.na(txns_09$CommissionAmount)))/length(txns_09$Transaction_ID)
#sum(!(is.na(txns_09$FeeAmount)))
#sum(!(is.na(txns_09$FeeAmount)))/length(txns_09$Transaction_ID)

#txns_09$Transaction_Code <- as.character(txns_09$Transaction_Code)

#txns_09 <- merge(txns_09, txn_codes, by.x="Transaction_Code",by.y="Code",all.x=T)

#txns_09$avail_bal_dummy <- ifelse(is.na(txns_09$avail_bal),"N","Y")
#txns_09$tfr_avail_bal_dummy <- ifelse(is.na(txns_09$tfr_avail_bal),"N","Y")
#txns_09$CommissionAmount_dummy <- ifelse(is.na(txns_09$CommissionAmount),"N","Y")
#txns_09$FeeAmount_dummy <- ifelse(is.na(txns_09$FeeAmount),"N","Y")

#table(txns_09$Description,txns_09$avail_bal_dummy)
#table(txns_09$Description,txns_09$tfr_avail_bal_dummy)
#table(txns_09$Description,txns_09$CommissionAmount_dummy)
#table(txns_09$Description,txns_09$FeeAmount_dummy)

#transactions2 <- subset(transactions, select=c("Transaction_ID", "origin_tracer_no",
#"avail_bal","tfr_avail_bal","FeeAmount","CommissionAmount"))

#write.csv(transactions2,"Transactions_balances_commissions",row.names=F)
}							

############ Loan Application Accessory (20/10)
##### Load data: 298,206 rows
{
gcs_get_object(paste0('laa/',laa,'.csv'), saveToDisk = paste0(FoldData,laa,'.csv'), overwrite=TRUE)
laas <-data.table(read.csv(paste0(FoldData,laa,'.csv'),sep=';',
                                       stringsAsFactors = FALSE))
    
laas$LoanApplication_Update_Date <- as.Date(laas$LoanApplication_Update_Date)
laas$LoanApplication_Stage_Code <- as.character(laas$LoanApplication_Stage_Code)
laas$Customer_ID <- as.character(laas$Customer_ID)
length(unique(paste0(laas$LoanApplication_ID,laas$LoanApplication_Stage_Code,laas$Loan_ID)))/length(laas$LoanApplication_ID) ## 1: laas$Loan_Application_ID is the unique key
}
##### Descriptive Analysis
{
## Categorical variables
Hmisc::describe(laas)

## Check formats
sum(grepl('^[I][C][L][A][0-9]{10}$',  laas$LoanApplication_ID))/length(laas$LoanApplication_ID) ### Loan Application IDs format is ICLA0123456789
sum(grepl('^[0-9]{1,7}$',  laas$Customer_ID))/length(laas$LoanApplication_ID) ### Customer_ID format is up to 7 digits
############ Portfolio_Manager (20/11/2020)
##### Load data: 4,913 rows
gcs_get_object(paste0('pfm/',pfm,'.csv'), saveToDisk = paste0(FoldData,pfm,'.csv'), overwrite=TRUE)
pfms <-data.table(read.csv(paste0(FoldData,pfm,'.csv'),sep=';',
                           stringsAsFactors = FALSE))

pfms$Customer_ID <- as.character(pfms$Customer_ID)
pfms$PortfolioManager_Start_Date <- as.Date(pfms$PortfolioManager_Start_Date)
pfms$PortfolioManager_End_Date <- as.Date(pfms$PortfolioManager_End_Date)
dates <- pfms$PortfolioManager_Update_Date ## don't ask me why
dates <- data.frame(as.Date(dates, format = "%Y-%m-%d"))
pfms$PortfolioManager_Update_Date <- dates
pfms <- pfms[order(-PortfolioManager_End_Date)]
dates <- pfms$PortfolioManager_End_Date ## don't ask me why
dates <- data.frame(as.Date(dates, format = "%Y-%m-%d"))
pfms$PortfolioManager_End_Date <- dates
pfms$PortfolioManager_ID <- as.character(pfms$PortfolioManager_ID)
pfms$PortfolioManager_Area_Code <- as.character(pfms$PortfolioManager_Area_Code)

length(unique(pfms$PortfolioManager_ID))/length(pfms$PortfolioManager_ID)  #### 1: The key is Portfolio_Manager_ID, in the future with changes should become Portfolio_Manager_ID/Update_Date
}


##### Descriptive Analysis
{
## Categorical variables
Hmisc::describe(pfms)

table(subset(pfms, Company_sublevel2_ID=="")$PortfolioManager_Status_Name) ## 20 Active without Region_ID
sum(pfms$PorfolioManager_Start_Date>pfms$PorfolioManager_End_Date, na.rm=T) ## 0
sum(pfms$PortfolioManager_Status_Name=='ACTIVE' & !(pfms$PorfolioManager_End_Date=="")) ## 0
sum(pfms$PortfolioManager_Status_Name=='INACTIVE' & !(pfms$PorfolioManager_End_Date==""))    ## 0

## Check formats
sum(grepl('^[0-9]{1,4}$',  pfms$PortfolioManager_ID))/length(pfms$PortfolioManager_ID) ### PortfolioManager_IDs have just up to 4 digits
}					

sum(!(accounts$Portfolio_Manager_ID %in% pfms$Portfolio_Manager_ID)) ### all Portfolio_Manager_IDs in accounts are in Portfolio_Managers
sum(!(loans$Portfolio_Manager_ID %in% pfms$Portfolio_Manager_ID)) ### all Portfolio_Manager_IDs in loans are in Portfolio_Managers
sum(!(customers$Portfolio_Manager_ID %in% pfms$Portfolio_Manager_ID)) ### all Portfolio_Manager_IDs in loans are in Portfolio_Managers

### check name and users
name_users <- ddply(pfms,.(PortfolioManager_Name, PortfolioManager_User_Name),
                    summarise,rows=length(PortfolioManager_ID))
############ Account Activity Summary (29/06/2020)
##### Load data: 39,530 rows
{
gcs_get_object('Account_Activity_Summary_29_06.csv', saveToDisk = paste0(FoldData,'Account_Activity_Summary_29_06.csv'), overwrite=TRUE)

aas <-data.table(read.csv(paste0(FoldData,'Account_Activity_Summary_29_06.csv' ),sep=';',
                                stringsAsFactors = FALSE,na.strings=''))
								
length(unique(paste0(aas$Account_ID,aas$Update_Date,aas$TT_Code,aas$TT_Db_Cr,sep=' ')))  #### Account_ID/Update_Date/TT_Code/TT_Db_Cr is the unique key of the table
sum(aas$TT_Number>1)/length(aas$Account_ID) # 0.63% rows are multiple transactions done on same hh:mm:ss --> to be checked

#### formatting								
aas$Update_Date <- as.POSIXct(substr(aas$Update_Date,1,10), format = "%d/%m/%Y", tz = "UTC")
aas$Update_Date <- as.Date(aas$Update_Date)	
aas$TT_Code <- as.character(aas$TT_Code)							
}

##### Descriptive Analysis
{
## Categorical variables

table(aas$Country)        ###   100% TN
table(aas$Currency)       ### 100% TND
table(aas$TT_Code)        ### 100   101   102   117   152   155   157   158   164
                          ###  1   2686  1324  14480   33     1  1323     1 19681
table(aas$TT_Db_Cr)       ###  Cr  Db 18491 (46.8%) 21039 (53.2%)
table(aas$Commission_Flag)### 100% N


## Numerical/Date variables
table(aas$Update_Date)     # Min. 2020-02-03 Max. 2020-02-28
summary(aas$TT_Number) # Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 1.000   1.000   1.000   1.034   1.000  68.000  
quantile(aas$TT_Number, c(0,.1,.25,.5,.75,.9,1), na.rm=T)  # 1    1    1    1    1    1   68 
summary(aas$TT_Volume) # 0        0      100    53100      350 79760000
quantile(aas$TT_Volume, c(0,.1,.25,.5,.75,.9,1), na.rm=T)  # 0        0        0      100      350     2500 79760000

sum(aas$TT_Volume==0) # 0 rows with Volume (Amount) = 0


## Check formats
sum(grepl('^[0-9]{3}[-][0-9]{7}$',  aas$Account_ID)) ### Account_ID have format 123-1234567
}					

#sum(!(aas$TT_Code %in% txn_codes$Code)) ### all TT_Code are in table of transaction codes

#sum(aas$TT_Number[aas$TT_Volume>0]) ## 39,779 txns with Volume>0 vs. 34,326 from transactions (BQ table transactions)
#sum(as.numeric(aas$TT_Volume))  ## Volume 3,366,652,418 vs. 3 366,999,404 from transactions (BQ table transactions)

#### Check account activity summary vs. transactions
{
aas$Movmt <- ifelse(aas$TT_Db_Cr=="Cr", aas$TT_Volume, aas$TT_Volume*-1)


## txns and movements per account on february from transactions table: 36,294 obs
  txns_022020 <- data.table(ddply(transactions[Date>=as.Date('2020-02-01') & Date<as.Date('2020-03-01') & Result=='C', ],
                              .(Customer_Account_Number, Db_Cr), plyr::summarise,
                              Txns_txns = length(Transaction_ID),
                              Mvmt_txns = sum(Amount)))
  txns_022020$Mvmt_txns <- ifelse(txns_022020$Db_Cr=='Db', (txns_022020$Mvmt_txns)*-1, txns_022020$Mvmt_txns)
  movmts_022020 <- data.table(ddply(txns_022020, .(Customer_Account_Number), plyr::summarise,
                                      Txns_txns = sum(Txns_txns),
                                      Mvmt_txns = sum(Mvmt_txns)))

sum(movmts_022020$Txns_txns) ### 48,405
sum(movmts_022020$Mvmt_txns) ### 2,444,866
	
## txns and movements per account on february from account activity summary table: 39,779 obs	
aas_022020  <- data.table(ddply(aas,
                                .(Account_ID), plyr::summarise, 
								Txns_aas = sum(TT_Number),
								Mvmt_aas = sum(Movmt)
								))			

sum(aas_022020$Txns_aas) ### 39,779
sum(aas_022020$Mvmt_aas)	###  -166678.2		
aas_022020$Mvmt_aas <- round(aas_022020$Mvmt_aas, digits=3)

aas_022020 <- merge(aas_022020, movmts_022020, by.x="Account_ID",by.y="Customer_Account_Number", all.x=T)	

kos <- subset(aas_022020, abs(aas_022020$Txns_aas-aas_022020$Txns_txns)>0.1 
              | abs(aas_022020$Mvmt_aas-aas_022020$Mvmt_txns)>0.1)
length(kos$Account_ID)/length(aas_022020$Account_ID)*100
#### Il y a 236 comptes (1.50%) ou la somme de nombre et volume de transactions sur le mois de fevrier 2020 depuis l'extraction 
#### account_activity_summary et celle transactions ne coincident pas	
write.csv(kos,"236aastobechecked.csv",row.names=F)		  
sum(aas_022020$Txns_txns) ## 40,116
sum(aas_022020$Txns_aas)  ## 39,779
sum(aas_022020$Mvmt_txns) ## -370,951.6 
sum(aas_022020$Mvmt_aas)  ## -166,678.2			  
#### check for transaction codes

## txns and movements per account on 30/09/2019 from transactions table: 23,170 obs
txnscodes_022020  <- data.table(ddply(transactions[Date>=as.Date("2020-02-01") & Date<as.Date("2020-03-01") & Reversal_Flag=="N"
								& transactions$Customer_Account_Number %in% aas_022020$Account_ID, ],
                                .(Transaction_Code), plyr::summarise, 
								Txns_txns = length(Transaction_ID),
                                Amount_txns = sum(as.numeric(Amount))
								))

sum(txnscodes_022020$Txns_txns) ### 40,116
sum(txnscodes_022020$Amount_txns) ### 38,560,656
	
## txns and movements per transaction code on 30/09/2019 from account activity summary table: 23,170 obs	
aastcodes_022020  <- data.table(ddply(aas,
                                .(TT_Code), plyr::summarise, 
								Txns_aas = sum(as.numeric(TT_Number)),
								Amount_aas = sum(as.numeric(TT_Volume))
								))			

sum(aastcodes_022020$Txns_aas) ### 34,324
sum(aastcodes_022020$Amount_aas)	###  3,366,652,418		


aas_txns_tcodes_022020 <- merge(aastcodes_022020, txnscodes_022020, by.x="TT_Code",by.y="Transaction_Code", all=T)	

kos_tcodes <- subset(aas_txns_tcodes_022020, aas_txns_tcodes_022020$Txns_txns!=aas_txns_tcodes_022020$Txns_aas 
| aas_txns_tcodes_022020$Amount_txns!=aas_txns_tcodes_022020$Amount_aas)
			
}
############ Loan Applications Core (20/11/2020)
##### Load data: 298,206 rows
{
gcs_get_object(paste0('lap/',lap,'.csv'), saveToDisk = paste0(FoldData,lap,'.csv'), overwrite=TRUE)
lap <-data.table(read.csv(paste0(FoldData,lap,'.csv'),sep=';',
                             stringsAsFactors = FALSE))

lap$LoanApplication_Update_Date <- as.Date(lap$LoanApplication_Update_Date)					
lap <- lap[order(-LoanApplication_End_Date)]
lap$LoanApplication_End_Date <- as.Date(lap$LoanApplication_End_Date)	
lap$LoanApplication_Start_Date <- as.Date(lap$LoanApplication_Start_Date)	
lap$PortfolioManager_ID <- as.character(lap$Portfolio_Manager_ID)
lap$LoanApplication_Purpose_Code <- as.character(lap$LoanApplication_Purpose_Code)
lap$Loan_Code <- as.character(lap$Loan_Code)
lap$LoanApplication_ID <- trimws(lap$LoanApplication_ID)
lap$LoanApplication_Stage_Code <- as.character(lap$LoanApplication_Stage_Code)

length(unique(paste0(lap$LoanApplication_ID,lap$LoanApplication_Stage_Code,lap$Loan_ID)))/length(lap$LoanApplication_ID)  #### 0.998062
}

lap_ids  <- data.table(ddply(lap,
                             .(LoanApplication_ID, LoanApplication_Stage_Code), plyr::summarise, 
                             rows = length(LoanApplication_ID)	
))			

lap_stages_doubles <- subset(lap_ids,lap_ids$rows>1)

lap_stages_doubles <- merge(lap_stages_doubles,lap, all.x=T)

##### Descriptive Analysis
{

lap$ladays <- lap$LoanApplication_End_Date-lap$LoanApplication_Start_Date
Hmisc::describe(lap)

table(subset(lap, !(is.na(LoanApplication_End_Date)))$LoanApplication_Stage_Code) ### all applications with End Date are disbursed (Stage Code=5)
table(subset(lap, !(is.na(LoanApplication_End_Code)))$LoanApplication_Stage_Code) ### all applications with End Date are disbursed (Stage Code=5)
table(subset(lap, !(Loan_ID==""))$LoanApplication_Stage_Code) ## only disbursed application (Stage=5) have Loan_ID: OK!!
## Categorical variables

## Check formats
sum(grepl('^[0-9]{3}[-][0-9]{7}$',  lap$Loan_Application_ID))/length(lap$Loan_Application_ID) ### Loan_Application_IDs are 123-123456
sum(grepl('^[0-9]{3}[-][0-9]{7}$',  lap$Loan_ID))/sum(!(lap$Loan_ID=='[NULL]')) 
sum(grepl('^[0-9]{1,4}$',  lap$Inputter_ID))/length(lap$Loan_Application_ID) ### Inputter_IDs have just up to 4 digits
sum(grepl('^[0-9]{1,4}$',  lap$Portfolio_Manager_ID))/length(lap$Loan_Application_ID) ### Portfolio_Manager_IDs have just up to 4 digits
sum(grepl('^[0-9]{1,6}$',  lap$Customer_ID))/length(lap$Loan_Application_ID)

table(lap$Stage[lap$Loan_ID=='[NULL]']) ### 51 disbursed applications without Loan_ID (over 38,584)

table(lap$Stage[!(lap$Loan_ID=='[NULL]')]) ### 251 Closed applications with Loan_ID
}					

## Coherency checks with other tables
### check loan applications vs. loans
{
loans_31032020 <-data.table(read.csv(paste0(FoldData,'Loan.csv'), sep=';',
                                stringsAsFactors = FALSE,na.strings=''))  ## 37,427

disbursed <- subset(lap, lap$Stage=='Disbursed') ## 38,584 disbursed loans
length(unique(disbursed$Loan_Application_ID)) ## 38,567 unique lap_ids
length(unique(disbursed$Loan_ID)) ## 38,533 unique loan_ids

sum(!(disbursed$Loan_ID=='[NULL]')) ###  38,533 with Loan_IDs

sum(!(disbursed$Loan_ID %in% loans$Loan_ID)) ### of which 1,127 are not in loans extractions

table(disbursed$Opening_Date [!(disbursed$Loan_ID %in% loans_31032020$Loan_ID)])

kos <- subset(disbursed, !(disbursed$Loan_ID %in% loans_31032020$Loan_ID))
kos <- subset(kos, kos$Loan_ID !='[NULL]')
length(kos$Loan_Application_ID)/length(disbursed$Loan_Application_ID)
### 1,127 applications "Disbursed" not in loans (31/3/2020)
write.csv(kos, "1127_disbursed_applications_not_in_loans.csv")

### check loan applications vs. events
check <- subset(events, !(events$Contract_ID %in% disbursed$Loan_ID))

kos  <- data.table(ddply(check,
                                .(Contract_ID), plyr::summarise, 
								Contracts = length(unique(Contract_ID))
								))	### 13 contracts in event not in disbursed lap (over 28,474)
								

###### check if these are applications from 2017
#lap2017 <-data.table(read.csv(paste0(FoldData,'cm_lap_advans_cm_lap_20191029161300_complement2017.csv'),
#                                stringsAsFactors = FALSE,na.strings=''))

#lap2017$Loan_Application_ID <- trimws(lap2017$Loan_Application_ID)
#lap2017$Loan_ID <- trimws(lap2017$Loan_ID)
#lap2017$Stage_Code <- trimws(lap2017$Stage_Code)
#setnames(lap2017, old=c("Closing.date"), new=c("Closing_Date"))
								
#lap2017$Closing_Date <- as.Date(lap2017$Closing_Date)							
#lap2017$Update_Date <- as.Date(lap2017$Update_Date)	
#lap2017$Opening_Date <- as.Date(lap2017$Opening_Date)

#lap2017$Customer_ID <- as.character(lap2017$Customer_ID)
#lap2017$Department <- as.character(lap2017$Department)	
#lap2017$Inputter_ID <- as.character(lap2017$Inputter_ID)
#lap2017$Portfolio_Manager_ID <- as.character(lap2017$Portfolio_Manager_ID)
#lap2017$Purpose_Requested <- as.character(lap2017$Purpose_Requested)
#lap2017$Loan_Code <- as.character(lap2017$Loan_Code)
								
#kos <- subset(kos, !(kos$Contract_ID %in% lap2017$Loan_ID))
								
#kos <- subset(kos, select=-Rows)
### 30 contracts disbursed after 1/1/2018 from events not in lap
#write.csv(kos, "30_contracts_events_not_in_lap.csv", row.names=F)

sum(!(lap$Customer_ID %in% customers$Customer_ID)) ## all Customer_IDs are in Customer table

sum(!(lap$Portfolio_Manager_ID %in% pfms$Portfolio_Manager_ID)) ## all PM IDs in loan applications core are in portfolio_managers
}

############ Dictionary (30/11/2020)
##### Load data: 58,504 rows
{
gcs_get_object(paste0('dic/',dic,'.csv'), saveToDisk = paste0(FoldData,dic,'.csv'), overwrite=TRUE)
dictionary <-data.table(read.csv(paste0(FoldData,dic,'.csv'),sep='^',
                            stringsAsFactors = FALSE))
								
length(unique(paste0(dictionary$Field_Name,dictionary$Field_Code)))/length(dictionary$Field_Name) ## ok: unique key is Field_Name/Field_Code

Field_Names_Codes  <- data.table(ddply(dictionary,
                             .(Field_Name, Field_Code), plyr::summarise, 
                             rows = length(Field_Name)	
))			

table(dictionary$Field_Name)
table(dictionary$Dictionary_Update_Date)
Hmisc::describe(dictionary)
}

############ Currency (02/12/2020)
##### Load data: 6 rows
{
  gcs_get_object(paste0('cur/',cur,'.csv'), saveToDisk = paste0(FoldData,cur,'.csv'), overwrite=TRUE)
  currency <-data.table(read.csv(paste0(FoldData,cur,'.csv'),sep=';',
                                   stringsAsFactors = FALSE))
  
}

############ Written Offs (17/07/2020)
##### Load data: 1,011256 rows
{
gcs_get_object('written_off__.csv', saveToDisk = paste0(FoldData,'written_off__.csv'), overwrite=TRUE)

wof <-data.table(read.csv(paste0(FoldData,'written_off__.csv' ),sep=';',
                                stringsAsFactors = FALSE,na.strings=''))
								
wof$Customer_ID <- as.character(wof$Customer_ID)
wof$Portfolio_Manager_ID <- as.character(wof$Portfolio_Manager_ID)
wof$WO_Date <- as.POSIXct(substr(wof$WO_Date,1,10), format = "%d/%m/%Y", tz = "UTC")
wof$Update_Date <- as.POSIXct(substr(wof$Update_Date,1,10), format = "%d/%m/%Y", tz = "UTC")
wof$Loan_ID <- trimws(wof$Loan_ID)
length(unique(wof$Loan_ID)) ## 1,581 Loan_IDs

wof_ids  <- data.table(ddply(wof,
                                .(Loan_ID), plyr::summarise, 
								rows = length(Loan_ID)
								))	
}

##### Descriptive Analysis
{
## Categorical variables

table(wof$Country)        ### 100% TN
summary(wof$WO_Date)        ### min. 2016-03-09 max. 2020-03-31
table(wof$Update_Date)    ### 2020-03-31
## Numerical/Date variables
quantile(wof$Principal_Written_Off, c(0,.1,.25,.5,.75,.9,1)) #### 0.0000   295.6884   802.3870  1894.1210  3830.0000  7645.2930 34818.6800 
quantile(wof$Principal_Due, c(0,.1,.25,.5,.75,.9,1))         #### 0.0000   132.4124   519.7230  1428.4120  3093.0960  6233.0586 33444.1570
quantile(wof$Interests_Written_Off, c(0,.1,.25,.5,.75,.9,1)) #### 0          0        0         0          0          0         0
quantile(wof$Interests_Due, c(0,.1,.25,.5,.75,.9,1))         #### 0.000       0.000      0.000     25.690    132.357    329.291   7135.642
quantile(wof$Penalty_Written_Off, c(0,.1,.25,.5,.75,.9,1))   #### 0          0        0         0          0          0         0
quantile(wof$Penalty_Due, c(0,.1,.25,.5,.75,.9,1))           #### 0.000      87.937    261.022    550.618    906.345   1488.151   4104.265

## Check formats
sum(grepl('^[0-9]{1,4}$',  wof$Portfolio_Manager_ID))/length(wof$Loan_ID) ### PM_IDs have up to 4 digits
sum(grepl('^[0-9]{1,6}$',  wof$Customer_ID))/length(wof$Loan_ID) ### Customer_IDs have up to 6 digits

}			
{
## loan balances at 12/11/2019
check <- subset(loan_balances, loan_balances$Loan_ID %in% wof$Loan_ID) # all wof loans are in loan balances with Loan_Status="awaitingapproval Charge_Off"
## all written off loans are in loan balance extraction
table(check$Loan_Status)     # awaitingapproval Charge-Off        cancelleddisbursement        Restricted             
                             #              1516                 1                 1

#check <- subset(check, select="Loan_ID")

#write.csv(check, "62_awaitingapproval_Wofs.csv")
}
