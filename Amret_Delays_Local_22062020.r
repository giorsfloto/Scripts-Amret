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

############ Contracts not group disbursed after 1/9/2018 and not early termination with mismatch class

mismatches_contracts_sql <- paste0("SELECT Contract_ID,
CASE WHEN ABS(Amount_Due-Total_Paid-Past_Due)<0.1 THEN 0 
     WHEN Amount_Due=0 THEN 1
     WHEN Amount_Due>0 AND Amount_Due-Total_Paid-Past_Due<0 THEN 2
     WHEN Amount_Due>0 AND Amount_Due-Total_Paid-Past_Due>0 THEN 3 END AS Mismatch
FROM
(
SELECT Contract_ID, Currency, Loan_Code, Disbursement_Date, Maturity_Date, 
Disbursement_Amount, Written_Off_Flag,
Principal_Paid, Interest_Paid, Total_Paid,
IFNULL(Amount_Due,0) AS Amount_Due, IFNULL(Principal_Amount_Due,0) AS Principal_Amount_Due,
IFNULL(Interest_Amount_Due,0 ) AS Interest_Amount_Due, 
LB_Date, LB_Principal_Paid, LB_Interest_Paid, Principal_Outstanding,
IFNULL(Principal_Past_Due,0) AS Principal_Past_Due,	
IFNULL(Interests_Past_Due,0) AS Interests_Past_Due,
IFNULL(Principal_Past_Due,0)+IFNULL(Interests_Past_Due,0) AS Past_Due,
Early_Termination, N_Days_Overdue
FROM
(
SELECT *
FROM
(
SELECT *
FROM
(
SELECT *
FROM
(SELECT * EXCEPT (CID)
FROM
(SELECT * EXCEPT (CID)
FROM
(SELECT Contract_ID, Currency, SUM(CASE WHEN Payment_Type='Principal' THEN Event_Paid_Amount ELSE 0 END)  AS Principal_Paid,
SUM(CASE WHEN Payment_Type='Interest' THEN Event_Paid_Amount ELSE 0 END)  AS Interest_Paid,
SUM(Event_Paid_Amount) AS Total_Paid
FROM `amret-mf-kh.test_amret_kh.event` 
GROUP BY 1, 2) A
INNER JOIN
(SELECT Contract_ID AS CID, Loan_Code, Disbursement_Date, Maturity_Date, 
Disbursement_Amount, Written_Off_Flag
FROM `amret-mf-kh.test_amret_kh.loan`
WHERE Disbursement_Date>='2018-09-01') B
ON A.Contract_ID=B.CID) AB
LEFT JOIN
(SELECT Contract_ID AS CID, MAX(Early_Termination) AS Early_Termination_Flag
FROM `amret-mf-kh.test_amret_kh.event` 
GROUP BY 1) C
ON AB.Contract_ID=C.CID)
WHERE Early_Termination_Flag='N')
) ABC
LEFT JOIN
(
SELECT CID, 
SUM(Amount_Due) AS Amount_Due, 
SUM(Capital_Amount_Due) AS Principal_Amount_Due, 
SUM(Interest_Amount_Due) AS Interest_Amount_Due
FROM
(
SELECT * EXCEPT (Contract_ID)
FROM
(SELECT Contract_ID AS CID, Payment_Due_Date, 
Amount_Due, Capital_Amount_Due, Interest_Amount_Due
FROM `amret-mf-kh.test_amret_kh.schedule`) H
LEFT JOIN
(SELECT Contract_ID, Loan_Code, Disbursement_Date
FROM `amret-mf-kh.test_amret_kh.loan`) I
ON H.CID=I.Contract_ID)
WHERE Payment_Due_Date>Disbursement_Date
GROUP BY 1
) D
ON ABC.Contract_ID=D.CID) ABCD
LEFT JOIN
(SELECT *
FROM 
(
SELECT Contract_ID AS CID, Update_Date AS LB_Date, 
Principal_Paid AS LB_Principal_Paid,
Interest_Paid AS LB_Interest_Paid, 
Principal_Outstanding  AS Principal_Outstanding, 
Principal_Past_Due AS Principal_Past_Due, 
Interests_Past_Due AS Interests_Past_Due,
Early_Termination, N_Days_Overdue, 
Principal_Past_Due+Interests_Past_Due AS Past_Due,
COUNT(*) OVER (PARTITION BY Contract_ID ORDER BY Update_Date DESC) AS Counter
FROM `amret-mf-kh.test_amret_kh.loan_balance`)
WHERE Counter=1) E
ON ABCD.Contract_ID=E.CID)
WHERE Loan_Code<>'21050'")

mismatches_contracts <- query_exec(mismatches_contracts_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)

wad <-data.table(read.csv(paste0(FoldData,'AmretInstWithWADpositive0.csv'),
                                header=T, quote = '"',stringsAsFactors = FALSE,na.strings=''))
								
wad[,X:=NULL]

wad <- merge(wad, mismatches_contracts, by.x=c("Contract_ID"), by.y=c("Contract_ID"), all.x=T)

wad$Mismatch <- ifelse(is.na(wad2$Mismatch), 4, wad2$Mismatch) ### 4 are contract with Mismatch NULL (Early Terminations)
mytable <- table(wad2$Mismatch)
prop.table(mytable)

#### contracts with payments late (Event=LP)
contracts_late_sql <- paste0("SELECT Contract_ID
FROM `amret-mf-kh.test_amret_kh.event`
WHERE Event='LP'
GROUP BY 1")

contracts_late <- query_exec(contracts_late_sql, project = project, max_pages = Inf, use_legacy_sql = FALSE)
contracts_late$late_flag <- 1

wad <- merge(wad, contracts_late, by.x=c("Contract_ID"), by.y=c("Contract_ID"), all.x=T)
wad$late_flag <- ifelse(is.na(wad$late_flag),0,wad$late_flag)