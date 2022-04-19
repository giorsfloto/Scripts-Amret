SELECT
  R_Dictionary_Record_ID,
  R_Batch_ID,
  R_Batch_DateTime,
  Company_Name,
  Country_Code,
  Field_Name,
  FIeld_Code,
  Field_Description,
  Parent_Field_Name,
  Parent_Field_Code,
  Dictionary_Update_Date
FROM (
  SELECT
    *
  FROM
    `amret-kh.raw.dic_*`) A
INNER JOIN (
  SELECT
    MAX(Dictionary_Update_Date) AS Last_Date
  FROM
    `amret-kh.raw.dic_*`) B
ON
  A.Dictionary_Update_Date=B.Last_Date
  
  
SELECT
  *
FROM (
  SELECT
    R_Currency_Record_ID,
    R_Batch_ID,
    R_Batch_DateTime,
    Quote_Currency_Code,
    Exchange_Rate,
    Company_Name,
    Country_Code,
    Base_Currency_Code,
    Exchange_Rate_Date,
    Currency_Update_DateTime,
    COUNT(*) OVER (PARTITION BY Base_Currency_Code, Quote_Currency_Code, Exchange_Rate_Date ORDER BY Currency_Update_DateTime DESC) AS R_Sequential_Number
  FROM
    `amret-kh.raw.cur_*`)
WHERE
  R_Sequential_Number=1
  
  
SELECT
  TXN.R_Transaction_Record_ID,
  TXN.R_Batch_ID,
  TXN.R_Batch_DateTime,
  TXN.Company_Name,
  TXN.Country_Code,
  TXN.Department_ID,
  TXN.Customer_ID,
  TXN.Customer_Account_ID,
  TXN.Transaction_ID,
  TXN.Operator_ID,
  TXN.Terminal_ID,
  TXN.Counterpart_ID,
  TXN.Counterpart_Account_ID,
  TXN.Channel_Name,
  TXN.Reversal_Flag,
  TXN.Transaction_Type_Code,
  TXN.DbCr_Code,
  TXN.Success_Code,
  TXN.Transaction_DateTime,
  CAST(TXN.Value_Date AS DATE) AS Value_Date,
  TXN.Transaction_Currency_Code,
  TXN.Transaction_Amount,
  TXN.Transaction_Fee_Amount,
  TXN.Transaction_Commission_Amount,
  TXN.Transaction_Latitude,
  TXN.Transaction_Longitude,
  TXN.User_Name,
  TXN.Location_Name,
  TXN.Customer_AccountBalance_Amount,
  TXN.Float_AccountBalance_Amount,
  TXN.Contract_ID,
  TXN.Instalment_Sequence_Number,
  COUNT(*) OVER (PARTITION BY TXN.Transaction_ID ORDER BY TXN.Transaction_DateTime DESC) AS R_Sequential_Number,
  DIC1.Field_Description AS Department_Name,
  DIC2.Field_Description AS Transaction_Type_Name,
  TXN.Transaction_Amount * IFNULL(USD.Exchange_Rate, 1) AS Transaction_Amount_USD,
  TXN.Transaction_Fee_Amount * IFNULL(USD.Exchange_Rate, 1) AS Transaction_Fee_Amount_USD,
  TXN.Transaction_Commission_Amount * IFNULL(USD.Exchange_Rate, 1) AS Transaction_Commission_Amount_USD,
FROM `amret-kh.raw.txn_*` TXN

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='DEPARTMENT_ID' ) DIC1
ON TXN.Department_ID=DIC1.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='TRANSACTION_TYPE_CODE' ) DIC2
ON TXN.Transaction_Type_Code=DIC2.Field_Code

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON TXN.Transaction_Currency_Code=USD.Base_Currency_Code AND CAST(TXN.Transaction_DateTime AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### portfolio_manager
SELECT
  * EXCEPT (Field_Code)
FROM (
  SELECT
    * EXCEPT (Field_Code)
  FROM (
    SELECT
      * EXCEPT (Field_Code)
    FROM (
      SELECT
        * EXCEPT (Field_Code)
      FROM (
        SELECT
          * EXCEPT (Field_Code)
        FROM (
          SELECT
            * EXCEPT (Field_Code)
          FROM (
            SELECT
              * EXCEPT (Field_Code)
            FROM (
              SELECT
                R_PortfolioManager_Record_ID,
                R_Batch_ID,
                R_Batch_DateTime,
                Company_Name,
                Country_Code,
                Company_sublevel1_ID,
                Company_sublevel2_ID,
                Company_sublevel3_ID,
                Department_ID,
                Department_sublevel1_ID,
                Department_sublevel2_ID,
                Department_sublevel3_ID,
                PortfolioManager_ID,
                Customer_ID,
                PortfolioManager_Update_Date,
                PortfolioManager_Area_Code,
                PortfolioManager_Name,
                PortfolioManager_Title_Name,
                PortfolioManager_Status_Name,
                PortfolioManager_User_Name,
                PortfolioManager_Start_Date,
                PortfolioManager_End_Date,
                LEAD(PortfolioManager_Update_Date) OVER (PARTITION BY PortfolioManager_ID ORDER BY PortfolioManager_Update_Date ASC) AS R_PortfolioManager_End_Date,
                COUNT(*) OVER (PARTITION BY PortfolioManager_ID ORDER BY PortfolioManager_Update_Date DESC) AS R_Sequential_Number
              FROM
                `amret-kh.raw.pfm_*` ) A
            LEFT JOIN (
              SELECT
                Field_Code,
                Field_Description AS Department_Name
              FROM
                `amret-mfi-kh.dwh.dictionary`
              WHERE
                UPPER(Field_Name)='DEPARTMENT_ID') B
            ON
              A.Department_ID=B.Field_Code) AB
          LEFT JOIN (
            SELECT
              Field_Code,
              Field_Description AS Company_Sublevel1_Name
            FROM
              `amret-mfi-kh.dwh.dictionary`
            WHERE
              UPPER(Field_Name)='COMPANY_SUBLEVEL1_ID') C
          ON
            AB.Company_sublevel1_ID=C.Field_Code) ABC
        LEFT JOIN (
          SELECT
            Field_Code,
            Field_Description AS Company_Sublevel2_Name
          FROM
            `amret-mfi-kh.dwh.dictionary`
          WHERE
            UPPER(Field_Name)='COMPANY_SUBLEVEL2_ID') D
        ON
          ABC.Company_sublevel2_ID=D.Field_Code) ABCD
      LEFT JOIN (
        SELECT
          Field_Code,
          Field_Description AS Company_Sublevel3_Name
        FROM
          `amret-mfi-kh.dwh.dictionary`
        WHERE
          UPPER(Field_Name)='COMPANY_SUBLEVEL3_ID') E
      ON
        ABCD.Company_sublevel3_ID=E.Field_Code) ABCDE
    LEFT JOIN (
      SELECT
        Field_Code,
        Field_Description AS Department_sublevel1_Name
      FROM
        `amret-mfi-kh.dwh.dictionary`
      WHERE
        UPPER(Field_Name)='DEPARTMENT_SUBLEVEL1_ID') F
    ON
      ABCDE.Department_sublevel1_ID=F.Field_Code) ABCDEF
  LEFT JOIN (
    SELECT
      Field_Code,
      Field_Description AS Department_sublevel2_Name
    FROM
      `amret-mfi-kh.dwh.dictionary`
    WHERE
      UPPER(Field_Name)='DEPARTMENT_SUBLEVEL2_ID') G
  ON
    ABCDEF.Department_sublevel2_ID=G.Field_Code) ABCDEFG
LEFT JOIN (
  SELECT
    Field_Code,
    Field_Description AS Department_sublevel3_Name
  FROM
    `amret-mfi-kh.dwh.dictionary`
  WHERE
    UPPER(Field_Name)='DEPARTMENT_SUBLEVEL3_ID') H
ON
  ABCDEFG.Department_sublevel3_ID=H.Field_Code
  
#### customer
SELECT
  * EXCEPT (Field_Code),
FROM (
  SELECT
    * EXCEPT (Field_Code),
  FROM (
    SELECT
      * EXCEPT (Field_Code),
    FROM (
      SELECT
        * EXCEPT (Field_Code),
        LEAD(Customer_Update_Date) OVER (PARTITION BY Customer_ID ORDER BY Customer_Update_Date ASC) AS R_Customer_End_Date,
        COUNT(*) OVER (PARTITION BY Customer_ID ORDER BY Customer_Update_Date DESC) AS R_Sequential_Number,
      FROM (
        SELECT
          R_Customer_Record_ID,
          R_Batch_ID,
          R_Batch_DateTime,
          Company_Name,
          Country_Code,
          Customer_ID,
          Customer_Update_Date,
          Department_ID,
          Customer_Inputter_ID,
          Customer_Channel_Name,
          PortfolioManager_ID,
          Customer_Area_Code,
          Customer_Creation_Date,
          Gender_Code,
          Customer_Birth_Year,
          Customer_Birth_Month,
          Sector_Code,
          Subsector_Code,
          Marital_Status_Name,
          Customer_Category_Code,
          Customer_Type_Code,
          Customer_Status_Name,
          Banked_Previously_Flag,
          NULL AS Opt_Out_Value
        FROM
          `amret-kh.raw.cus_*`) A
      LEFT JOIN (
        SELECT
          Field_Code,
          Field_Description AS Customer_Area_Name
        FROM
          `amret-mfi-kh.dwh.dictionary`
        WHERE
          UPPER(Field_Name)='CUSTOMER_AREA_CODE') B
      ON
        A.Customer_Area_Code=B.Field_Code) AB
    LEFT JOIN (
      SELECT
        Field_Code,
        Field_Description AS Sector_Name
      FROM
        `amret-mfi-kh.dwh.dictionary`
      WHERE
        UPPER(Field_Name)='SECTOR_CODE') C
    ON
      AB.Sector_Code=C.Field_Code) ABC
  LEFT JOIN (
    SELECT
      Field_Code,
      Field_Description AS Subsector_Name
    FROM
      `amret-mfi-kh.dwh.dictionary`
    WHERE
      UPPER(Field_Name)='SUBSECTOR_CODE') D
  ON
    ABC.Subsector_Code=D.Field_Code) ABCD
LEFT JOIN (
  SELECT
    Field_Code,
    Field_Description AS Department_Name
  FROM
    `amret-mfi-kh.dwh.dictionary`
  WHERE
    UPPER(Field_Name)='DEPARTMENT_ID') E
ON
  ABCD.Department_ID=E.Field_Code
  
#### account_balance
SELECT
  ABA.R_AccountBalance_Record_ID,
  ABA.R_Batch_ID,
  CAST( ABA.R_Batch_DateTime AS Timestamp) AS R_Batch_DateTime,
  ABA.Company_Name,
  ABA.Country_Code,
  ABA.Account_ID,
  ABA.Account_Balance_Date,
  ABA.Customer_ID,
  ABA.Account_Code,
  ABA.Capital_Balance_Amount,
  ABA.Interest_Balance_Amount,
  ABA.Unpaidfee_Balance_Amount,
  ABA.Available_Balance_Amount,
  ABA.Account_Currency_Code,
  COUNT(*) OVER (PARTITION BY ABA.Account_ID ORDER BY ABA.Account_Balance_Date DESC) AS R_Sequential_Number,
  DIC.Field_Description AS Account_Product_Name,
  ABA.Capital_Balance_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Balance_Amount_USD,
  ABA.Interest_Balance_Amount * IFNULL(USD.Exchange_Rate, 1) AS Interest_Balance_Amount_USD,
  ABA.Unpaidfee_Balance_Amount * IFNULL(USD.Exchange_Rate, 1) AS Unpaidfee_Balance_Amount_USD,
  ABA.Available_Balance_Amount * IFNULL(USD.Exchange_Rate, 1) AS Available_Balance_Amount_USD,
FROM `amret-kh.raw.aba_*` ABA

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='ACCOUNT_CODE') DIC
ON ABA.Account_Code=DIC.Field_Code

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON ABA.Account_Currency_Code=USD.Base_Currency_Code AND CAST(ABA.Account_Balance_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### account
SELECT 
  ACC.R_Account_Record_ID,
  ACC.R_Batch_ID,
  ACC.R_Batch_DateTime,
  ACC.Company_Name,
  ACC.Country_Code,
  ACC.Account_ID,
  ACC.Account_Update_Date,
  ACC.Customer_ID,
  ACC.Department_ID,
  ACC.Account_Inputter_ID,
  ACC.Account_Channel_Name,
  ACC.PortfolioManager_ID,
  ACC.Account_Code,
  ACC.Transaction_First_DateTime,
  ACC.Account_Start_Date,
  ACC.Account_End_Date,
  ACC.Account_Maturity_Date,
  ACC.Account_Status_Name,
  ACC.Account_Interest_Rate,
  ACC.Account_Currency_Code,
  ACC.Account_Type_Code,
  ACC.Overdraft_Authorized_Amount,
  LEAD(ACC.Account_Update_Date) OVER (PARTITION BY ACC.Account_ID ORDER BY ACC.Account_Update_Date ASC) AS R_Account_End_Date,
  COUNT(*) OVER (PARTITION BY ACC.Account_ID ORDER BY ACC.Account_Update_Date DESC) AS R_Sequential_Number,
  DIC1.Field_Description AS Department_Name,
  DIC2.Field_Description AS Account_Product_Description,
  ACC.Overdraft_Authorized_Amount * IFNULL(USD.Exchange_Rate, 1) AS Overdraft_Authorized_Amount_USD,
  
FROM `amret-kh.raw.acc_*` ACC

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='DEPARTMENT_ID') DIC1
ON ACC.Department_ID=DIC1.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='ACCOUNT_CODE') DIC2
ON ACC.Account_Code=DIC2.Field_Code


LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON ACC.Account_Currency_Code=USD.Base_Currency_Code AND CAST(ACC.Account_Update_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### loan
SELECT
  LOA.R_Loan_Record_ID,
  LOA.R_Batch_ID,
  LOA.R_Batch_DateTime,
  LOA.Company_Name,
  LOA.Country_Code,
  LOA.Loan_ID,
  LOA.Loan_Update_Date,
  LOA.Customer_ID,
  LOA.Department_ID,
  LOA.Inputter_ID,
  LOA.Authoriser_ID,
  LOA.Channel_Name,
  LOA.PortfolioManager_ID,
  LOA.Loan_Product_Code,
  LOA.Loan_Start_Date,
  LOA.Loan_Disbursement_Date,
  LOA.Loan_End_Date,
  LOA.Loan_Maturity_Date,
  LOA.Net_Disbursed_Amount, 
  LOA.Loan_Purpose_Code,
  LOA.Loan_Process_Code,
  LOA.Loan_Currency_Code,
  LOA.Instalment_Frequency_Code,
  LOA.Instalments_Total_Number,
  LOA.Loan_Cycle_Number,
  LOA.Loan_Interest_Rate,
  LOA.Payment_Type_Name,
  LOA.Loan_Restructuration_Flag,
  LOA.Restructuration_Covid_Flag,
  LOA.Repayment_Account_ID,
  LOA.Loan_Type_Code,
  LOA.Capital_Disbursed_Amount,
  LOA.Effective_Interest_Rate,
  LOA.Guarantors_Number,
  LOA.Guarantors_IDs,
  LOA.R_Recommendation_ID,
  LOA.Loan_Status_Name,
  COUNT(*) OVER (PARTITION BY LOA.Loan_ID ORDER BY LOA.Loan_Update_Date DESC) AS R_Sequential_Number,
  LEAD(LOA.Loan_Update_Date) OVER (PARTITION BY LOA.Loan_ID ORDER BY LOA.Loan_Update_Date ASC) AS R_Loan_End_Date,
  DIC1.Field_Description AS Department_Name,
  DIC2.Field_Description AS Loan_Product_Name,
  LOA.Net_Disbursed_Amount * IFNULL(USD.Exchange_Rate, 1) AS Net_Disbursed_Amount_USD, 
  LOA.Capital_Disbursed_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Disbursed_Amount_USD
FROM `amret-kh.raw.loa_*` LOA

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='DEPARTMENT_ID' ) DIC1
ON LOA.Department_ID=DIC1.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='LOAN_PRODUCT_CODE' ) DIC2
ON LOA.Loan_Product_Code=DIC2.Field_Code

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON LOA.Loan_Currency_Code=USD.Base_Currency_Code AND CAST(LOA.Loan_Update_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### loan_application_core
SELECT 
    LAP.R_LAP_Record_ID,
    LAP.R_Batch_ID,
    LAP.R_Batch_DateTime,
    LAP.Company_Name,
    LAP.Country_Code,
    LAP.Department_ID,
    LAP.Customer_ID,
    LAP.LoanApplication_ID,
    LAP.Inputter_ID,
    LAP.PortfolioManager_ID,
    LAP.LoanApplication_Update_Date,
    LAP.LoanApplication_Stage_Code,
    LAP.Loan_ID,
    LAP.Term_Requested_Code,
    LAP.Term_Reviewed_Code,
    LAP.Term_Approved_Code,
    LAP.LoanApplication_Start_Date,
    LAP.LoanApplication_End_Date,
    LAP.LoanApplication_End_Code,
    LAP.Loan_Currency_Code,
    LAP.LoanApplication_Requested_Amount,
    LAP.LoanApplication_Reviewed_Amount,
    LAP.LoanApplication_Approved_Amount,
    LAP.LoanApplication_Channel_Name,
    LAP.LoanApplication_Purpose_Code,
    LAP.Loan_Code,
    LAP.Loan_Cycle_Number,
    COUNT(*) OVER (PARTITION BY LAP.LoanApplication_ID ORDER BY LAP.LoanApplication_Update_Date DESC) AS R_Sequential_Number,
    DIC1.Field_Description AS LoanApplication_Stage_Name,
    DIC2.Field_Description AS Department_Name,
    DIC3.Field_Description AS LoanApplication_Purpose_Name,
    DIC4.Field_Description AS Loan_Product_Name,
    DIC5.Field_Description AS LoanApplication_End_Name,
    LAP.LoanApplication_Requested_Amount * IFNULL(USD.Exchange_Rate, 1) AS LoanApplication_Requested_Amount_USD,
    LAP.LoanApplication_Reviewed_Amount * IFNULL(USD.Exchange_Rate, 1) AS LoanApplication_Reviewed_Amount_USD,
    LAP.LoanApplication_Approved_Amount * IFNULL(USD.Exchange_Rate, 1) AS LoanApplication_Approved_Amount_USD,
FROM `amret-kh.raw.lap_*` LAP

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='LOANAPPLICATION_STAGE_CODE' ) DIC1
ON LAP.LoanApplication_Stage_Code=DIC1.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='DEPARTMENT_ID' ) DIC2
ON LAP.Department_ID=DIC2.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='LOAN_PURPOSE_CODE' ) DIC3
ON LAP.LoanApplication_Purpose_Code=DIC3.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='LOAN_PRODUCT_CODE' ) DIC4
ON LAP.Loan_Code=DIC4.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='LOANAPPLICATION_END_CODE' ) DIC5
ON LAP.LoanApplication_End_Code=DIC5.Field_Code

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON LAP.Loan_Currency_Code=USD.Base_Currency_Code AND CAST(LAP.LoanApplication_Update_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### schedule
SELECT 
  SCH.R_Schedule_Record_ID, 
  SCH.R_Batch_ID, 
  SCH.R_Batch_DateTime, 
  SCH.Company_Name, 
  SCH.Country_Code, 
  SCH.Department_ID, 
  SCH.Customer_ID, 
  SCH.Contract_ID, 
  SCH.Contract_Type_Code, 
  SCH.Payment_Frequency_Name,
  SCH.Frequency_Code, 
  SCH.Instalment_Sequence_Number, 
  SCH.Payment_Due_Date, 
  SCH.Schedule_Currency_Code,
  SCH.Total_Due_Amount, 
  SCH.Capital_Due_Amount, 
  SCH.Interest_Due_Amount, 
  SCH.Schedule_Update_Date,
  SCH.Instalment_Status_Code, 
  SCH.R_Sequential_Number,
  SCH.Total_Due_Amount * IFNULL(USD.Exchange_Rate, 1) AS Total_Due_Amount_USD,
  SCH.Capital_Due_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Due_Amount_USD,
  SCH.Interest_Due_Amount * IFNULL(USD.Exchange_Rate, 1) AS Interest_Due_Amount_USD, 
FROM (
  SELECT *, COUNT(*) OVER (PARTITION BY Contract_ID, Instalment_Sequence_Number ORDER BY Schedule_Update_Date DESC) AS R_Sequential_Number
  FROM `amret-kh.raw.sch_*` ) SCH
 
INNER JOIN (SELECT Loan_ID FROM `amret-mfi-kh.dwh.loan` WHERE R_Sequential_Number=1) LOA
ON SCH.Contract_ID=LOA.Loan_ID

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON SCH.Schedule_Currency_Code=USD.Base_Currency_Code AND CAST(SCH.Schedule_Update_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### loan_balance
SELECT
  LBA.R_LoanBalance_Record_ID,
  LBA.R_Batch_ID,
  LBA.R_Batch_DateTime,
  LBA.Company_Name,
  LBA.Country_Code,
  LBA.Loan_ID,
  LBA.Loan_Balance_Date,
  LBA.Customer_ID,
  LBA.Loan_Product_Code,
  LBA.Principal_Outstanding_Amount,
  LBA.Principal_Overdue_Amount,
  LBA.Interest_Outstanding_Amount,
  LBA.Interest_Overdue_Amount,
  LBA.Penalty_Overdue_Amount,
  LBA.Loan_Status_Name,
  LBA.Event_Code,
  LBA.LoanBalance_Currency_Code,
  LBA.Days_Overdue_Number,
  COUNT(*) OVER (PARTITION BY LBA.Loan_ID ORDER BY LBA.Loan_Balance_Date DESC) AS R_Sequential_Number,
  LOA.Department_ID,
  LOA.PortfolioManager_ID,
  LOA.Loan_Disbursement_Date,
  LOA.Loan_Maturity_Date,
  LOA.Net_Disbursed_Amount,
  LOA.Capital_Disbursed_Amount,
  LOA.Loan_Cycle_Number,
  LOA.Instalment_Frequency_Code,
  LOA.Instalments_Total_Number,
  LOA.Loan_Restructuration_Flag,
  DIC1.Field_Description AS Loan_Product_Name,
  DIC2.Field_Description AS Department_Name,
  LBA.Principal_Outstanding_Amount * IFNULL(USD.Exchange_Rate, 1) AS Principal_Outstanding_Amount_USD,
  LBA.Principal_Overdue_Amount * IFNULL(USD.Exchange_Rate, 1) AS Principal_Overdue_Amount_USD,
  LBA.Interest_Outstanding_Amount * IFNULL(USD.Exchange_Rate, 1) AS Interest_Outstanding_Amount_USD,
  LBA.Interest_Overdue_Amount * IFNULL(USD.Exchange_Rate, 1) AS Interest_Overdue_Amount_USD,
  LBA.Penalty_Overdue_Amount * IFNULL(USD.Exchange_Rate, 1) AS Penalty_Overdue_Amount_USD,
  LOA.Net_Disbursed_Amount * IFNULL(USD.Exchange_Rate, 1) AS Net_Disbursed_Amount_USD,
  LOA.Capital_Disbursed_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Disbursed_Amount_USD,
FROM `amret-kh.raw.lba_*` LBA

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.loan` WHERE R_Sequential_Number=1) LOA
ON LBA.Loan_ID=LOA.Loan_ID

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='LOAN_PRODUCT_CODE' ) DIC1
ON LBA.Loan_Product_Code=DIC1.Field_Code

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.dictionary` WHERE UPPER(Field_Name)='DEPARTMENT_ID' ) DIC2
ON LOA.Department_ID=DIC2.Field_Code

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON LBA.LoanBalance_Currency_Code=USD.Base_Currency_Code AND CAST(LBA.Loan_Balance_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)

#### event
SELECT
  EVE.R_Event_Record_ID,
  EVE.R_Batch_ID,
  EVE.R_Batch_DateTime,
  EVE.Company_Name,
  EVE.Country_Code,
  EVE.Department_ID,
  EVE.Customer_ID,
  EVE.Contract_ID,
  EVE.Contract_Type,
  EVE.Instalment_Sequence_Number,
  EVE.Transaction_ID,
  EVE.Event_Code,
  EVE.Payment_Due_Date,
  EVE.Event_Currency_Code,
  EVE.Event_DateTime,
  EVE.Total_Paid_Amount,
  EVE.Capital_Paid_Amount,
  EVE.Interest_Paid_Amount,
  EVE.Penalty_Paid_Amount,
  EVE.Capital_Remaining_Amount,
  EVE.Interest_Remaining_Amount,
  EVE.Early_Termination_Flag,
  
  COUNT(*) OVER (PARTITION BY EVE.Contract_ID, EVE.Instalment_Sequence_Number, EVE.Transaction_ID ORDER BY EVE.Event_DateTime DESC) AS R_Sequential_Number,
  EVE.Total_Paid_Amount * IFNULL(USD.Exchange_Rate, 1) AS Total_Paid_Amount_USD,
  EVE.Capital_Paid_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Paid_Amount_USD,
  EVE.Interest_Paid_Amount * IFNULL(USD.Exchange_Rate, 1) AS Interest_Paid_Amount_USD,
  EVE.Penalty_Paid_Amount * IFNULL(USD.Exchange_Rate, 1) AS Penalty_Paid_Amount_USD,
  EVE.Capital_Remaining_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Remaining_Amount_USD,
  EVE.Interest_Remaining_Amount * IFNULL(USD.Exchange_Rate, 1) AS Interest_Remaining_Amount_USD, 
FROM `amret-kh.raw.eve_*` EVE

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON EVE.Event_Currency_Code=USD.Base_Currency_Code AND CAST(EVE.Payment_Due_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE) --- exchange rate of the payment date to compare paid with schedule amount

#### loan_scoring
CREATE OR REPLACE TABLE
  `amret-mfi-kh.d6s_scoring.loan_scoring`
PARTITION BY
  RANGE_BUCKET(Department_ID_Scoring,
    GENERATE_ARRAY(0, 200, 1)) ### partition by Department_ID_Scoring
  AS
SELECT
  ABC.Customer_ID AS Customer_ID,
  Contract_ID,
  Loan_Product_Code,
  Loan_Cycle_Number,
  Capital_Disbursed_Amount,
  Capital_Disbursed_Amount_USD,
  Loan_Disbursement_Date,
  Loan_Maturity_Date,
  Loan_Type_Code,
  Payment_Type_Name,
  Loan_Restructuration_Flag,
  Restructuration_Covid_Flag,
  R_Recommendation_ID,
  Loan_Currency_Code,
  PortfolioManager_ID,
  Loan_Status_Name,
  Loan_Update_Date,
  Loan_End_Date,
  Department_ID_Scoring,
  FALSE AS Optout_Flag,
  CAST(CASE
      WHEN Payment_Type_Name='FLEXIBLE' THEN TRUE
    ELSE
    FALSE
  END
    AS BOOLEAN) AS Variable_Schedule_Flag,
  CASE
    WHEN Loan_Product_Code IN ('21056','3900') THEN TRUE
  ELSE
  FALSE
END
  AS Loan_Staff_Flag,
  Principal_Outstanding_Amount,
  Principal_Outstanding_Amount_USD,
FROM (
  SELECT
    Contract_ID,
    Loan_Product_Code,
    Loan_Cycle_Number,
    Capital_Disbursed_Amount,
    Capital_Disbursed_Amount_USD,
    Loan_Disbursement_Date,
    Loan_Maturity_Date,
    CASE
      WHEN Loan_Status_Name='CLOSE' THEN IFNULL(Last_Payment_Date, Loan_End_Date)
    ELSE
    NULL
  END
    AS Loan_End_Date,
    -- if status is closed, loan_end_date is taken from our calculation from schedule, if that is not available because the loan was not validate we take the one provided by Amret, otherwise if the loan is ACTIVE, loan_end_date is blank
    Customer_ID,
    Loan_Type_Code,
    Payment_Type_Name,
    Loan_Restructuration_Flag,
    Restructuration_Covid_Flag,
    R_Recommendation_ID,
    Loan_Currency_Code,
    PortfolioManager_ID,
    Loan_Status_Name,
    Loan_Update_Date,
    Principal_Outstanding_Amount,
    Principal_Outstanding_Amount_USD,
  FROM (
    SELECT
      Contract_ID,
      Loan_Product_Code,
      Loan_Cycle_Number,
      Capital_Disbursed_Amount,
      Capital_Disbursed_Amount_USD,
      Loan_Disbursement_Date,
      Loan_Maturity_Date,
      Loan_End_Date,
      Customer_ID,
      Loan_Type_Code,
      Payment_Type_Name,
      Loan_Restructuration_Flag,
      Restructuration_Covid_Flag,
      R_Recommendation_ID,
      Loan_Currency_Code,
      PortfolioManager_ID,
      CASE
        WHEN Principal_Outstanding_Amount IS NULL THEN 'CLOSE'
      ELSE
      B.Loan_Status_Name
    END
      AS Loan_Status_Name,
      --- if loan is not between last available balances it is closed, else we take the status from last available balances
      Loan_Update_Date,
      Principal_Outstanding_Amount,
      Principal_Outstanding_Amount_USD,
    FROM (
      SELECT
        Loan_ID AS Contract_ID,
        Loan_Product_Code,
        Loan_Cycle_Number,
        Capital_Disbursed_Amount,
        Capital_Disbursed_Amount_USD,
        Loan_Disbursement_Date,
        Loan_Maturity_Date,
        Loan_End_Date,
        Customer_ID,
        Loan_Type_Code,
        Payment_Type_Name,
        Loan_Restructuration_Flag,
        Restructuration_Covid_Flag,
        R_Recommendation_ID,
        Loan_Currency_Code,
        PortfolioManager_ID,
        Loan_Status_Name,
        Loan_Update_Date
      FROM
        `amret-mfi-kh.dwh.loan`
      WHERE
        R_Sequential_Number=1) A
    LEFT JOIN (
      SELECT
        * EXCEPT (Loan_Balance_Date,
          Last_Available_Date)
      FROM (
        SELECT
          Loan_ID,
          Principal_Outstanding_Amount,
          Principal_Outstanding_Amount_USD,
          Loan_Status_Name,
          Loan_Balance_Date,
          MAX(Loan_Balance_Date) OVER() AS Last_Available_Date
        FROM
          `amret-mfi-kh.dwh.loan_balance`)
      WHERE
        Loan_Balance_Date=Last_Available_Date) B	-- we select last available loan balances
    ON
      A.Contract_ID=B.Loan_ID) AB
  LEFT JOIN (
    SELECT
      Contract_ID AS CID,
      CAST(MAX(Event_DateTime) AS DATE) AS Last_Payment_Date
    FROM
      `amret-mfi-kh.dwh.event`
    GROUP BY
      1) C
  ON
    AB.Contract_ID=C.CID) ABC
LEFT JOIN (
  SELECT
    Customer_ID,
    Department_ID,
    Department_ID_Scoring
  FROM (
    SELECT
      * EXCEPT (Counter)
    FROM (
      SELECT
        Customer_ID,
        Department_ID,
        COUNT(*) OVER (PARTITION BY Customer_ID ORDER BY Capital_Disbursed_Amount_USD DESC, Loan_ID) AS Counter -- counter=1 for highest amount contract for each customer
      FROM (
        SELECT
          Customer_ID,
          Department_ID,
          Capital_Disbursed_Amount_USD,
          Loan_ID
        FROM
          `amret-mfi-kh.dwh.loan`
        WHERE
          R_Sequential_Number=1))
    WHERE
      Counter=1) D
  LEFT JOIN (
    SELECT
      Department_ID AS T24_Code,
      Department_ID_Scoring
    FROM
      `amret-mfi-kh.d6s_scoring.department_scoring`) E
  ON
    D.Department_ID=E.T24_Code) DE
ON
  ABC.Customer_ID=DE.Customer_ID
  
### schedule_scoring
CREATE OR REPLACE TABLE `amret-mfi-kh.d6s_scoring.schedule_scoring`
PARTITION BY RANGE_BUCKET(Department_ID_Scoring, GENERATE_ARRAY(0, 200, 1)) ### partition by Department_ID_Scoring
AS
SELECT 
  SCH.Customer_ID,
  SCH.Contract_ID,
  SCH.Instalment_Sequence_Number, 
  SCH.Payment_Due_Date,
  SCH.Schedule_Currency_Code,
  SCH.Total_Due_Amount,
  SCH.Capital_Due_Amount,
  DEP.Department_ID_Scoring,
  LOA.Loan_Product_Code,
  LOA.Loan_Type_Code,
  IFNULL(LBA.Loan_Status_Name, LOA.Loan_Status_Name) AS Loan_Status_Name,
  SCH.Total_Due_Amount * IFNULL(USD.Exchange_Rate, 1) AS Total_Due_Amount_USD,
  SCH.Capital_Due_Amount * IFNULL(USD.Exchange_Rate, 1) AS Capital_Due_Amount_USD, 
FROM (SELECT *
     FROM `amret-mfi-kh.dwh.schedule` 
     WHERE R_Sequential_Number = 1 AND (Instalment_Status_Code IS NULL OR UPPER(Instalment_Status_Code)<>'SKIPPED')) SCH

LEFT JOIN (SELECT * FROM `amret-mfi-kh.dwh.loan` WHERE R_Sequential_Number=1) LOA
ON SCH.Contract_ID=LOA.Loan_ID

LEFT JOIN (
  SELECT Customer_ID, Department_ID
  FROM (
    SELECT Customer_ID, Department_ID,
      COUNT(*) OVER (PARTITION BY Customer_ID ORDER BY Capital_Disbursed_Amount DESC, Loan_ID) AS Counter ## counter=1 for highest amount contract for each customer
    FROM (
      SELECT Customer_ID, Department_ID, Capital_Disbursed_Amount, Loan_ID
      FROM `amret-mfi-kh.dwh.loan`
      WHERE R_Sequential_Number=1))
  WHERE Counter=1) CUS
ON LOA.Customer_ID=CUS.Customer_ID

LEFT JOIN (SELECT Department_ID, Department_ID_Scoring FROM `amret-mfi-kh.d6s_scoring.department_scoring`) DEP
ON CUS.Department_ID=DEP.Department_ID

LEFT JOIN (
  SELECT * FROM (SELECT * FROM `amret-mfi-kh.dwh.loan_balance`) A
  INNER JOIN (SELECT MAX(Loan_Balance_Date) AS Last_Date FROM `amret-mfi-kh.dwh.loan_balance`) B
  ON A.Loan_Balance_Date=B.Last_Date
) LBA
ON LOA.Loan_ID=LBA.Loan_ID

LEFT JOIN (SELECT * FROM  `amret-mfi-kh.dwh.currency` WHERE Quote_Currency_Code='USD') USD
ON SCH.Schedule_Currency_Code=USD.Base_Currency_Code AND CAST(SCH.Schedule_Update_Date AS DATE)=CAST(USD.Exchange_Rate_Date AS DATE)
