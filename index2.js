const {BigQuery} = require('@google-cloud/bigquery');
const bigquery = new BigQuery();

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: As a portfolio manager, I can see my historical performance
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.historyBoxPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.history_box_pm`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT R_Record_ID, R_DateTime, Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY) AS Update_Date
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) A
  LEFT JOIN
  (SELECT PortfolioManager_ID AS PM_ID, MIN(PortfolioManager_Update_Date) AS Start_Date ### temporary fix because in Bank B we don't have Start_Date
  FROM ${tableQueryHierarchy}
  GROUP BY 1) B
  ON A.PortfolioManager_ID=B.PM_ID) AB
  LEFT JOIN
  (SELECT Portfolio_Manager_ID AS PM_ID, COUNT(*) as Disbursed_Loans,
  SUM(Disbursement_Amount) AS Disbursed_Amount
  FROM
  (SELECT Portfolio_Manager_ID, Loan_ID, Disbursement_Amount,
  COUNT(*) OVER (PARTITION BY Loan_ID ORDER BY Update_Date) AS Counter # flag equal to 1 for first available info about the loan (a loan could be reassigned after disbursement)
  FROM ${tableQueryLoan})
  WHERE Counter=1 
  GROUP BY 1) C
  ON AB.PortfolioManager_ID=C.PM_ID) ABC
  LEFT JOIN
  (SELECT PM_ID, AVG(PAR0_Percentage) AS Average_PAR0_Percentage,
  AVG(PAR30_Percentage) AS Average_PAR30_Percentage
  FROM
  (SELECT PM_ID, Date, 
  SUM(PAR0_Volume)/SUM(Principal_Outstanding) AS PAR0_Percentage,
  SUM(PAR30_Volume)/SUM(Principal_Outstanding) AS PAR30_Percentage
  FROM
  (SELECT Portfolio_Manager_ID AS PM_ID, Date, 
  SUM(CASE WHEN Days_Overdue>0 THEN Principal_Outstanding ELSE 0 END) AS PAR0_Volume,
  SUM(CASE WHEN Days_Overdue>30 THEN Principal_Outstanding ELSE 0 END) AS PAR30_Volume, 
  SUM(Principal_Outstanding) AS Principal_Outstanding
  FROM ${tableQueryLoanBalance} ## we have data only since Nov 2019
  WHERE Loan_Status='Active'
  GROUP BY 1, 2)
  WHERE Principal_Outstanding>0
  GROUP BY 1, 2)
  GROUP BY 1) D
  ON ABC.PortfolioManager_ID=D.PM_ID
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `history_box_pm`},
    writeDisposition: `WRITE_APPEND`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy}, ${tableQueryLoan}, and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: As a portfolio manager, I can see my historical performance stats
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.summaryStatsPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.summary_stats_pm`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryLoanApplicationScore = `${projectIdQuery}.dwh.loan_application_core`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;
  const tableQueryScoring = `${projectIdQuery}.dwh.scoring`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID, Date)
  FROM
  (SELECT * EXCEPT (PM_ID)
  FROM 
  (SELECT * EXCEPT (PM_ID)
  FROM 
  (SELECT R_Record_ID, R_DateTime, Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY) AS Update_Date
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) A
  LEFT JOIN
  (SELECT Portfolio_Manager_ID AS PM_ID, COUNT(*) as Disbursed_Loans,
  SUM(Disbursement_Amount) AS Disbursed_Amount
  FROM
  (SELECT Portfolio_Manager_ID, Loan_ID, Disbursement_Amount,
  COUNT(*) OVER (PARTITION BY Loan_ID ORDER BY Update_Date) AS Counter # flag equal to 1 for first available info about the loan (a loan could be reassigned after disbursement)
  FROM ${tableQueryLoan}
  WHERE Disbursement_Date>=DATETIME_TRUNC(CURRENT_DATETIME(),MONTH))
  WHERE Counter=1 
  GROUP BY 1) B
  ON A.PortfolioManager_ID=B.PM_ID) AB
  LEFT JOIN
  (SELECT Portfolio_Manager_ID AS PM_ID, COUNT(*) as Loan_Applications, SUM(Amount_Reviewed) AS Amount_Reviewed
  FROM ${tableQueryLoanApplicationScore}
  WHERE Version=1 AND Stage NOT IN ('Disbursed','Closed')
  GROUP BY 1) C
  ON AB.PortfolioManager_ID=C.PM_ID) ABC
  LEFT JOIN
  (SELECT PM_ID, Date, SUM(Outstanding_Loans) AS Outstanding_Loans, 
  SUM(Principal_Outstanding) AS Principal_Outstanding,
  SUM(PAR0_Outstanding) AS PAR0_Outstanding,
  SUM(PAR30_Outstanding) AS PAR30_Outstanding,
  SUM(A_Outstanding) AS A_Outstanding,
  SUM(B_Outstanding) AS B_Outstanding,
  SUM(C_Outstanding) AS C_Outstanding,
  SUM(D_Outstanding) AS D_Outstanding,
  SUM(Other_Outstanding) AS Other_Outstanding,
  SUM(CASE WHEN Scoring_Class='A' THEN 1 ELSE 0 END) AS A_Customers,
  SUM(CASE WHEN Scoring_Class='B' THEN 1 ELSE 0 END) AS B_Customers,
  SUM(CASE WHEN Scoring_Class='C' THEN 1 ELSE 0 END) AS C_Customers,
  SUM(CASE WHEN Scoring_Class='D' THEN 1 ELSE 0 END) AS D_Customers,
  SUM(CASE WHEN Scoring_Class NOT IN ('A','B','C','D') OR Scoring_Class IS NULL THEN 1 ELSE 0 END) AS Other_Customers,
  FROM
  (SELECT PM_ID, Customer_ID, Scoring_Class, Date,
  COUNT(*) AS Outstanding_Loans, 
  SUM(Principal_Outstanding) AS Principal_Outstanding, 
  SUM(CASE WHEN Days_Overdue>0 THEN Principal_Outstanding ELSE 0 END) AS PAR0_Outstanding,
  SUM(CASE WHEN Days_Overdue>30 THEN Principal_Outstanding ELSE 0 END) AS PAR30_Outstanding, 
  SUM(CASE WHEN Scoring_Class='A' THEN Principal_Outstanding ELSE 0 END) AS A_Outstanding, 
  SUM(CASE WHEN Scoring_Class='B' THEN Principal_Outstanding ELSE 0 END) AS B_Outstanding, 
  SUM(CASE WHEN Scoring_Class='C' THEN Principal_Outstanding ELSE 0 END) AS C_Outstanding, 
  SUM(CASE WHEN Scoring_Class='D' THEN Principal_Outstanding ELSE 0 END) AS D_Outstanding, 
  SUM(CASE WHEN Scoring_Class NOT IN ('A','B','C','D') OR Scoring_Class IS NULL THEN Principal_Outstanding ELSE 0 END) AS Other_Outstanding,
  FROM
  (SELECT * EXCEPT (CID, Scoring_Date)
  FROM
  (SELECT Loan_ID, Portfolio_Manager_ID AS PM_ID, Customer_ID,
  DATETIME_TRUNC(R_DateTime, DAY) AS Date, Principal_Outstanding, Days_Overdue
  FROM ${tableQueryLoanBalance}
  WHERE Loan_Status='Active') D
  LEFT JOIN
  (SELECT Customer_ID AS CID, Scoring_Date, MAX(Class) AS Scoring_Class
  FROM ${tableQueryScoring}
  GROUP BY 1, 2) E
  ON D.Customer_ID=E.CID AND D.Date=E.Scoring_Date) 
  GROUP BY 1, 2, 3, 4)
  GROUP BY 1, 2) DE
  ON ABC.PortfolioManager_ID=DE.PM_ID AND CAST(ABC.Update_Date AS DATETIME)=DE.Date
  
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `summary_stats_pm`},
    writeDisposition: `WRITE_APPEND`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy}, ${tableQueryLoan}, ${tableQueryScoring}, ${tableQueryLoanApplicationScore} and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: disbursementsSummaryBox
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.disbursementsSummaryBox = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.disbursements_summary_box`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryLoanApplicationScore = `${projectIdQuery}.dwh.loan_application_core`;
  const tableQueryObjective = `${projectIdQuery}.dwh.objective`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
 SELECT * EXCEPT (PM_ID, Obj_Date), 
IFNULL(Rzd_Disbursed_Loans,0)+IFNULL(Loan_Applications,0) AS Pot_Disbursed_Loans,
IFNULL(Rzd_Disbursed_Amount,0)+IFNULL(Amount_Reviewed,0) AS Pot_Disbursed_Amount,
IFNULL(Rzd_Cycle1_Loans,0)+IFNULL(Cycle1_Loan_Applications,0) AS Pot_Cycle1_Loans,
CASE WHEN Obj_Disbursed_Loans>0 THEN Rzd_Disbursed_Loans/Obj_Disbursed_Loans END AS Rzd_Disbursed_Loans_Perc,
CASE WHEN Obj_Disbursed_Amount>0 THEN Rzd_Disbursed_Amount/Obj_Disbursed_Amount END AS Rzd_Disbursed_Amount_Perc,
CASE WHEN Obj_Cycle1_Disbursed_Loans>0 THEN Rzd_Cycle1_Loans/Obj_Cycle1_Disbursed_Loans END AS Rzd_Cycle1_Loans_Perc
FROM
(SELECT * EXCEPT (PM_ID), NULL AS Cycle1_Loan_Applications
FROM 
(SELECT * EXCEPT (PM_ID)
FROM 
(SELECT R_Record_ID, R_DateTime, Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY) AS Update_Date
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) A
LEFT JOIN
(SELECT Portfolio_Manager_ID AS PM_ID, COUNT(*) as Rzd_Disbursed_Loans,
SUM(Disbursement_Amount) AS Rzd_Disbursed_Amount,
SUM(CASE WHEN Cycle=1 THEN 1 ELSE 0 END) AS Rzd_Cycle1_Loans
FROM
(SELECT Portfolio_Manager_ID, Loan_ID, Disbursement_Amount, Cycle,
COUNT(*) OVER (PARTITION BY Loan_ID ORDER BY Update_Date) AS Counter # flag equal to 1 for first available info about the loan (a loan could be reassigned after disbursement)
FROM ${tableQueryLoan}
WHERE Disbursement_Date>=DATETIME_TRUNC(CURRENT_DATETIME(),MONTH))
WHERE Counter=1 
GROUP BY 1) B
ON A.PortfolioManager_ID=B.PM_ID) AB
LEFT JOIN
(SELECT Portfolio_Manager_ID AS PM_ID, COUNT(*) as Loan_Applications, SUM(Amount_Reviewed) AS Amount_Reviewed
FROM ${tableQueryLoanApplicationScore} 
WHERE Version=1 AND Stage NOT IN ('Disbursed','Closed')
GROUP BY 1) C
ON AB.PortfolioManager_ID=C.PM_ID) ABC 
LEFT JOIN
(SELECT Portfolio_Manager_ID AS PM_ID, 
CAST(DATETIME_ADD(Month_Last_Date, INTERVAL 1 YEAR) AS TIMESTAMP) AS Obj_Date, ### using 2019 objectives
New_Disbursements AS Obj_Disbursed_Loans,
Total_Disbursement AS Obj_Disbursed_Amount, New_Borrowers AS Obj_Cycle1_Disbursed_Loans
FROM ${tableQueryObjective} 
) D
ON ABC.PortfolioManager_ID=D.PM_ID 
AND EXTRACT(YEAR FROM ABC.Update_Date)=EXTRACT(YEAR FROM D.Obj_Date)
AND EXTRACT(MONTH FROM ABC.Update_Date)=EXTRACT(MONTH FROM D.Obj_Date)
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `disbursements_summary_box`},
    writeDisposition: `WRITE_APPEND`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy}, ${tableQueryLoan}, ${tableQueryObjective} and ${tableQueryLoanApplicationScore} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: disbursementsDetails
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.disbursementsDetails = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.disbursements_details`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID) 
  FROM
  (SELECT Portfolio_Manager_ID AS PortfolioManager_ID, Customer_ID, Disbursement_Date, Loan_ID, Cycle, Maturity_Date, Instalments_Number,
  Disbursement_Amount AS Total_Amount, R_Preconisation_ID, DATETIME_TRUNC(CURRENT_DATETIME(),DAY) AS Update_Date, 
  Currency
  FROM ${tableQueryLoan}
  WHERE Disbursement_Date>=DATETIME_TRUNC(CURRENT_DATETIME(),MONTH) AND Version=1) A
  LEFT JOIN
  (SELECT R_Record_ID, R_DateTime, Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) B
  ON A.PortfolioManager_ID=B.PM_ID
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `disbursements_details`},
    writeDisposition: `WRITE_APPEND`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy} and ${tableQueryLoan} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: pipelineDetailsPM
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.pipelineDetailsPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.pipeline_details_pm`;
  
  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanApplicationScore = `${projectIdQuery}.dwh.loan_application_core`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT R_Record_ID, R_DateTime, Portfolio_Manager_ID AS PortfolioManager_ID, Customer_ID, Loan_Application_ID, Opening_Date,
  Stage, Update_Date, Amount_Requested, Amount_Reviewed, Amount_Approved, Currency,
  DATETIME_TRUNC(CURRENT_DATETIME(),DAY) AS Date
  FROM ${tableQueryLoanApplicationScore}
  WHERE Version=1 AND Stage NOT IN ('Disbursed','Closed')) A 
  LEFT JOIN
  (SELECT Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) B
  ON A.PortfolioManager_ID=B.PM_ID
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `pipeline_details_pm`},
    writeDisposition: `WRITE_APPEND`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy} and ${tableQueryLoanApplicationScore} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: arrearsSummaryBox
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.arrearsSummaryBox = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.arrears_summary_box`;
  
  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT R_Record_ID, R_DateTime, Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name, TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(),DAY) AS Update_Date
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) A
  LEFT JOIN
  (SELECT *, 
  CASE WHEN Principal_Outstanding>0 THEN PAR0_Amount/Principal_Outstanding END AS PAR0_Perc,
  CASE WHEN Principal_Outstanding>0 THEN PAR7_Amount/Principal_Outstanding END AS PAR7_Perc,
  CASE WHEN Principal_Outstanding>0 THEN PAR30_Amount/Principal_Outstanding END AS PAR30_Perc,
  CASE WHEN Principal_Outstanding>0 THEN PAR60_Amount/Principal_Outstanding END AS PAR60_Perc,
  CASE WHEN Principal_Outstanding>0 THEN PAR120_Amount/Principal_Outstanding END AS PAR120_Perc
  FROM
  (SELECT Portfolio_Manager_ID AS PM_ID, Date,
  SUM(CASE WHEN Days_Overdue>0 THEN 1 ELSE 0 END) AS Arrear_Loans, 
  SUM(CASE WHEN Days_Overdue>0 THEN Principal_Outstanding ELSE 0 END) AS Arrear_Amount, 
  SUM(CASE WHEN Days_Overdue>0 THEN 1 ELSE 0 END) AS PAR0_Number, 
  SUM(CASE WHEN Days_Overdue>7 THEN 1 ELSE 0 END) AS PAR7_Number, 
  SUM(CASE WHEN Days_Overdue>30 THEN 1 ELSE 0 END) AS PAR30_Number, 
  SUM(CASE WHEN Days_Overdue>60 THEN 1 ELSE 0 END) AS PAR60_Number, 
  SUM(CASE WHEN Days_Overdue>120 THEN 1 ELSE 0 END) AS PAR120_Number,
  SUM(CASE WHEN Days_Overdue>0 THEN Principal_Outstanding ELSE 0 END) AS PAR0_Amount,
  SUM(CASE WHEN Days_Overdue>7 THEN Principal_Outstanding ELSE 0 END) AS PAR7_Amount,
  SUM(CASE WHEN Days_Overdue>30 THEN Principal_Outstanding ELSE 0 END) AS PAR30_Amount,
  SUM(CASE WHEN Days_Overdue>60 THEN Principal_Outstanding ELSE 0 END) AS PAR60_Amount,
  SUM(CASE WHEN Days_Overdue>120 THEN Principal_Outstanding ELSE 0 END) AS PAR120_Amount, 
  SUM(Principal_Outstanding) AS Principal_Outstanding
  FROM ${tableQueryLoanBalance}
  WHERE Loan_Status='Active'
  GROUP BY 1, 2)) B
  ON A.PortfolioManager_ID=B.PM_ID
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `arrears_summary_box`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy} and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: arrearsDetailsPM
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.arrearsDetailsPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.arrears_details_pm`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT R_Record_ID, R_DateTime, Portfolio_Manager_ID AS PortfolioManager_ID, Date, Loan_ID, Days_Overdue,Principal_Due, Penalty_Due, Principal_Outstanding, 
  FROM ${tableQueryLoanBalance}
  WHERE Loan_Status='Active' AND Days_Overdue>0
  ORDER BY Date DESC, Loan_ID) A
  LEFT JOIN
  (SELECT Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) B
  ON A.PortfolioManager_ID=B.PM_ID
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `arrears_details_pm`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the queries as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy} and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: outstandingSummaryBox
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.outstandingSummaryBox = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.outstanding_summary_box`;
  
  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name, Date, Loan_Name, COUNT(*) as Outstanding_Loans, 
  SUM(Principal_Outstanding) AS Amount
  FROM
  (SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT R_Record_ID, R_DateTime, Portfolio_Manager_ID AS PortfolioManager_ID, 
  Date, Loan_ID, Principal_Outstanding, Loan_Name
  FROM ${tableQueryLoanBalance}
  WHERE Loan_Status='Active'
  ORDER BY Date DESC, Loan_ID) A
  LEFT JOIN
  (SELECT Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) B
  ON A.PortfolioManager_ID=B.PM_ID) 
  GROUP BY Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name, Date, Loan_Name
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `outstanding_summary_box`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy} and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: maturingSummaryBox
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.maturingSummaryBox = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.maturing_summary_box`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryEligible = `${projectIdQuery}.dwh.eligible`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT PortfolioManager_ID, Update_Date, Days_To_Maturity, 
  COUNT(*) AS Maturing_Loans, SUM(Principal_Outstanding) AS Maturing_Amount, 
  SUM(RA_Eligible) AS RA_Loans, 
  SUM(CASE WHEN RA_Eligible=1 THEN Principal_Outstanding ELSE 0 END) AS RA_Amount
  FROM
  (
  SELECT * EXCEPT (Current_Loan_ID), DATETIME_DIFF(Maturity_Date,Update_Date, DAY) AS Days_To_Maturity
  FROM 
  (SELECT * EXCEPT (LID)
  FROM
  (SELECT Loan_ID, Portfolio_Manager_ID AS PortfolioManager_ID, 
  Disbursement_Amount, Principal_Outstanding, 
  Date, DATETIME_TRUNC(R_DateTime, DAY) AS Update_Date, 
  FROM ${tableQueryLoanBalance} 
  WHERE Maturity_Date>Date AND Loan_Status='Active') A
  LEFT JOIN
  (SELECT Loan_ID AS LID, Maturity_Date
  FROM ${tableQueryLoan} 
  WHERE Version=1) B
  ON A.Loan_ID=B.LID) AB
  LEFT JOIN
  (SELECT Current_Loan_ID, Scoring_Date, 1 as RA_Eligible, New_Loan_Amount
  FROM  ${tableQueryEligible}) C
  ON AB.Loan_ID=C.Current_Loan_ID AND AB.Update_Date=C.Scoring_Date)
  GROUP BY 1, 2, 3) ABC
  LEFT JOIN
  (SELECT Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) D ## last information available for the PM_ID
  ON ABC.PortfolioManager_ID=D.PM_ID
  `;

  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `maturing_summary_box`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy}, ${tableQueryEligible} and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: maturingDetailsPM
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.maturingDetailsPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.maturing_details_pm`;
  
  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryEligible = `${projectIdQuery}.dwh.eligible`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

  // Define query
  const query =     
  `
  SELECT * EXCEPT (PM_ID)
  FROM
  (SELECT * EXCEPT (Current_Loan_ID), DATETIME_DIFF(Maturity_Date,Update_Date, DAY) AS Days_To_Maturity
  FROM 
  (SELECT * EXCEPT (LID)
  FROM
  (SELECT R_Record_ID, Loan_ID, Portfolio_Manager_ID AS PortfolioManager_ID,
  Cycle, Disbursement_Amount, Principal_Outstanding, 
  Date, DATETIME_TRUNC(R_DateTime, DAY) AS Update_Date, 
  FROM ${tableQueryLoanBalance} 
  WHERE Maturity_Date>Date AND Loan_Status='Active') A
  LEFT JOIN
  (SELECT Loan_ID AS LID, Maturity_Date, Customer_ID
  FROM ${tableQueryLoan}
  WHERE Version=1) B
  ON A.Loan_ID=B.LID) AB
  LEFT JOIN
  (SELECT Current_Loan_ID, Scoring_Date, "Y" as RA_Eligible, New_Loan_Amount
  FROM ${tableQueryEligible}) C
  ON AB.Loan_ID=C.Current_Loan_ID AND AB.Update_Date=C.Scoring_Date) ABC
  LEFT JOIN
  (SELECT Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) D
  ON ABC.PortfolioManager_ID=D.PM_ID
  `;
  
    // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `maturing_details_pm`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy}, ${tableQueryEligible} and ${tableQueryLoanBalance} loaded to table ${tableUpdate}`);
}


/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: disbursementsdailyPM
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.disbursementsdailyPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.disbursements_daily_pm`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoan = `${projectIdQuery}.dwh.loan`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;

// Define query
  const query =     
  `
 SELECT * EXCEPT (PM_ID) 
  FROM
  (SELECT PortfolioManager_ID, CAST(Disbursement_Date AS DATE) AS Disbursement_Date, COUNT(*) as Disbursed_Loans,
   SUM(Disbursement_Amount) AS Disbursed_Amount
   FROM
  (SELECT Portfolio_Manager_ID AS PortfolioManager_ID, Loan_ID, Disbursement_Date, 
   Disbursement_Amount,
   COUNT(*) OVER (PARTITION BY Loan_ID ORDER BY Update_Date) AS Counter # flag equal to 1 for first available info about the loan (a loan could be reassigned after    disbursement)
  FROM ${tableQueryLoan})
   WHERE Counter=1 
   GROUP BY 1, 2) A
  LEFT JOIN
  (SELECT R_Record_ID, R_DateTime, Company, Country, Company_Sublevel1_ID, 
  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
  Department_Sublevel1_ID, Department_Sublevel1_Name, 
  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
  PortfolioManager_Title_Name
  FROM ${tableQueryHierarchy}
  WHERE R_Version=1) B
  ON A.PortfolioManager_ID=B.PM_ID
  `;
  
    // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `disbursements_daily_pm`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy} and ${tableQueryLoan} loaded to table ${tableUpdate}`);
}

/**
 * Background Cloud Function to be triggered by Pub/Sub.
 * This function is exported by index.ts, and executed when
 * the trigger topic receives a message.
 *
 * SERVICE: disbursementsdailyPM
 * 
 * @param {object} data The event payload.
 * @param {object} context The event metadata.
 */
exports.disbursementsdailyPM = async (data, context) => {
  // Read and parse pubsub message
  const pubSubMessage = data;
  var payload = JSON.parse(Buffer.from(pubSubMessage.data, 'base64').toString());

  // Define IDs of table to update
  const projectIdUpdate = payload.resource.labels.project_id;
  const tableUpdate = `${projectIdUpdate}.service.disbursements_daily_pm`;

  // Define IDs of table to query
  const projectIdQuery = payload.resource.labels.project_id;
  const tableQueryLoanBalance = `${projectIdQuery}.dwh.loan_balance`;
  const tableQueryAccountBalance = `${projectIdQuery}.dwh.account_balance`;
  const tableQueryHierarchy = `${projectIdQuery}.service.hierarchy`;
  const tableQueryCustomer = `${projectIdQuery}.dwh.customer`;

// Define query
  const query =     
  `
	SELECT * EXCEPT (CID,Borrower,LateOnTime), 
	Principal_Outstanding_Amount+Interests_Outstanding_Amount AS Total_Outstanding_Amount
	FROM
	(SELECT * EXCEPT (CID)
	FROM
	(SELECT * EXCEPT (PM_ID)
	FROM
	(SELECT Customer_ID, Portfolio_Manager_ID, Inputter_ID, Channel, Creation_Date, Gender, Age, Activity_Code, 
	Sector_Code, Subsector_Code, Marital_Status, Update_Date, Customer_Class, Customer_Type_Code, 
	Customer_Status, Previously_Banked_Flag, Opt_Out, End_Date, Version, Activity, Sector, 
	Subsector, Customer_Class_Name
	FROM ${tableQueryCustomer}
	WHERE Version=1) A
	LEFT JOIN
	(SELECT Company, Country, Company_Sublevel1_ID, 
	  Company_Sublevel1_Name, Company_Sublevel2_ID, Company_Sublevel2_Name, 
	  Company_Sublevel3_ID, Company_Sublevel3_Name, Department_ID, Department_Name, 
	  Department_Sublevel1_ID, Department_Sublevel1_Name, 
	  Department_Sublevel2_ID, Department_Sublevel2_Name, Department_Sublevel3_ID, 
	  Department_Sublevel3_Name, PortfolioManager_ID AS PM_ID, PortfolioManager_Name, 
	  PortfolioManager_Title_Name
	  FROM ${tableQueryHierarchy}
	  WHERE R_Version=1) B
	ON A.Portfolio_Manager_ID=B.PM_ID) AB
	LEFT JOIN
	(SELECT C.Customer_ID AS CID, Account_Update_Date, Capital_Balance_Amount, Available_Balance_Amount
	FROM
	(SELECT Customer_ID, Account_Update_Date, SUM(Capital_Balance_Amount) AS Capital_Balance_Amount, 
	SUM(Available_Balance_Amount) AS Available_Balance_Amount
	FROM ${tableQueryAccountBalance}
	GROUP BY 1, 2) C	
	INNER JOIN
	(SELECT Customer_ID, MAX(Account_Update_Date) AS Last_Date
	FROM ${tableQueryAccountBalance}
	GROUP BY 1) d
	ON C.Customer_ID=D.Customer_ID AND C.Account_Update_Date=D.Last_Date) CD
	ON AB.Customer_ID=CD.CID) ABCD
	LEFT JOIN
	(SELECT *, [Borrower, LateOnTime] AS Tag 
	FROM
	(
	SELECT E.Customer_ID AS CID, Loan_Update_Date, Loans_Outstanding_Number, 
	Principal_Outstanding_Amount, Principal_Due_Amount, Interests_Outstanding_Amount, 
	Interests_Due_Amount, Penalty_Due_Amount,
	CASE WHEN Loans_Outstanding_Number>0 THEN 'Borrower' ELSE NULL END AS Borrower,
	CASE WHEN Principal_Due_Amount+Interests_Outstanding_Amount>0 THEN 'Late' ELSE 'On Time' END AS LateOnTime
	FROM
	(SELECT Customer_ID, Update_Date AS Loan_Update_Date, COUNT(*) as Loans_Outstanding_Number, 
	SUM(Principal_Outstanding) AS Principal_Outstanding_Amount, SUM(Principal_Due) AS Principal_Due_Amount, 
	SUM( Interests_Outstanding) AS Interests_Outstanding_Amount, SUM( Interests_Due) AS Interests_Due_Amount, 
	SUM(Penalty_Due) AS Penalty_Due_Amount
	FROM ${tableQueryLoanBalance} 
	WHERE Loan_Status='Active'
	GROUP BY 1, 2) E
	INNER JOIN
	(SELECT Customer_ID, MAX(Update_Date) AS Last_Date
	FROM ${tableQueryLoanBalance} 
	GROUP BY 1) F
	ON E.Customer_ID=F.Customer_ID AND E.Loan_Update_Date=F.Last_Date)) EF
	 ON ABCD.Customer_ID=EF.CID
 `;
 
 
  // Define query options
  const options = {
    query: query,
    destinationTable: {projectId: projectIdUpdate, datasetId: `service` , tableId: `customer`},
    writeDisposition: `WRITE_TRUNCATE`,
    timeoutMs: 100000, // Time out after 100 seconds.
    useLegacySql: false, // Use standard SQL syntax for queries.
  };

  // Run the query as a job
  const [job] = await bigquery.createQueryJob(options);

  // Generate log
  console.log(`Job ${job.id} started.`);
  console.log(`Query results from table ${tableQueryHierarchy}, ${tableQueryLoanBalance}, ${tableQueryAccountBalanceBalance} and ${tableQueryCustomer} loaded to table ${tableUpdate}`);
}
