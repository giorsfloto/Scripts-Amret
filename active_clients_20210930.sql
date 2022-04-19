CREATE OR REPLACE TABLE `amret-kh.raw_sandbox.active_customers`
AS
WITH vars AS (
  SELECT '2021-09-30' as date
)

SELECT date, COUNT(DISTINCT Customer_ID) as customers
        FROM
		(
                SELECT Customer_ID, date
                    FROM --- customers with active loans (changes in the outstanding)
                    (
                        SELECT Loan_ID,
                            Customer_ID,
                            CAST((SELECT date FROM vars) as date) as date,
                            (
                                Principal_Outstanding_Amount + Interest_Outstanding_Amount
                            ) AS Total_Outstanding_Amount,
                            Loan_Balance_Date,
                            LEAD(
                                Principal_Outstanding_Amount + Interest_Outstanding_Amount
                            ) OVER (
                                PARTITION BY Loan_ID
                                ORDER BY Loan_Balance_Date DESC
                            ) AS Prev_Total_Outstanding_Amount
                        FROM `amret-kh.dwh.loan_balance`
                    )
                WHERE Loan_Balance_Date>DATE_ADD(CAST((SELECT date FROM vars) as date),INTERVAL -6 MONTH) AND Loan_Balance_Date<=CAST((SELECT date FROM vars) as date)
				AND Total_Outstanding_Amount <> Prev_Total_Outstanding_Amount
                GROUP BY 1, 2
      
				
                UNION ALL
                --- customers with transactions
                SELECT Customer_ID, CAST((SELECT date FROM vars) as date) as date
                FROM `amret-kh.dwh.transaction`
				WHERE CAST(Transaction_DateTime AS DATE)>DATE_ADD(CAST((SELECT date FROM vars) as date),INTERVAL -6 MONTH) AND CAST(Transaction_DateTime AS DATE)<=CAST((SELECT date FROM vars) as date)
                GROUP BY 1, 2
				
				        UNION ALL
                --- customers with active accounts (changes in the balances)
                SELECT Customer_ID, CAST((SELECT date FROM vars) as date) as date
                    
                FROM (
                        SELECT Customer_ID,
						    CAST((SELECT date FROM vars) as date) as date,
                            Account_ID,
                            Capital_Balance_Amount,
                            Account_Balance_Date,
                            LEAD(Capital_Balance_Amount) OVER (
                                PARTITION BY Account_ID
                                ORDER BY Account_Balance_Date DESC
                            ) AS Prev_Capital_Balance_Amount
                        FROM `amret-kh.dwh.account_balance`
                    )
                WHERE Account_Balance_Date>DATE_ADD(CAST((SELECT date FROM vars) as date),INTERVAL -6 MONTH) AND Account_Balance_Date<=CAST((SELECT date FROM vars) as date)
				AND Capital_Balance_Amount <> Prev_Capital_Balance_Amount
                GROUP BY 1,2
				)
		GROUP BY 1
				