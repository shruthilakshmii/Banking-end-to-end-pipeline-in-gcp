CREATE OR REPLACE PROCEDURE `usecases-471314.bank_raw.sp_acct_trans_analytical_module2`()
BEGIN
  -- Fraud Detection
  CREATE  or replace table `usecases-471314.bank_currated.PotentialFraudAccounts` (
    AccountID STRING,
    TransactionID INT64,
    PaymentID STRING,
    Suspicious BOOLEAN,
    CreditScore INT64,
    TotalFlaggedEvents INT64
  );

  INSERT INTO `usecases-471314.bank_currated.PotentialFraudAccounts`
  SELECT 
    st.AccountID, 
    CAST(st.TransactionID as int64), 
    cast(hv.PaymentID as string), 
    st.Suspicious, 
    lc.CreditScore, 
    COUNT(*) AS TotalFlaggedEvents
  FROM (
    SELECT AccountID, TransactionID, Suspecious AS Suspicious
    FROM `usecases-471314.bank_raw.transactions_raw`
    WHERE Suspecious = TRUE
  ) st
  FULL JOIN (
    SELECT FromAccountID AS AccountID, PaymentID
    FROM `usecases-471314.bank_raw.payments_raw`
    WHERE Amount * ExchangeRate > 100000
  ) hv ON cast(st.AccountID as string) = cast(hv.AccountID as string)
  FULL JOIN (
    SELECT AccountID, CreditScore
    FROM `usecases-471314.bank_raw.accounts_raw`
    WHERE CreditScore < 400
  ) lc ON cast(st.AccountID as string) = cast(lc.AccountID as string)
  GROUP BY st.AccountID, st.TransactionID, hv.PaymentID, st.Suspicious, lc.CreditScore;

  -- Payment Reconciliation
  CREATE  or replace table `usecases-471314.bank_curated.ReconciliationReport` (
    PaymentID STRING,
    ClearingSystem1 STRING,
    ClearingSystem2 STRING,
    AmountDifferencePercent FLOAT64,
    DateDifferenceDays INT64,
    Status STRING
  );

  INSERT INTO `usecases-471314.bank_curated.ReconciliationReport`
  SELECT 
    cast(p1.PaymentID as string), 
    p1.ClearingSystem AS ClearingSystem1, 
    p2.ClearingSystem AS ClearingSystem2, 
    ABS(p1.Amount - p2.Amount) / p1.Amount * 100 AS AmountDifferencePercent, 
    ABS(DATE_DIFF(p1.PaymentDate, p2.PaymentDate, DAY)) AS DateDifferenceDays, 
    CASE 
      WHEN ABS(p1.Amount - p2.Amount) / p1.Amount * 100 > 1 
           OR ABS(DATE_DIFF(p1.PaymentDate, p2.PaymentDate, DAY)) > 2 THEN 'Mismatched'
      ELSE 'Matched'
    END AS Status
  FROM `usecases-471314.bank_raw.payments_raw` p1
  JOIN `usecases-471314.bank_raw.payments_raw` p2 ON p1.PaymentID = p2.PaymentID
  WHERE p1.ClearingSystem <> p2.ClearingSystem;

  -- Account Activity Heatmap
  CREATE or replace table `usecases-471314.bank_curated.ActivityHeatmap` (
    AccountID STRING,
    DayOfWeek INT64,
    HourOfDay INT64,
    TransactionCount INT64,
    AvgTransactionAmount FLOAT64  );
--select parse_date('%d-%m-%Y %H:%m','21-01-1999 14:33')
  INSERT INTO `usecases-471314.bank_curated.ActivityHeatmap`
  SELECT 
    AccountID, 
    EXTRACT(DAYOFWEEK FROM parse_date('%d-%m-%Y',substr(EventTs,1,10))) AS DayOfWeek, 
    cast(substr(EventTs,12,2) as int64) AS HourOfDay, 
    COUNT(*) AS TransactionCount, 
    AVG(Amount) AS AvgTransactionAmount
    --,CASE WHEN COUNT(*) > AVG(COUNT(*)) THEN 'High Activity' 
    --  ELSE 'Normal Activity' 
    --END AS Anomaly
  FROM `usecases-471314.bank_raw.transactions_raw`
  GROUP BY AccountID, DayOfWeek, HourOfDay;

  -- Multi-Currency Portfolio Risk Analysis

  create or replace table `usecases-471314.bank_curated.ExchangeRates`(Currency string,ExchangeRate float64);
  insert into `usecases-471314.bank_curated.ExchangeRates`
  values('EUR',98.1),('GBP',66.3);

  CREATE  or replace table `usecases-471314.bank_curated.PortfolioRisk` (
    CustomerID STRING,
    PortfolioValueUSD FLOAT64,
    VolatileCurrencySharePercent FLOAT64,
    RiskLevel STRING
  );

  INSERT INTO `usecases-471314.bank_curated.PortfolioRisk`
  WITH Portfolio AS (
    SELECT 
      cast(CustomerID as string) CustomerID, 
      SUM(cast(Balance as int64) * cast(ExchangeRate.ExchangeRate as int64)) AS PortfolioValueUSD, 
      SUM(CASE WHEN accounts.Currency IN ('GBP', 'EUR') THEN cast(Balance as float64) * cast(ExchangeRate.ExchangeRate as float64) ELSE 0 END) / SUM(cast(Balance as float64) * cast(ExchangeRate.ExchangeRate as float64)) * 100 AS VolatileCurrencySharePercent
    FROM `usecases-471314.bank_raw.accounts_raw` Accounts
    JOIN `usecases-471314.bank_curated.ExchangeRates` ExchangeRate
    ON Accounts.Currency = ExchangeRate.Currency
    GROUP BY CustomerID
  )
  SELECT 
    CustomerID, 
    PortfolioValueUSD, 
    VolatileCurrencySharePercent,
    CASE 
      WHEN PortfolioValueUSD > 500000 AND VolatileCurrencySharePercent > 50 THEN 'High Risk'
      WHEN PortfolioValueUSD > 200000 THEN 'Medium Risk'
      ELSE 'Low Risk'
    END AS RiskLevel
  FROM Portfolio;

  -- Fee Revenue Leakage Detection
  CREATE  or replace table `usecases-471314.bank_curated.FeeLeakage` (
    TransactionID INT64,
    AccountID STRING,
    Amount FLOAT64,
    TransactionFee FLOAT64,
    ExpectedFee FLOAT64,
    Status STRING
  );

  INSERT INTO `usecases-471314.bank_curated.FeeLeakage`
  SELECT 
    TransactionID, 
    AccountID, 
    Amount, 
    TransactionFee, 
    (Amount * 0.01) AS ExpectedFee, 
    CASE 
      WHEN TransactionFee < (Amount * 0.001) THEN 'Potential Leak'
      ELSE 'Normal'
    END AS Status
  FROM `usecases-471314.bank_raw.transactions_raw`;

  -- Customer Profitability Analysis
  CREATE  or replace table `usecases-471314.bank_curated.CustomerProfitability` (
    CustomerID INT64,
    AccountBalances FLOAT64,
    TotalTransactionFees FLOAT64,
    ODUtilizationPercent FLOAT64,
    Profitability FLOAT64
  );

  INSERT INTO `usecases-471314.bank_curated.CustomerProfitability`
  WITH Profitability AS (
    SELECT 
      a.CustomerID, 
      SUM(a.Balance) AS AccountBalances, 
      SUM(t.TransactionFee) AS TotalTransactionFees, 
      (AVG((a.ODLimit - a.Balance)/a.ODLimit))*100 AS ODUtilizationPercent
    FROM `usecases-471314.bank_raw.accounts_raw` a
    JOIN `usecases-471314.bank_raw.transactions_raw` t ON substr(cast(a.AccountID as string),1,3) = substr(cast(t.AccountID as string),1,3)
    GROUP BY a.CustomerID
  )
  SELECT 
    CustomerID, 
    AccountBalances, 
    TotalTransactionFees, 
     ODUtilizationPercent,
    TotalTransactionFees + (ODUtilizationPercent * 0.02) - 1000 AS Profitability
  FROM Profitability;

  -- Cross-Sell Opportunity Identification
  CREATE OR REPLACE TABLE `usecases-471314.bank_curated.CrossSellOpportunities` (
    CustomerID INT64,
    AvgAccountBalance FLOAT64,
    HighValueTransactionsCount INT64,
    Eligibility STRING
  );

  INSERT INTO `usecases-471314.bank_curated.CrossSellOpportunities`
  WITH HighValueCustomers AS (
    SELECT 
      CustomerID, 
      AVG(Balance) AS AvgAccountBalance, 
      COUNT(CASE WHEN Amount > 10000 THEN 1 END) AS HighValueTransactionsCount
    FROM `usecases-471314.bank_raw.accounts_raw` a
    JOIN `cusecases-471314.bank_raw.transactions_raw` t ON substr(cast(a.AccountID as string),1,3) = substr(cast(t.AccountID as string),1,3)
    GROUP BY CustomerID
  )
  SELECT 
    CustomerID, 
    AvgAccountBalance, 
    HighValueTransactionsCount,
    CASE 
      WHEN AvgAccountBalance > 50000 AND HighValueTransactionsCount >= 3 THEN 'Eligible'
      ELSE 'Not Eligible'
    END AS Eligibility
  FROM HighValueCustomers;

  --Monthly Clearing System Efficiency
  CREATE  or replace table `usecases-471314.bank_curated.ClearingSystemEfficiency` (
    ClearingSystem STRING,
    AvgProcessingTimeDays FLOAT64,
    MaxProcessingTimeDays INT64,
    MinProcessingTimeDays INT64
  );

  INSERT INTO `usecases-471314.bank_curated.ClearingSystemEfficiency`
  SELECT 
    ClearingSystem, 
    abs(AVG(DATE_DIFF(parse_date('%d-%m-%Y',TransactionDate), PaymentDate, DAY))) AS AvgProcessingTimeDays,
    MAX(DATE_DIFF(parse_date('%d-%m-%Y',TransactionDate), PaymentDate, DAY)) AS MaxProcessingTimeDays,
    MIN(DATE_DIFF(parse_date('%d-%m-%Y',TransactionDate), PaymentDate, DAY)) AS MinProcessingTimeDays
  FROM `usecases-471314.bank_raw.payments_raw` p
  JOIN `usecases-471314.bank_raw.transactions_raw` t ON substr(cast(p.PaymentID as string),1,3) = substr(cast(t.TransactionID as string),1,3)
  GROUP BY ClearingSystem;
END;