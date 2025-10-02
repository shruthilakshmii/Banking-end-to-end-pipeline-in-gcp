CREATE OR REPLACE PROCEDURE `usecases-471314.bank_raw.sp_acct_trans_analytical_module1`() 
BEGIN
create or replace table `usecases-471314.bank_analytics.bank_ledger_flattened` as
WITH 
  --Accounts with Overdraft Limit Usage
  OverdraftUsageAccounts AS (
    SELECT 
      AccountID,
      AccountType,
      Balance,
      ODLimit,
      (ODLimit - Balance) / ODLimit * 100 AS ODUtilizationPercent
    FROM `usecases-471314.bank_raw.accounts_raw`
    WHERE ODLimit > Balance
  ),
  
  --High-Value Payments in Foreign Currencies
  HighValueForeignPayments AS (
    SELECT 
      PaymentID,
      FromAccountID,
      ToAccountID,
      Amount * ExchangeRate AS AmountUSD,
      PaymentDate,
      MerchantName,
      Description
    FROM `usecases-471314.bank_raw.payments_raw`
    WHERE Amount * ExchangeRate > 10000 AND Currency <> 'USD'
  ),

  --Accounts Opened in Last Year with Low Balances
  RecentLowBalanceAccounts AS (
    SELECT 
      AccountID,
      AccountType,
      Balance,
      Currency,
      DateOpened
    FROM `usecases-471314.bank_raw.accounts_raw`
    WHERE DateOpened >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR) AND Balance < 1000
  ),

  --Transactions Flagged as Failed
  FailedTransactions AS (
    SELECT 
      TransactionID,
      AccountID,
      Amount,
      Currency,
      Status,
      EventTs,
      Description
    FROM `usecases-471314.bank_raw.transactions_raw`
    WHERE Status = 'Failed'
  ),
  
  --Payments with Excessive Fees
  ExcessiveFeePayments AS (
    SELECT 
      PaymentID,
      Amount,
      Currency,
      Fee,
      (Fee / Amount) * 100 AS ExcessiveFeePercent,
      Description
    FROM `usecases-471314.bank_raw.payments_raw`
    WHERE Fee / Amount > 0.05
  ),
  
  --Merchant Payment Analysis
  MerchantPaymentSummary AS (
    SELECT 
      MerchantName,
      SUM(Amount) AS TotalPayments,
      AVG(Amount) AS AvgPaymentAmount,
      MAX(Amount) AS MaxPaymentAmount,
      MIN(Amount) AS MinPaymentAmount,
      COUNT(*) AS TotalTransactions
    FROM `usecases-471314.bank_raw.payments_raw`
    GROUP BY MerchantName
  ),

  -- Transactions by Customer Segment
  CustomerSegmentTransactions AS (
    SELECT 
      CustomerSegment,
      COUNT(*) AS TotalTransactions,
      SUM(Amount) AS TotalAmount,
      AVG(Amount) AS AvgTransactionAmount
    FROM `usecases-471314.bank_raw.payments_raw`
    GROUP BY CustomerSegment
  ),
  --Monthly Failed Transactions by Account
  MonthlyFailedTransactions AS (
    SELECT 
      AccountID,
      FORMAT_DATE('%Y-%m', parse_date('%d-%m-%Y',TransactionDate)) AS YearMonth,
      COUNT(*) AS FailedTransactionCount
    FROM `usecases-471314.bank_raw.transactions_raw`
    WHERE Status = 'Failed'
    GROUP BY AccountID, YearMonth
  )

SELECT 
    distinct a.AccountID,
    a.AccountType,
    a.Balance,
    a.Currency AS AccountCurrency,
    a.CreditScore,
    a.DateOpened,
    a.ManagerID,
    a.ODLimit,
    
    -- Overdraft usage logic
    ou.ODUtilizationPercent,
    
    -- High-value payments in foreign currencies
    hfp.AmountUSD AS HighValuePaymentAmount,
    hfp.PaymentDate AS HighValuePaymentDate,
    hfp.MerchantName AS HighValueMerchant,
    hfp.Description AS HighValuePaymentDescription,

    -- Low balance accounts
    rba.Balance AS RecentLowBalance,

    -- Failed transactions
    ft.TransactionID AS FailedTransactionID,
    ft.Amount AS FailedTransactionAmount,
    ft.Currency AS FailedTransactionCurrency,
    ft.Status AS FailedTransactionStatus,
    ft.EventTs AS FailedTransactionEventTs,
    ft.Description AS FailedTransactionDescription,

    -- Payments with excessive fees
    efp.ExcessiveFeePercent,
    
    -- Merchant payment summary
    mps.TotalPayments AS MerchantTotalPayments,
    mps.AvgPaymentAmount AS MerchantAvgPaymentAmount,
    mps.MaxPaymentAmount AS MerchantMaxPaymentAmount,
    mps.MinPaymentAmount AS MerchantMinPaymentAmount,
    mps.TotalTransactions AS MerchantTotalTransactions,

    -- Customer segment transactions
    cst.TotalTransactions AS SegmentTotalTransactions,
    cst.TotalAmount AS SegmentTotalAmount,
    cst.AvgTransactionAmount AS SegmentAvgTransactionAmount,

    -- Monthly failed transactions
    mft.FailedTransactionCount AS MonthlyFailedTransactionCount,
    
    p.PaymentID,
    p.FromAccountID,
    p.ToAccountID,
    p.Amount AS PaymentAmount,
    p.Currency AS PaymentCurrency,
    p.PaymentDate,
    p.MerchantName,
    p.Description AS PaymentDescription,
    p.PaymentType,
    p.Fee AS PaymentFee,

    t.TransactionID,
    t.TransactionType,
    t.Amount AS TransactionAmount,
    t.Currency AS TransactionCurrency,
    t.Status AS TransactionStatus,
    t.EventTs AS TransactionEventTs,
    t.Description AS TransactionDescription,
    t.TransactionFee,
    t.TransactionDate,
    t.Suspecious AS IsSuspicious
FROM 
    `usecases-471314.bank_raw.accounts_raw` a
LEFT JOIN 
    `usecases-471314.bank_raw.payments_raw` p ON cast(a.AccountID as string) = p.FromAccountID OR cast(a.AccountID as string) = p.ToAccountID
LEFT JOIN 
    `usecases-471314.bank_raw.transactions_raw` t ON cast(a.AccountID as string) = t.AccountID
LEFT JOIN 
    OverdraftUsageAccounts ou ON cast(a.AccountID as string) = cast(ou.AccountID  as string)
LEFT JOIN 
    HighValueForeignPayments hfp ON cast(a.AccountID as string)= hfp.FromAccountID OR cast(a.AccountID as string) = hfp.ToAccountID
LEFT JOIN 
    RecentLowBalanceAccounts rba ON cast(a.AccountID  as string)= cast(rba.AccountID as string)
LEFT JOIN 
    FailedTransactions ft ON cast(a.AccountID  as string)= ft.AccountID
LEFT JOIN 
    ExcessiveFeePayments efp ON p.PaymentID = efp.PaymentID
LEFT JOIN 
    MerchantPaymentSummary mps ON p.MerchantName = mps.MerchantName
LEFT JOIN 
    CustomerSegmentTransactions cst ON p.CustomerSegment = cst.CustomerSegment
LEFT JOIN 
    MonthlyFailedTransactions mft ON cast(a.AccountID as string) = mft.AccountID
    --where t.TransactionID is not null
    where ODUtilizationPercent is not null
    ;
End;
