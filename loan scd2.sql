CREATE OR REPLACE PROCEDURE `usecases-471314.bank_raw.sp_scd2_loans_update`()
BEGIN
  -- Create the loans table if it does not exist
  CREATE TABLE IF NOT EXISTS `usecases-471314.bank_raw.loans_raw` (
    StartDate DATE,
    LoanType STRING,
    PaymentFrequency STRING,
    InterestRate FLOAT64,
    CustomerID INT64,
    Status STRING,
    LoanID INT64,
    Amount FLOAT64,
    EndDate DATE,
    Collateral STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP);

  -- Perform SCD2 history operation

  MERGE `usecases-471314.bank_raw.loans_raw` T
  USING `usecases-471314.bank_raw.loans_staging` S
  ON T.LoanID = S.LoanID
  WHEN MATCHED AND (
  T.LoanType != S.LoanType OR
  T.PaymentFrequency != S.PaymentFrequency OR
  T.InterestRate != S.InterestRate OR
  T.Amount != S.Amount OR
  T.EndDate != S.EndDate OR
  T.Collateral != S.Collateral OR
  T.Status != S.Status
  )
  AND T.is_current = TRUE
  THEN
  -- Mark the old record as expired
  UPDATE SET 
    T.is_current = FALSE,
    T.valid_to = CURRENT_TIMESTAMP();

  -- Insert the new record with valid_from and valid_to dates
  INSERT into unified-financial-data-lake.inceptez_bank_raw.loans_raw (StartDate, LoanType, PaymentFrequency, InterestRate, CustomerID, Status, LoanID, Amount, EndDate,Collateral, is_current, valid_from, valid_to)
  select S.StartDate, S.LoanType, S.PaymentFrequency, S.InterestRate, S.CustomerID, S.Status, S.LoanID, S.Amount,
   S.EndDate, S.Collateral,
   TRUE, -- Set new record as current
   CURRENT_TIMESTAMP(), -- valid_from
   NULL -- valid_to is null because it's the current record
   from `usecases-471314.bank_raw.loans_staging` s;

END;


CREATE OR REPLACE EXTERNAL TABLE `usecases-471314.bank_raw.loans_staging` 
OPTIONS (FORMAT='JSON',
 uris=['gs://bank_project_1/loans_day1.json']);

call `usecases-471314.bank_raw.sp_scd2_loans_update`();
Select count(1) from usecases-471314.bank_raw.loans_raw;

--Source writes the subsequent customer info into GCS customer_day2.json from day2 onwards, load incremental data
CREATE OR REPLACE EXTERNAL TABLE `usecases-471314.bank_raw.loans_staging` 
OPTIONS (FORMAT='JSON',
 uris=['gs://bank_project_1/loans_day2.json']);

call `usecases-471314.bank_raw.sp_scd2_loans_update`();
Select count(1) from usecases-471314.bank_raw.loans_raw;

