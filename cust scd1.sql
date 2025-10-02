
CREATE OR REPLACE PROCEDURE `usecases-471314.bank_raw.sp_scd1_customer_update`()
BEGIN
CREATE TABLE IF NOT EXISTS `usecases-471314.bank_raw.customer_raw`(
  CustomerID int64,FirstName string,
  LastName string,DateOfBirth date,
  Email string,PhoneNumber string,
  Address string,BranchID int64,
  last_update timestamp);

MERGE `usecases-471314.bank_raw.customer_raw` T
USING `usecases-471314.bank_raw.customer_staging` S
ON T.CustomerID = S.CustomerID
WHEN MATCHED THEN
UPDATE SET
  T.FirstName = S.FirstName,
  T.LastName = S.LastName,
  T.DateOfBirth = CAST(S.DateOfBirth AS DATE),
  T.Email = S.Email,
  T.PhoneNumber = S.PhoneNumber,
  T.Address = S.Address,
  T.BranchID = S.BranchID,
  T.last_update = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
INSERT (CustomerID, FirstName, LastName, DateOfBirth, Email, PhoneNumber, Address, BranchID, last_update)
VALUES (S.CustomerID, S.FirstName, S.LastName, CAST(S.DateOfBirth AS DATE), S.Email, S.PhoneNumber, S.Address, S.BranchID, CURRENT_TIMESTAMP());
END;

CREATE OR REPLACE EXTERNAL TABLE `usecases-471314.bank_raw.customer_staging` 
OPTIONS (FORMAT='csv',
 uris=['gs://bank_project_1/cust.csv']);
call `usecases-471314.bank_raw.sp_scd1_customer_update`();
Select count(1) from usecases-471314.bank_raw.customer_raw;
Select * from usecases-471314.bank_raw.customer_raw where customerid in (404570, 404571, 404572, 404573, 404574);

--Source writes the subsequent customer info into GCS customer_day2.json from day2 onwards, load incremental data
CREATE OR REPLACE EXTERNAL TABLE `usecases-471314.bank_raw.customer_staging` 
OPTIONS (FORMAT='csv',
 uris=['gs://bank_project_1/cust1.csv']);
call `usecases-471314.bank_raw.sp_scd1_customer_update`();
Select count(1) from usecases-471314.bank_raw.customer_raw;
Select * from usecases-471314.bank_raw.customer_raw where customerid in (404570, 404571, 404572, 404573, 404574);
