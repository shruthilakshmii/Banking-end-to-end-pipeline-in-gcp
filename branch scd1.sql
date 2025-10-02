
CREATE OR REPLACE PROCEDURE `usecases-471314.bank_raw.sp_scd1_branch_update`()
BEGIN
CREATE TABLE IF NOT EXISTS `usecases-471314.bank_raw.branch_raw`(
  branchID int64,FirstName string,
  LastName string,DateOfBirth date,
  Email string,PhoneNumber string,
  Address string,BranchID int64,
  last_update timestamp);

MERGE `usecases-471314.bank_raw.branch_raw` T
USING `usecases-471314.bank_raw.branch_staging` S
ON T.branchID = S.branchID
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
INSERT (branchID, FirstName, LastName, DateOfBirth, Email, PhoneNumber, Address, BranchID, last_update)
VALUES (S.branchID, S.FirstName, S.LastName, CAST(S.DateOfBirth AS DATE), S.Email, S.PhoneNumber, S.Address, S.BranchID, CURRENT_TIMESTAMP());
END;

CREATE OR REPLACE EXTERNAL TABLE `usecases-471314.bank_raw.branch_staging` 
OPTIONS (FORMAT='csv',
 uris=['gs://bank_project_1/branch_raw.csv']);
call `usecases-471314.bank_raw.sp_scd1_branch_update`();
Select count(1) from usecases-471314.bank_raw.branch_raw;

