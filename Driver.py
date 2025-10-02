from CommonFunction import getPartitionRDBMSdata,get_min_max_db,get_sparksess
from pyspark.sql.functions import max,current_timestamp,current_date
from sys import argv
import os


def get_last_processed_date(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file,"r") as file:
            last_processed_date = file.read().strip()
            return last_processed_date

def save_last_processed_date(checkpoint_file,date,table_name):
    if date is not None:
        with open(checkpoint_file,"w") as file:
            file.write(str(date))
            print(f"******Last Processed Date Saved in {table_name} CheckPoint File for Consequent Runs ************************")
            print(f"************************Last Processed Date Updated: {date}************************")
            print("                                                                                                                ")

    else:
        print("************************ No new records found. Last processed date remains unchanged. ************************")


def perform_incremental_load(spark,dbpropfile,db_name,table_name,checkpoint_file,date_column,numpartition,partcolumn,bqdbname,bqtblname):

    # Check if it's an incremental or full load
    print("************************           Check if it's an incremental or full load            ************************")
    last_processed_date = get_last_processed_date(checkpoint_file)
    if last_processed_date:
        print(f"************************                 Incremental Load for '{table_name}'                ************************")
        print(f"************************ Only greater than this date: {last_processed_date} will be Ingested for '{table_name}' tbl ************************")
        query = f"(select * from {table_name} where {date_column} > '{last_processed_date}') as tbl"
    else:
        print(f"************************                   Full Load for '{table_name}' Table                ************************")
        query = f"(select * from {table_name}) as tbl"

    print("                                                                                                                  ")
    # Get min and max value from the table for partitioning
    print(f"************************ Getting Min and Max value from '{table_name}' table for Partitioning  ************************")
    bounds = get_min_max_db(spark,dbpropfile,db_name,table_name,partcolumn)

    lowerbound = bounds[0][0]
    upperbound = bounds[0][1]


    # Fetch data from the table using partitioning
    print(f"************************  Reading '{table_name}' table from Cloud SQL data for Partitioning  ************************")
    rdbms_df = getPartitionRDBMSdata(spark,dbpropfile,db_name,query,lowerbound,upperbound,numpartition,partcolumn)
    date_df = rdbms_df.withColumn("load_dt",current_date())
    timestamp_df = date_df.withColumn("load_ts",current_timestamp())
    # timestamp_df.show()

    print("                                                                                                                  ")
    print(f"************************  Writing '{table_name}' table in BigQuery RawLayer for Analytics  ************************")
    spark.conf.set("viewEnabled","true")
    spark.conf.set("materializationDataset","bank_raw")
    timestamp_df.write.\
        mode("append").\
        format('com.google.cloud.spark.bigquery').\
        option("temporaryGcsBucket",'inceptez-common-bucket-1/tmp').\
        option('table', f'{bqdbname}.{bqtblname}').\
        save()



    count = timestamp_df.count()
    print(f"************************            Count of records in the '{table_name}' dataframe: {count}         ************************")

    # Show count of records
    if count > 0:
        max_date = timestamp_df.agg(max(col=date_column)).collect()[0][0]
        save_last_processed_date(checkpoint_file,max_date,table_name)
    else:
        print(f"************************ No new records fetched. The '{table_name}' checkpoint file will not be updated. ************************")
        print("                                                                                                                  ")


def main(arg1_sparksess,arg2_dbpropfile,arg3_checkpointfile_accounts,arg4_checkpointfile_transactions,arg5_checkpointfile_payments):

    spark = get_sparksess(arg1_sparksess)
    spark.sparkContext.setLogLevel("ERROR")


    
    # Perform incremental/full load for the accounts table
    perform_incremental_load(spark=spark,
                             dbpropfile=arg2_dbpropfile,
                             db_name="postgres",
                             table_name="accounts",
                             checkpoint_file=arg3_checkpointfile_accounts,
                             date_column="DateOpened",
                             numpartition=4,
                             partcolumn="AccountID",
                             bqdbname="bank_raw",
                             bqtblname="accounts_raw")

    print("************************ TABLE 2: Perform incremental/full load for the 'transactions' table ************************")
    # Perform incremental/full load for the transactions table
    perform_incremental_load(spark=spark,
                             dbpropfile=arg2_dbpropfile,
                             db_name="postgres",
                             table_name="transactions",
                             checkpoint_file=arg4_checkpointfile_transactions,
                             date_column="TransactionDate",
                             numpartition=6,
                             partcolumn="TransactionID",
                             bqdbname="bank_raw",
                             bqtblname="transactions_raw")

    print("************************ TABLE 3: Perform incremental/full load for the 'payments' table  ************************")
    # Perform incremental/full load for the payments table
    perform_incremental_load(spark=spark,
                             dbpropfile=arg2_dbpropfile,
                             db_name="postgres",
                             table_name="payments",
                             checkpoint_file=arg5_checkpointfile_payments,
                             date_column="PaymentDate",
                             numpartition=6,
                             partcolumn="PaymentID",
                             bqdbname="bank_raw",
                             bqtblname="payments_raw")







if __name__ == '__main__':

    if len(argv) == 6:
        main(argv[1],argv[2],argv[3],argv[4],argv[5])
    else:
        print("Invalid Entry")
