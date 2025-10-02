from pyspark.sql import SparkSession
from pyspark import SparkConf
from configparser import ConfigParser

def get_sparksess(sparksess):
    # Initialize SparkConf
    conf = SparkConf()
    # Read spark configurations from the properties file
    config = ConfigParser()
    # print(config.read(sparksess))
    sparksess="/home/safainctech/Unified_Financial_Data_Lake_for_Advanced_Analytics/sparksess.properties"
    config.read(sparksess)
    for configname, configvalue in config.items("CONFIGS"):
        conf.set(configname,configvalue)
        # Create SparkSession with the configurations
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        # Set GCS-related Hadoop configurations for Spark
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        return spark


def get_min_max_db(spark,dbpropfile,db,tbl,partcolumn):
    config = ConfigParser()
    dbpropfile="/home/safainctech/Unified_Financial_Data_Lake_for_Advanced_Analytics/connection.properties"
    config.read(dbpropfile)

    user = config.get("DBCRED", "user")
    passw = config.get("DBCRED", "pass")
    driver = config.get("DBCRED", "driver")
    host = config.get("DBCRED", "host")
    port = config.get("DBCRED", "port")
    url = f"{host}:{port}/{db}"

    qry = f"(select MIN({partcolumn}) as minval, MAX({partcolumn}) as maxval from {tbl}) as tble"

    bound_df = spark.read.format("jdbc")\
        .option("url",url)\
        .option("user",user)\
        .option("password",passw)\
        .option("dbtable",qry)\
        .option("driver",driver)\
        .load()

    bounds = bound_df.collect()

    if bounds[0][0] == 0 or bounds[0][1] == 0:
        return None
    else:
        return bounds





def getPartitionRDBMSdata(spark,dbpropfile,db,qry,lowerbound,upperbound,numpartition,parcolumn):

    dbpropfile="/home/safainctech/Unified_Financial_Data_Lake_for_Advanced_Analytics/connection.properties"
    config = ConfigParser()
    config.read(dbpropfile)

    user = config.get("DBCRED","user")
    passw = config.get("DBCRED","pass")
    driver = config.get("DBCRED","driver")
    host = config.get("DBCRED","host")
    port = config.get("DBCRED","port")
    url = f"{host}:{port}/{db}"


    df = spark.read.format("jdbc")\
        .option("url",url)\
        .option("user",user)\
        .option("password",passw)\
        .option("dbtable",qry)\
        .option("driver",driver)\
        .option("partitionColumn",parcolumn)\
        .option("lowerBound",lowerbound)\
        .option("upperBound",upperbound)\
        .option("numPartitions",numpartition)\
        .load()

    return df

