from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, FloatType, MapType
import requests

def bq_full_load(spark,df,mod,bqdbname,bqtblname):

    spark.conf.set("viewEnabled","true")
    spark.conf.set("materializationDataset","core_banking")
    df.write.\
        mode(f'{mod}').\
        format('com.google.cloud.spark.bigquery').\
        option("temporaryGcsBucket",'usecases-471314-common-bucket-1/tmp').\
        option('table', f'{bqdbname}.{bqtblname}').\
        save()



spark = SparkSession.builder \
    .appName("JsonSchemaExample") \
    .getOrCreate()

# List of URLs to fetch data from
urls = [
    "https://restcountries.com/v3.1/currency/usd",
    "https://restcountries.com/v3.1/currency/eur",
    "https://restcountries.com/v3.1/currency/sgd",
    "https://restcountries.com/v3.1/currency/inr",
    "https://restcountries.com/v3.1/currency/cad"
]

# Define the schema
schema = StructType([
    StructField("name", StructType([
        StructField("common", StringType(), True),
        StructField("official", StringType(), True),
        StructField("nativeName", MapType(StringType(), StructType([
            StructField("official", StringType(), True),
            StructField("common", StringType(), True)
        ])), True)
    ]), True),
    StructField("tld", ArrayType(StringType()), True),
    StructField("cca2", StringType(), True),
    StructField("ccn3", StringType(), True),
    StructField("cca3", StringType(), True),
    StructField("cioc", StringType(), True),
    StructField("independent", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("unMember", BooleanType(), True),
    StructField("currencies", MapType(StringType(), StructType([
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True)
    ])), True),
    StructField("idd", StructType([
        StructField("root", StringType(), True),
        StructField("suffixes", ArrayType(StringType()), True)
    ]), True),
    StructField("capital", ArrayType(StringType()), True),
    StructField("altSpellings", ArrayType(StringType()), True),
    StructField("region", StringType(), True),
    StructField("subregion", StringType(), True),
    StructField("languages", MapType(StringType(), StringType()), True),
    StructField("translations", MapType(StringType(), StructType([
        StructField("official", StringType(), True),
        StructField("common", StringType(), True)
    ])), True),
    StructField("latlng", ArrayType(FloatType()), True),
    StructField("landlocked", BooleanType(), True),
    StructField("borders", ArrayType(StringType()), True),
    StructField("area", FloatType(), True),
    StructField("demonyms", MapType(StringType(), StructType([
        StructField("f", StringType(), True),
        StructField("m", StringType(), True)
    ])), True),
    StructField("flag", StringType(), True),
    StructField("maps", StructType([
        StructField("googleMaps", StringType(), True),
        StructField("openStreetMaps", StringType(), True)
    ]), True),
    StructField("population", IntegerType(), True),
    StructField("gini", MapType(StringType(), FloatType()), True),
    StructField("fifa", StringType(), True),
    StructField("car", StructType([
        StructField("signs", ArrayType(StringType()), True),
        StructField("side", StringType(), True)
    ]), True),
    StructField("timezones", ArrayType(StringType()), True),
    StructField("continents", ArrayType(StringType()), True),
    StructField("flags", StructType([
        StructField("png", StringType(), True),
        StructField("svg", StringType(), True),
        StructField("alt", StringType(), True)
    ]), True),
    StructField("coatOfArms", StructType([
        StructField("png", StringType(), True),
        StructField("svg", StringType(), True)
    ]), True),
    StructField("startOfWeek", StringType(), True),
    StructField("capitalInfo", StructType([
        StructField("latlng", ArrayType(FloatType()), True)
    ]), True)
])

# Fetch data from each URL and create a DataFrame
all_data = []
for url in urls:
    response = requests.get(url)
    json_data = response.json()
    all_data.extend(json_data)

# Create a DataFrame from the aggregated JSON data with the defined schema
df = spark.createDataFrame(all_data, schema=schema)

spark.conf.set("spark.sql.debug.maxToStringFields", '1000')

df.printSchema()
df.show(1, False)

bq_full_load(spark,df,"overwrite","bank_raw","api_raw")


from pyspark.sql.functions import *
print("Risk Categorization and Aggregation based on the regions")
df_with_risk = df.withColumn(
    "risk_level",
    when((col("region") == "Africa") | (col("population") < 1000000), "High")
    .when((col("region") == "Europe") & (col("population") > 50000000), "Low")
    .otherwise("Medium")
)

df_with_risk=df_with_risk.withColumnRenamed("common","country")
df_with_risk.select("name.common", "region", "population", "risk_level").show(10, False)

bq_full_load(spark,df_with_risk,"overwrite","bank_analytics","risk_category")


print("Multi-currency Details Segregation")

from pyspark.sql.functions import explode

currency_df = df.select(
    col("name.common").alias("country"),
    explode(col("currencies")).alias("currency_code", "currency_details"),
    col("region"),
    col("population")
).select(
    "country",
    "currency_code",
    col("currency_details.name").alias("currency_name"),
    col("currency_details.symbol").alias("currency_symbol"),
    "region"
)

bq_full_load(spark,currency_df,"overwrite","bank_curated","currency_details")


print("Population analysis across countries and regions")
currency_df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

window_spec = Window.partitionBy("region").orderBy(col("population").desc())

largest_country_df = df.withColumn("rank", rank().over(window_spec)) \
                       .filter(col("rank") == 1) \
                       .select("region", col("name.common").alias("largest_country"), "population")

#largest_country_df.show()
largest_country_df=largest_country_df.withColumnRenamed("common","country")
bq_full_load(spark,largest_country_df,"overwrite","bank_analytics","largest_country_details")