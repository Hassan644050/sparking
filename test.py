import os,sys
import pyspark
from pyspark.sql import SparkSession
import yaml
from pathlib import Path

my_path = Path(__file__).resolve()
config_path = my_path.parent/'config/etl_config.yaml'
with config_path.open() as config_file:
    config = yaml.safe_load(config_file)

# spark = SparkSession.builder \
#                     .master('local[1]') \
#                     .appName('SparkByExamples.com') \
#                     .getOrCreate()

spark = SparkSession \
        .builder \
        .appName("LocalQuery") \
        .config("spark.executor.heartbeatInterval", "100000ms") \
        .config("spark.driver.extraClassPath","H:/BigData/spark/jars") \
        .config("spark.driver.memory", "5G") \
        .config("spark.executor.memory", "10G") \
        .getOrCreate()
print(spark)

print(config['spark']['jar_directory'])
print(config['database']['sqlserver']['url'])
print(config['database']['sqlserver']['name'])

def get_table_data(table_name):
    jbdcurl = config['database']['sqlserver']['url'] + config['database']['sqlserver']['name']
    properties = {"user": config['database']['sqlserver']['username'],"password": config['database']['sqlserver']['password'],"driver": config['database']['password']['driver']}
    data = spark.read.jdbc(url=jbdcurl,table=table_name,properties=properties)
    return data

def get_table_data_qa():
    jbdcurl = "jdbc:sqlserver://localhost:1433;databaseName=EShopOnHand_V4"
    properties = {"user": "sa","password": "12345","driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    data = spark.read.jdbc(url=jbdcurl,table="VendorProduct",properties=properties)
    return data

data=get_table_data_qa()
data.show()
