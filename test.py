import os,sys
import pyspark
from pyspark.sql import SparkSession
import yaml
from pathlib import Path

my_path = Path(__file__).resolve()
config_path = my_path.parent/'config/etl_config.yaml'
with config_path.open() as config_file:
    config = yaml.safe_load(config_file)

spark = SparkSession \
        .builder \
        .master("local[2]") \
        .appName("LocalQuery") \
        .config("spark.executor.heartbeatInterval", "100000ms") \
        .config("spark.driver.extraClassPath",str(config['spark']['jar_directory'])) \
        .config("spark.driver.memory", "5G") \
        .config("spark.executor.memory", "10G") \
        .getOrCreate()

url=str(config['database']['sqlserver']['url'])
dbName=str(config['database']['sqlserver']['name'])
userName=str(config['database']['sqlserver']['username'])
password1=str(config['database']['sqlserver']['password'])

def get_table_data_qa(url,dbName,username,password):
    jbdcurl = url+dbName
    properties = {"user": username,"password": password,"driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    data = spark.read.jdbc(url=jbdcurl,table="Division",properties=properties)
    return data

data=get_table_data_qa(url,dbName,userName,password1)
data.show()
