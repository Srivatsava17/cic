
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import json


spark = SparkSession.builder\
            .appName('Python Spark')\
            .config("spark.sql.execution.arrow.enabled", "true")\
            .getOrCreate()

import requests

condition = None

vault_url='http://34.221.55.148:8200'
headers = {
    'X-Vault-Token': 'hvs.NwJdeGnPnmyNQ3IhCrYSsPPN'
}

url = vault_url+"/v1/secret/data/postgre_path"
response = requests.get(url, headers=headers)

extract=spark.read\
.format("jdbc")\
.option("url", "jdbc:postgresql://agilisium-innovation-lab.cabfbspytumf.us-west-2.rds.amazonaws.com:5432/Automated_Data_Lake")\
.option("user", response.json()['data']['data']['username'])\
.option("password", response.json()['data']['data']['password'])\
.option("dbtable", "storage")\
.option("driver","org.postgresql.Driver")\
.load()

if condition:
    extract = extract.filter(condition)


    
        
dropcolumn = extract.drop("storage_id")

        
dropcolumn.write.format('csv').mode('overwrite').save('s3://pharcomm360/etl_output/', header = True)
display(dropcolumn)
    
