__version__ = '0.5.7'

import sys
import boto3
import json
import base64
import hashlib
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrameWriter


def get_postgres_credentials():
    '''
    Return credentials for PostgreSQL
    '''
    with open("../postgres_credentials.json", "r") as read_file:
        auth = json.load(read_file)

    return auth


def encode(file_path, bucket_name, region_name='us-west-2', mode=1):
    '''
    Return encoded string 
    0: base64 (first 50)
    1: md5
    '''
    s3 = boto3.client('s3', region_name)
    
    file_obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = file_obj['Body'].read()
    
    if mode == 0:
        return base64.b64encode(data).decode()[:50]
    else:
        return hashlib.md5(data).hexdigest()


if __name__ == "__main__":
    bucket_name = sys.argv[1]
    
    spark = SparkSession\
        .builder\
        .appName('DuplicateDetector')\
        .getOrCreate()
    
    sqlContext = SQLContext(spark.sparkContext)
    
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    
    # Get a list of file paths
    file_paths = [file_obj.key for file_obj in my_bucket.objects.filter(Prefix='test_1/')]
    rows = spark.sparkContext.parallelize(file_paths).map(lambda x: Row(path=x, content=encode(x, bucket_name)))
    output = rows.collect()
    df = sqlContext.createDataFrame(output)
    
    #Create the Database properties
    auth = get_postgres_credentials()
    db_properties={}
    db_url = "jdbc:postgresql://{}:{}/{}".format(auth['host'], auth['port'], auth['database'])
    db_properties['username'] = auth['user']
    db_properties['password'] = auth['password']
    db_properties['driver'] = "org.postgresql.Driver"

    #Save the DataFrame to the table. 
    df.write.jdbc(url=db_url,table='images', mode='overwrite', properties=db_properties)

    spark.stop()
