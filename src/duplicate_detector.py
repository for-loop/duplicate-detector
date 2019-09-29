from __future__ import print_function

__version__ = '0.6.3'

import sys
import boto3
import base64
import hashlib
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
import pgconf as pc

def encode(file_path, bucket_name, region_name='us-west-2', method='checksum'):
    '''
    Return encoded string 
    checksum: md5
    base: base64
    '''
    s3 = boto3.client('s3', region_name)
    
    file_obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    data = file_obj['Body'].read()
    
    if method == 'checksum':
        return hashlib.md5(data).hexdigest()
    elif method == 'base':
        return base64.b64encode(data).decode()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: duplicate_detector.py <s3bucket>", file=sys.stderr)
        sys.exit(-1)
    
    spark = SparkSession\
        .builder\
        .appName('DuplicateDetector')\
        .getOrCreate()
    
    # Get a list of file paths in S3 bucket
    bucket_name = sys.argv[1]
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    file_paths = [file_obj.key for file_obj in my_bucket.objects.filter(Prefix='test_1/')]
    
    # Make DataFrame
    df = spark.sparkContext.parallelize(file_paths)\
        .map(lambda x: Row(path=x, content=encode(x, bucket_name)))\
        .toDF()
    
    # Save the DataFrame to PostgreSQL table. 
    pg_conf = pc.PostgresConfigurator()
    df.write.jdbc(url = pg_conf.get_url(),
                  table = 'images', 
                  mode = 'overwrite', 
                  properties = pg_conf.get_properties())

    spark.stop()
