from __future__ import print_function

__version__ = '0.8.7'

import ddargv
import io
import boto3
import base64
import hashlib
import time
import numpy as np
import skimage.io as skio
import skimage.transform as transform
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
import pgconf as pc
import sqlalchemy
import pandas as pd


def load(file_path, bucket_name, region_name):
    '''
    Load and return data in binary format
    '''
    s3 = boto3.client('s3', region_name)
    file_obj = s3.get_object(Bucket=bucket_name, Key=file_path)
    
    return file_obj['Body'].read()


def resize(file_path, bucket_name, region_name, reduce_factor = 128):
    '''
    Load and return image data of reduced resolution
    '''
    s3 = boto3.resource('s3', region_name)
    bucket = s3.Bucket(bucket_name)
    file_obj = bucket.Object(file_path)
    file_stream = io.BytesIO()
    file_obj.download_fileobj(file_stream)
    img = skio.imread(file_stream)
    width = img.shape[0]//reduce_factor
    height = img.shape[1]//reduce_factor
    
    # The following outputs a result that is close to skimage.transform.resize()
    # with preserve_range=True and anti_aliasing=False.
    # It is done manually instead because the installed skimage vers 0.10
    # does not support the parameters.
    PRESERVE_RANGE_FACTOR = 255
    
    return (transform.resize(img, (width, height), mode='reflect')*PRESERVE_RANGE_FACTOR).astype(np.uint8)
    
    
def encode(file_path, bucket_name, region_name, method):
    '''
    Return encoded string 
    checksum: md5
    base: base64
    base_small: base64 on reduced image resolution
    '''
    if method == 'checksum':
        data = load(file_path, bucket_name, region_name)
        return hashlib.md5(data).hexdigest()
    elif method == 'base':
        data = load(file_path, bucket_name, region_name)
        return base64.b64encode(data).decode()
    elif method == 'base_small':
        data = resize(file_path, bucket_name, region_name)
        return base64.b64encode(data).decode()


def log_benchmark(start_time, table_name_images, table_name_contents, pg_url):
    '''
    Log benchmark: log elapsed time (s) and size of the tables (bytes)
    * start_time: Start time
    * table_name_images: Name of the images table
    * table_name_contents: Name of the contents table
    * auth: Authentication for PostgreSQL
    '''
    print('Elapsed time: {} s'.format(time.time() - start_time))

    # print database size
    engine = sqlalchemy.create_engine(pg_url)

    query = "SELECT pg_total_relation_size('{}')".format(table_name_images)
    df = pd.read_sql_query(query, con = engine)
    table_size_images = df['pg_total_relation_size'].iloc[0]

    query = "SELECT pg_total_relation_size('{}');".format(table_name_contents)
    df = pd.read_sql_query(query, con = engine)
    table_size_contents = df['pg_total_relation_size'].iloc[0]

    print("Size of tables: {} bytes".format(table_size_images + table_size_contents))


def main():
    args = ddargv.ParseArgs("Duplicate Detector")
    bucket_name, method_name, region_name, dir_name = args.get_all() #parse_args()

    # Start time for benchmark
    t = time.time()
    
    spark = SparkSession\
        .builder\
        .appName('DuplicateDetector')\
        .getOrCreate()
    
    # Get a list of file paths in S3 bucket
    s3 = boto3.resource('s3')
    my_bucket = s3.Bucket(bucket_name)
    file_paths = [file_obj.key for file_obj in my_bucket.objects.filter(Prefix="{}/".format(dir_name))]
    
    # Make DataFrame
    df = spark.sparkContext.parallelize(file_paths)\
        .map(lambda x: Row(path=x, content=encode(x, bucket_name, region_name, method_name)))\
        .toDF()\
        .withColumn("image_id", F.monotonically_increasing_id())
    
    df.cache()
    
    # Split df into contends and images
    sqlContext = SQLContext(spark.sparkContext)
    df.registerTempTable('images')
    df_contents = sqlContext.sql("SELECT content FROM images GROUP BY content")\
                            .withColumn("content_id", F.monotonically_increasing_id())
    
    df_contents.registerTempTable("contents")
    df_images = sqlContext.sql("SELECT image_id, path, content_id FROM images AS i INNER JOIN contents AS c ON i.content = c.content")
    
    # Save the DataFrame to PostgreSQL table 
    pg_conf = pc.PostgresConfigurator()
    table_name_images = 'images_{}_{}'.format(method_name, dir_name)
    df_images.write.jdbc(url = pg_conf.get_url(),
                         table = table_name_images, 
                         mode = 'overwrite', 
                         properties = pg_conf.get_properties())

    table_name_contents = 'contents_{}_{}'.format(method_name, dir_name)
    df_contents.write.jdbc(url = pg_conf.get_url(),
                           table = table_name_contents,
                           mode = 'overwrite',
                           properties = pg_conf.get_properties())
    
    spark.stop()

    log_benchmark(t, table_name_images, table_name_contents, pg_conf.get_url_w_password())
    

if __name__ == "__main__":
    main()
