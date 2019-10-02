from __future__ import print_function

__version__ = '0.8.3'

import argparse
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
import psycopg2
from psycopg2 import Error


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


def parse_args():
    '''
    Parse command line args
    '''
    parser = argparse.ArgumentParser(description = "Duplicate Detector")

    parser.add_argument("bucket", type = str, nargs = 1,
                        metavar = "bucket_name", default = None,
                        help = "Name of the S3 bucket where the files are stored.")
    
    parser.add_argument("-m", "--method", type = str, nargs = 1,
                        metavar = "algorithm", default = ["checksum"],
                        help = "Method of detecting duplicates. The default \
                        is 'checksum'.")
    
    parser.add_argument("-r", "--region", type = str, nargs = 1,
                        metavar = "region_name", default = ["us-west-2"],
                        help = "Name of the region where the S3 bucket is located. \
                        The default is 'us-west-2'.")
    
    parser.add_argument("-d", "--dir", type = str, nargs = 1,
                        metavar = "directory", default = ["validation"],
                        help = "Name of the directory where the files are located. \
                        The default is 'validation'.")
    
    args = parser.parse_args()
    
    if args.bucket != None: bucket_name = args.bucket[0]
    if args.method != None: method_name = args.method[0]
    if args.region != None: region_name = args.region[0]
    if args.dir != None: dir_name = args.dir[0]
    
    return (bucket_name, method_name, region_name, dir_name)


def log_benchmark(start_time, table_name_images, table_name_contents, auth):
    '''
    Log benchmark: log elapsed time (s) and size of the tables (bytes)
    * start_time: Start time
    * table_name_images: Name of the images table
    * table_name_contents: Name of the contents table
    * auth: Authentication for PostgreSQL
    '''
    print('Elapsed time: {} s'.format(time.time() - start_time))

    # print database size
    try:
        connection = psycopg2.connect(**auth)
        cursor = connection.cursor()

        query = "SELECT pg_total_relation_size('{}');".format(table_name_images)
        cursor.execute(query)
        rows = cursor.fetchone()
        table_size_images = rows[0]

        query = "SELECT pg_total_relation_size('{}');".format(table_name_contents)
        cursor.execute(query)
        rows = cursor.fetchone()
        table_size_contents = rows[0]

        print("Size of tables: {} bytes".format(table_size_images + table_size_contents))

    except () as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()



def main():
    bucket_name, method_name, region_name, dir_name = parse_args()

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

    log_benchmark(t, table_name_images, table_name_contents, pg_conf.get_auth())

    
if __name__ == "__main__":
    main()
