from __future__ import print_function

__version__ = '0.7.5'

import argparse
import io
import boto3
import base64
import hashlib
import numpy as np
import skimage.io as skio
import skimage.transform as transform
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
import pgconf as pc


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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Duplicate Detector")
    
    parser.add_argument("method", type = str, nargs = 1,
                        metavar = "algorithm", default = ["checksum"],
                        help = "Method of detecting duplicates. The default \
                        is 'checksum'.")

    parser.add_argument("bucket", type = str, nargs = 1,
                        metavar = "bucket_name", default = None,
                        help = "Name of the S3 bucket where the files are stored.")

    parser.add_argument("-r", "--region", type = str, nargs = 1,
                        metavar = "region_name", default = ["us-west-2"],
                        help = "Name of the region where the S3 bucket is located. \
                        The default is 'us-west-2'.")
    
    parser.add_argument("-d", "--dir", type = str, nargs = 1,
                        metavar = "directory", default = ["validation"],
                        help = "Name of the directory where the files are located. \
                        The default is 'validation'.")
    
    args = parser.parse_args()
    
    if args.method != None: method_name = args.method[0]
    if args.bucket != None: bucket_name = args.bucket[0]
    if args.region != None: region_name = args.region[0]
    if args.dir != None: dir_name = args.dir[0]
    
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
        .withColumn("img_id", F.monotonically_increasing_id())
    
    # Save the DataFrame to PostgreSQL table. 
    pg_conf = pc.PostgresConfigurator()
    df.write.jdbc(url = pg_conf.get_url(),
                  table = 'images', 
                  mode = 'overwrite', 
                  properties = pg_conf.get_properties())

    spark.stop()
