__version__ = '0.5.1'

import psycopg2
from psycopg2 import Error
import json
import base64
import hashlib
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

def get_postgres_credentials():
    '''
    Return credentials for PostgreSQL
    '''
    with open("../postgres_credentials.json", "r") as read_file:
        auth = json.load(read_file)

    return auth

def encode(data, mode=1):
    '''
    Return encoded string 
    0: base64 (first 50)
    1: md5
    '''
    if mode == 0:
        return base64.b64encode(data).decode()[:50]
    else:
        return hashlib.md5(data).hexdigest()

def insert(row):
    '''
    Insert a row into PostgreSQL
    '''
    file_path = row[0]
    data = row[1]
    try:
        auth = get_postgres_credentials()
        
        connection = psycopg2.connect(user = auth['user'],
                                      password = auth['password'],
                                      host = auth['host'],
                                      port = auth['port'],
                                      database = auth['database'])
        cursor = connection.cursor()

        encoded_str = encode(data)
        q = "INSERT INTO images (path, encoding) VALUES ('" + file_path + "', '" + encoded_str + "');"
        cursor.execute(q)
        connection.commit()
    except () as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL closed")

if __name__ == "__main__":
    
    sc = SparkContext('local')
    spark = SparkSession(sc)

    # Create image DataFrame using image data source in Apache Spark 2.4
    image_df = spark.read.format("image").load('s3a://femto-data/test_1/')
    
    image_df.select("image.origin", "image.data").foreach(insert)
