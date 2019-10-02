# Table of Contents
1. [Problem](README.md#problem)
2. [Approach](README.md#approach)
3. [Dependencies](README.md#dependencies)
4. [Run](README.md#run)
5. [Tests](README.md#tests)

# Problem

This is my Insight Data Engineering project.

**Objective:** Build a data pipeline that compares millions of `jpg` files and output a SQL table, which contains information about duplicated content.

Some of the problems to consider include the following:

* The dataset is big (2 million images; 500 GB+). I must push the data quickly through the pipeline so that I can iteratively refine it.
* There are more than one way to detect duplicate content.
* Some algorithms may detect not only the duplicate content, but also similar content. That's okay.
* Benchmark and evaluate pros and cons for each algorithm.
* Exactly four weeks are allowed from brainstorming to completion.
* I had no prior experience with AWS, Spark, or PostgreSQL, so I must learn as I go.

# Approach

1. Transfer [Open Image Dataset](https://github.com//cvdfoundation/open-images-dataset) to your Amazon S3 bucket.
2. Set up Spark cluster (3 workers, 18 cores) using [Pegasus](https://github.com/InsightDataScience/pegasus)
3. Load the data into DataFrame using Apache Spark.
4. Iterate through each image and do one of the following (not all will be available):
	* Base64 encode (with resampling to reduce resolution)
	* Calculate md5 checksum
	* Create trie-like structure and do pixel-by-pixel comparison
	* Calculate Euclidean distance on a linearized vector against other images
	* Compare features using SIFT algorithm
5. Output the result to PostgreSQL.
6. Build a simple frontend to visualize the result.

I coded in **Python 3**.

# Dependencies
* Authentication for PostgreSQL. Create `postgres_credentials.json` at root level. It should contain the following fields:
```json
{
    "user":"xxxx",
    "password":"xxxx",
    "host":"x.x.x.x",
    "port":"xxxx",
    "database":"xxxx"
}
```
* [JDBC driver](https://jdbc.postgresql.org/download.html): Download to `~/drivers/` on master node
* [boto3](https://github.com/boto/boto3): Install on all nodes
* [scikit-image](https://scikit-image.org/docs/dev/install.html): Install on all nodes

# Run

1. From the Terminal, cd to the `src` directory
2. Execute the following command:
```bash
spark-submit --master spark://<master DNS>:7077 --jars ~/drivers/postgresql-42.2.8.jar duplicate_detector.py <bucket name> [--method <method name> --region <region name> --dir <directory name>]
```
### Supported method
* `checksum`: Calculate md5 checksum
* `base`: Base64 encoding
* `base_small` Low resolution resampling followed by Base64 encoding

For more details, use `-h` option:
```bash
spark-submit --master spark://<master DNS>:7077 --jars ~/drivers/postgresql-42.2.8.jar duplicate_detector.py -h
```
3. Log onto PostgreSQL database and review `images_<method name>_<directory name>` and `contents_<method name>_<directory name>` tables

# Tests
The following test cases (in Amazon S3) are used for benchmark:
1. `test_1`: A set of 15 `jpg` files (5.3 MB) containing one set of duplicate and one set of triplicate
2. `test_2`: A set of 120 `jpg` files (40.5 MB) containing one set of duplicate and one set of triplicate
3. `test_3`: A set of 10 `jpg` files (7.3 MB) containing the same image with watermark in different locations
4. `test_4`: A set of 3421 `jpg` files (1.0 GB) containing one set of duplicate and one set of triplicate
