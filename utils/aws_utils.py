import os.path
from pyspark.sql.functions import *
import utils.aws_utils as ut
from pyspark import *
import yaml


def get_redshift_jdbc_url(redshift_config: dict):
    host = redshift_config["redshift_conf"]["host"]
    port = redshift_config["redshift_conf"]["port"]
    database = redshift_config["redshift_conf"]["database"]
    username = redshift_config["redshift_conf"]["username"]
    password = redshift_config["redshift_conf"]["password"]
    return "jdbc:redshift://{}:{}/{}?user={}&password={}".format(host, port, database, username, password)


def get_mysql_jdbc_url(mysql_config: dict):
    host = mysql_config["mysql_conf"]["hostname"]
    port = mysql_config["mysql_conf"]["port"]
    database = mysql_config["mysql_conf"]["database"]
    return "jdbc:mysql://{}:{}/{}?autoReconnect=true&useSSL=false".format(host, port, database)

#MYSQL Source
def mysql_TD(spark, app_secret, table_name, partition_col):
    print("\nReading data from MYSQL DB,")
    jdbc_params = {"url": ut.get_mysql_jdbc_url(app_secret),
                   "lowerBound": "1",
                   "upperBound": "100",
                   "dbtable": table_name,
                   "numPartitions": "2",
                   "partitionColumn": partition_col,
                   "user": app_secret["mysql_conf"]["username"],
                   "password": app_secret["mysql_conf"]["password"]
                   }
    # print(jdbcParams)

    # use the ** operator/un-packer to treat a python dictionary as **kwargs
    #print("\nReading data from MySQL DB using SparkSession.read.format(),")
    txnDF = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load() \
        .withColumn('insert_date', current_date())
    return txnDF

#SFTP Source
def sftp_OL(spark, current_dir, app_secret, pem_path, file_path):
    ol_txn_df = spark.read \
        .format("com.springml.spark.sftp") \
        .option("host", app_secret["sftp_conf"]["hostname"]) \
        .option("port", app_secret["sftp_conf"]["port"]) \
        .option("username", app_secret["sftp_conf"]["username"]) \
        .option("pem", pem_path) \
        .option("fileType", "csv") \
        .option("delimiter", "|") \
        .load(file_path) \
        .withColumn('insert_date', current_date())

    return ol_txn_df

#MONGODB Student Source
def mongodb_SD(spark, dbName, collName):
    student_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", dbName) \
        .option("collection", collName) \
        .load() \
        .withColumn('insert_date', current_date())

    return student_df

#MONGODB Customer Source
def mongodb_CD(spark, dbName, collName):
    customer_df = spark \
        .read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("database", dbName) \
        .option("collection", collName) \
        .load() \
        .withColumn('insert_date', current_date())

    return customer_df

#S3 Source
#def s3_bucket_CP(spark):
#    campaigns_df = spark \
#        .read \
#        .csv('s3://spark-s3-bucket-01/KC_Extract_1_20171009.csv') \
#        .withColumn('insert_date', current_date())

#    return campaigns_df

def s3_bucket_CP(spark, path):
    campaigns_df = spark \
        .read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .csv(path) \
        .withColumn('insert_date', current_date())

    return campaigns_df

def read_parquet_from_s3(spark,file_path):
    return spark.read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .parquet(file_path)


def write_data_to_Redshift(txn_df,jdbc_url,s3_path,redshift_table_name):
    txn_df.coalesce(1).write \
            .format("io.github.spark_redshift_community.spark.redshift") \
            .option("url", jdbc_url) \
            .option("tempdir", s3_path) \
            .option("forward_spark_s3_credentials", "true") \
            .option("dbtable", redshift_table_name) \
            .mode("overwrite") \
            .save()