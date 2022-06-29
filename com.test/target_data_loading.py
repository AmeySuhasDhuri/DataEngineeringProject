import spark as spark
from pyspark.sql import SparkSession
import yaml
import os.path
import utils.aws_utils as ut
from pyspark.sql.functions import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read com.test enterprise applications") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    print("\nReading data ingested into S3 bucket")
    file_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + 'CP'
    txn_df = spark.read.parquet(file_path)
    txn_df.show(5, False)

    #Create a temporary table
    txn_Df.createOrReplaceTempView("CP")

    spark.sql("""
            SELECT
                MONOTONICALLY_INCREASING_ID() AS REGIS_KEY, REGIS_CNSM_ID AS CNSM_ID,REGIS_CTY_CODE AS CTY_CODE,
                REGIS_ID, REGIS_DATE, REGIS_LTY_ID AS LTY_ID, REGIS_CHANNEL, REGIS_GENDER, REGIS_CITY, Street, City, State, INS_DT
            FROM
                (SELECT
                    DISTINCT REGIS_CNSM_ID, CAST(REGIS_CTY_CODE AS SMALLINT), CAST(REGIS_ID AS INTEGER),
                    REGIS_LTY_ID, REGIS_DATE, REGIS_CHANNEL, REGIS_GENDER, REGIS_CITY, ADDR.street as Street, ADDR.City as City, ADDR.state as State, CP.INS_DT as INS_DT
            FROM
        CP JOIN ADDR ON CP.REGIS_CNSM_ID = ADDR.consumer_id
      ) CP
            """) \
    .show(5, False)