from pyspark.sql import SparkSession
import yaml
import os.path
import utils.aws_utils as ut
from pyspark.sql.functions import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--packages "mysql:mysql-connector-java:8.0.15" pyspark-shell'
    )

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read com.test enterprise applications") \
        .master('local[*]') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    src_list = app_conf['source_list']
    for src in src_list:
        src_config = app_conf[src]
        stg_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + src

        # MYSQL Source
        if src == 'TD':
            # use the ** operator/un-packer to treat a python dictionary as **kwargs
            print("\nReading data from MySQL DB using SparkSession.read.format(),")
            mysql_TD_df = ut.mysql_TD(spark, app_secret, src_config)
            mysql_TD_df.show()
            mysql_TD_df.write.partitionBy('insert_date').mode('overwrite').parquet(stg_path)

        # SFTP Source
        elif src == 'OL':
            print("\nReading data from SFTP using SparkSession.read.format(),")
            sftp_OL_df = ut.sftp_OL(spark, app_secret, src_conf["sftp_conf"]["directory"] + "/receipts_delta_GBR_14_10_2017.csv")
            sftp_OL_df.show(5, False)
            sftp_OL_df.write.partitionBy('insert_date').mode('overwrite').parquet(stg_path)

        # MONGODB Source
        elif src == 'CD':
            print("\nReading data from MONGODB using SparkSession.read.format(),")
            mongodb_CD_df = ut.mongodb_CD(spark, src_conf["mongodb_config"]["database"], src_conf["mongodb_config"]["collection"])
            mongodb_CD_df.show()
            mongodb_CD_df.write.partitionBy('insert_date').mode('overwrite').parquet(stg_path)

        # S3 Source
        elif src == 'CP':
            print("\nReading data from S3 Bucket using SparkSession.read.format(),")
            s3_bucket_CP_df = ut.s3_bucket_CP(spark)
            s3_bucket_CP_df.show(5, False)
            s3_bucket_CP_df.write.partitionBy('insert_date').mode('overwrite').parquet(stg_path)

