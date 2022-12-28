from pyspark.sql import SparkSession
import yaml
import os.path
import utils.aws_utils as ut
from pyspark.sql.functions import *

if __name__ == '__main__':

    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        '--jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar"\
         --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" pyspark-shell'
    )

    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("Read com.test enterprise applications") \
        .master('local[*]') \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    tgt_list = app_conf['target_list']
        for tgt in tgt_list:
            print('Preparing', tgt, 'data,')
            tgt_conf = app_conf[tgt]

            if tgt == 'REGIS_DIM':
                print('Loading the source data from S3,')
                for src in tgt_conf['source_data']:
                    file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src

                    src_df = ut.read_parquet_from_s3(spark, file_path)
                    src_df.show()
                    if src == 'CD':
                        src_df = src_df.withColumn("street", col("address.street")) \
                            .withColumn("city", col("address.city")) \
                            .withColumn("state", col("address.state")) \
                            .drop("address")

                    src_df.createOrReplaceTempView(src)
                    src_df.printSchema()
                    src_df.show(5, False)

                print('Preparing the', tgt, 'data,')
                regis_dim_df = spark.sql(tgt_conf['loadingQuery'])
                regis_dim_df.show()

                jdbc_url = ut.get_redshift_jdbc_url(app_secret)
                print(jdbc_url)

                print("Writing data in redshift")
                ut.write_data_to_Redshift(regis_dim_df,
                                                   jdbc_url,
                                                   "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                                   tgt_conf['tableName'])
                print("Completed   <<<<<<<<<")

            elif tgt == 'CHILD_DIM':
                src = tgt_conf['source_data']
                file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src

                src_df = ut.read_parquet_from_s3(spark, file_path)

                src_df.createOrReplaceTempView(src)
                src_df.printSchema()
                src_df.show(5, False)

                print('Preparing the', tgt, 'data,')
                child_dim_df = spark.sql(tgt_conf['loadingQuery'])
                child_dim_df.show()

                jdbc_url = ut.get_redshift_jdbc_url(app_secret)
                print(jdbc_url)

                print("Writing data in redshift")
                ut.write_data_to_Redshift(child_dim_df,
                                                   jdbc_url,
                                                   "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                                   tgt_conf['tableName'])
                print("Completed   <<<<<<<<<")

            elif tgt == 'RTL_TXN_FCT':
                for tgt in tgt_list:
                    print('Preparing', tgt, 'data,')
                    tgt_conf = app_conf[tgt]

                    # if tgt == 'RTL_TXN_FCT':
                    print('Loading the source data,')
                    for src in tgt_conf['source_data']:
                        file_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"] + "/" + src

                        src_df = ut.read_parquet_from_s3(spark, file_path)
                        src_df.createOrReplaceTempView(src)
                        src_df.printSchema()
                        src_df.show(5, False)

                    jdbc_url = ut.get_redshift_jdbc_url(app_secret)
                    print(jdbc_url)

                    REGIS_DIM = spark.read \
                        .format("io.github.spark_redshift_community.spark.redshift") \
                        .option("url", jdbc_url) \
                        .option("tempdir", "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp") \
                        .option("forward_spark_s3_credentials", "true") \
                        .option("dbtable", "DATAMART.REGIS_DIM") \
                        .load()

                    REGIS_DIM.show(5, False)
                    REGIS_DIM.createOrReplaceTempView("REGIS_DIM")

                    print('Preparing the', tgt, 'data,')
                    rtl_txn_fct_df = spark.sql(tgt_conf['loadingQuery'])
                    rtl_txn_fct_df.show()

                    print("Writing data in redshift")
                    ut.write_data_to_Redshift(rtl_txn_fct_df,
                                                  jdbc_url,
                                                  "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/temp",
                                                  tgt_conf['tableName'])
                    print("Completed   <<<<<<<<<")


# spark-submit --jars "https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/1.2.36.1060/RedshiftJDBC42-no-awssdk-1.2.36.1060.jar" --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py
#  spark-submit  --packages "org.apache.spark:spark-avro_2.11:2.4.2,io.github.spark-redshift-community:spark-redshift_2.11:4.0.1,org.apache.hadoop:hadoop-aws:2.7.4" com/pg/target_data_loading.py


# print("\nReading data ingested into S3 bucket")
# file_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + 'CP'
# CP_df = spark.read\
#     .option("header", "true")\
#     .option("delimiter", "|")\
#     .parquet(file_path)
# CP_df.show(5, False)
#
# #Create a temporary table
# CP_df.createOrReplaceTempView("CP")
# CP_df.printSchema()
# #spark.sql("select * from CP").show()
#
# spark.sql(app_conf['REGIS_DIM_1']['loadingQuery']).show(5, False)
#
# print("\nReading customer data ingested into MongoDB")
# file_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + 'CD'
# CD_df = spark.read \
#     .option("header", "true") \
#     .option("delimiter", "|") \
#     .parquet(file_path)
# CD_df.show(5, False)
#
# # Create a temporary table
# CD_df.createOrReplaceTempView("CD")
# CD_df.printSchema()
# # spark.sql("select * from CP").show()
#
# # spark.sql("""
# #        select consumer_id, street. City, state
# #        from CD
# #        INNER JOIN CP on CP.REGIS_CNSM_ID = CD.consumer_id
# #""").show()
#
# # spark.sql("""
# #        select CP.*, CD.street, CD.City, CD.state
# #        from CD
# #        INNER JOIN CP on CP.CNSM_ID = CD.consumer_id
# #""").show()
#
# spark.sql(app_conf['REGIS_DIM']['loadingQuery']).show(5, False)