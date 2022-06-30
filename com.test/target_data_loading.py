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

    print("\nReading data ingested into S3 bucket")
    file_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + 'CP'
    CP_df = spark.read\
        .option("header", "true")\
        .option("delimiter", "|")\
        .parquet(file_path)
    CP_df.show(5, False)

    #Create a temporary table
    CP_df.createOrReplaceTempView("CP")
    CP_df.printSchema()
    #spark.sql("select * from CP").show()

    spark.sql(app_conf['REGIS_DIM_1']['loadingQuery']).show(5, False)

    print("\nReading customer data ingested into MongoDB")
    file_path = 's3a://' + app_conf['s3_conf']['s3_bucket'] + '/' + app_conf['s3_conf']['staging_location'] + '/' + 'CD'
    CD_df = spark.read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .parquet(file_path)
    CD_df.show(5, False)

    # Create a temporary table
    CD_df.createOrReplaceTempView("CD")
    CD_df.printSchema()
    # spark.sql("select * from CP").show()

    spark.sql(app_conf['REGIS_DIM']['loadingQuery']).show(5, False)

