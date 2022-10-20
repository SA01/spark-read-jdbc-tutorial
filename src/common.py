from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf


def create_spark_session() -> SparkSession:
    conf = SparkConf().set("spark.driver.memory", "8g")

    spark_session = SparkSession\
        .builder\
        .master("local[4]")\
        .config(conf=conf)\
        .appName("Read from JDBC tutorial") \
        .config("spark.jars", "postgresql-42.5.0.jar") \
        .getOrCreate()

    return spark_session


def read_data_from_db(spark: SparkSession, connection_str: str, username: str, password: str, table: str) -> DataFrame:
    properties = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    data_df = spark.read.jdbc(
        url=connection_str,
        properties=properties,
        table=table
    )

    return data_df
