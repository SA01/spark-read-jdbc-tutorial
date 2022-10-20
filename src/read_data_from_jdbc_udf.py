from typing import Callable

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as spark_sql
import psycopg2
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, LongType, DecimalType, TimestampType
import time

from common import create_spark_session


query_schema = ArrayType(StructType(fields=[
    StructField(name="salesorderid", dataType=LongType()),
    StructField(name="salesorderdetailid", dataType=LongType()),
    StructField(name="carriertrackingnumber", dataType=StringType()),
    StructField(name="orderqty", dataType=LongType()),
    StructField(name="productid", dataType=LongType()),
    StructField(name="specialofferid", dataType=LongType()),
    StructField(name="unitprice", dataType=DecimalType()),
    StructField(name="unitpricediscount", dataType=DecimalType()),
    StructField(name="rowguid", dataType=StringType()),
    StructField(name="modifieddate", dataType=TimestampType())
]))


def read_data_from_query(
        spark: SparkSession, connection_str: str, username: str, password: str, query: str
) -> DataFrame:
    data_df = spark.read\
        .format("jdbc")\
        .option("url", connection_str)\
        .option("query", query)\
        .option("user", username)\
        .option("password", password)\
        .option("driver", "org.postgresql.Driver")\
        .load()

    return data_df


def read_sales_order_detail(host: str, username: str, password: str, db: str) -> Callable[[DataFrame], DataFrame]:
    def read_sales_order_detail_inner(product_ids: DataFrame) -> DataFrame:
        # Create query prefix
        query_base = "SELECT salesorderid, salesorderdetailid, carriertrackingnumber, orderqty, productid, " \
                     "specialofferid, unitprice, unitpricediscount, rowguid, modifieddate " \
                     "FROM sales.salesorderdetail where productid in ("

        # Create dataframe with queries for each product id mod
        df_with_queries = product_ids\
            .groupby("product_id_mod")\
            .agg(
                spark_sql.collect_set("productid").alias("product_ids")
            )\
            .withColumn("product_ids", spark_sql.array_join(spark_sql.col("product_ids"), ","))\
            .withColumn(
                "query",
                spark_sql.concat(spark_sql.lit(query_base), spark_sql.col("product_ids"), spark_sql.lit(")"))
            )

        # Load data for each query. Explode and project to create rows
        df_with_data = df_with_queries.withColumn(
            "data",
            read_data_piece_from_db(
                spark_sql.col("query"),
                spark_sql.lit(host),
                spark_sql.lit(username),
                spark_sql.lit(password),
                spark_sql.lit(db)
            )
        )\
        .withColumn("data", spark_sql.explode(spark_sql.col("data")))\
        .select("data.*")

        return df_with_data
    return read_sales_order_detail_inner


@spark_sql.udf(returnType=query_schema)
def read_data_piece_from_db(query: str, host: str, username: str, password: str, database: str):
    try:
        connection = psycopg2.connect(host=host, user=username, password=password, dbname=database)
        cursor = connection.cursor()
        cursor.execute(query=query)

        result = cursor.fetchall()
        result_list = [
            (item[0], item[1], str(item[2]), item[3], item[4], item[5], item[6], item[7], item[8], item[9])
            for item in result
        ]

        connection.close()
        return result_list
    except Exception as e:
        print(e)
        raise e


def read_product_ids(spark: SparkSession, host: str, username: str, password: str, db: str) -> DataFrame:
    num_partitions = 8
    connection_str = f"jdbc:postgresql://{host}:5432/{db}"

    product_id_query = "select productid, count(1) as num_rows from sales.salesorderdetail group by productid"

    product_ids = read_data_from_query(
        spark=spark, connection_str=connection_str, username=username, password=password, query=product_id_query
    )
    return product_ids\
        .withColumn("product_id_mod", spark_sql.col("productid") % spark_sql.lit(20)) \
        .repartition(num_partitions, "product_id_mod")


if __name__ == '__main__':
    host="localhost"
    db="Adventureworks"
    username = "postgres"
    password = "postgres"

    spark = create_spark_session()

    sales_order_detail = read_product_ids(spark=spark, host=host, username=username, password=password, db=db)\
        .transform(read_sales_order_detail(host=host, username=username, password=password, db=db))

    sales_order_detail\
        .groupby("productid")\
        .agg(spark_sql.count(spark_sql.lit(1)).alias("num_rows"))\
        .sort(spark_sql.col("num_rows").desc())\
        .show(truncate=False, n=1000)

    time.sleep(1000)
