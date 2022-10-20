from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as spark_sql
import time

from common import create_spark_session, read_data_from_db


def read_sales(spark: SparkSession, host: str, username: str, password: str) -> DataFrame:
    table = "sales.salesorderheader"
    sales_df = read_data_from_db(spark=spark, connection_str=host, username=username, password=password, table=table)

    return sales_df


def read_sales_order_detail(spark: SparkSession, host: str, username: str, password: str):
    table = "sales.salesorderdetail"
    sales_detail_df = read_data_from_db(spark=spark, connection_str=host, username=username, password=password, table=table)

    return sales_detail_df


def get_sales_by_product(spark: SparkSession, connection_str: str, username: str, password: str) -> DataFrame:
    products = read_data_from_db(spark=spark, connection_str=connection_str, username=username, password=password, table="production.product")
    sales = read_data_from_db(spark=spark, connection_str=connection_str, username=username, password=password, table="sales.salesorderdetail")\
        .repartition(8)

    sales_by_product = sales\
        .withColumn("product_total_in_sale", spark_sql.col("orderqty") * spark_sql.col("unitprice"))\
        .join(products, ["productid"], "inner")\
        .groupby("name")\
        .agg(
            spark_sql.sum("product_total_in_sale").alias("sales_total"),
            spark_sql.sum("orderqty").alias("num_sold")
        )

    return sales_by_product


if __name__ == '__main__':
    connection_str = "jdbc:postgresql://localhost:5432/Adventureworks"
    username = "postgres"
    password = "postgres"

    spark = create_spark_session()

    most_used_products = get_sales_by_product(spark=spark, connection_str=connection_str, username=username, password=password)
    most_used_products.sort(spark_sql.col("num_sold").desc()).show(truncate=False)

    time.sleep(10000)
