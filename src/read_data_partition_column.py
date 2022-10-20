from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as spark_sql
import time

from common import create_spark_session, read_data_from_db


def read_data_from_db_with_partition_column(
        spark: SparkSession, connection_str: str, username: str, password: str, table: str,
        partition_column: str, upper_bound: int, lower_bound: int, num_partitions: int
    ) -> DataFrame:
    properties = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver",
        "partitionColumn": partition_column,
        "upperBound": str(upper_bound),
        "lowerBound": str(lower_bound),
        "numPartitions": str(num_partitions)
    }
    sales_df = spark.read.jdbc(
        url=connection_str,
        properties=properties,
        table=table
    )

    return sales_df


def read_sales_order_detail_with_limits(spark: SparkSession, connection_str: str, username: str, password: str) -> DataFrame:
    table = "sales.salesorderdetail"
    partition_column = "salesorderdetailid"
    upper_bound = 121317
    lower_bound = 1

    num_partitions = 50

    sales_order_detail_df = read_data_from_db_with_partition_column(
        spark=spark, connection_str=connection_str, username=username, password=password, table=table,
        partition_column=partition_column, upper_bound=upper_bound, lower_bound=lower_bound,
        num_partitions=num_partitions
    )

    return sales_order_detail_df


def get_sales_by_product(spark: SparkSession, connection_str: str, username: str, password: str) -> DataFrame:
    products = read_data_from_db(spark=spark, connection_str=connection_str, username=username, password=password, table="production.product")
    sales = read_sales_order_detail_with_limits(spark=spark, connection_str=connection_str, username=username, password=password)

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
