from typing import List

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as spark_sql
import time
import numpy as np

from common import create_spark_session, read_data_from_db


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


def read_product_ids(spark: SparkSession, connection_str: str, username: str, password: str) -> List[str]:
    product_id_query = "select productid, count(1) as num_rows from sales.salesorderdetail group by productid"

    product_ids = read_data_from_query(
        spark=spark, connection_str=connection_str, username=username, password=password, query=product_id_query
    ).select("productid").collect()

    product_ids = [str(item['productid']) for item in product_ids]

    return product_ids


def read_data_from_db_with_predicates(
        spark: SparkSession, connection_str: str, username: str, password: str, table: str, predicates: List[str]
    ) -> DataFrame:
    properties = {
        "user": username,
        "password": password,
        "driver": "org.postgresql.Driver"
    }
    sales_df = spark.read.jdbc(
        url=connection_str,
        properties=properties,
        table=table,
        predicates=predicates
    )

    return sales_df


def create_predicates(spark: SparkSession, connection_str: str, username: str, password: str) -> List[str]:
    num_partitions = 50
    predicate_values = read_product_ids(spark=spark, connection_str=connection_str, username=username,
                                        password=password)
    predicates = np.array_split(predicate_values, num_partitions)
    predicates = ["productid in (" + ",".join(predicate_set) + ")" for predicate_set in predicates]
    # predicates.extend(predicates[0:2])
    [print(pred_stmt) for pred_stmt in predicates]
    return predicates


def read_sales_order_detail_with_predicates(spark: SparkSession, connection_str: str, username: str, password: str) -> DataFrame:
    table = "sales.salesorderdetail"
    predicates = create_predicates(spark=spark, connection_str=connection_str, username=username, password=password)

    sales_order_detail_df = read_data_from_db_with_predicates(
        spark=spark, connection_str=connection_str, username=username, password=password, table=table, predicates=predicates
    )
    print(sales_order_detail_df.count())

    return sales_order_detail_df


def get_sales_by_product(spark: SparkSession, connection_str: str, username: str, password: str) -> DataFrame:
    products = read_data_from_db(spark=spark, connection_str=connection_str, username=username, password=password, table="production.product")
    sales = read_sales_order_detail_with_predicates(spark=spark, connection_str=connection_str, username=username, password=password)

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
