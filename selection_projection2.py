from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import random

spark = SparkSession \
        .builder \
        .appName("Kafka Streaming") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

car_df_raw = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "car_sales") \
                .option("startingOffsets", "latest") \
                .load()

car_df = car_df_raw.selectExpr("CAST(value AS STRING)", "timestamp")

car_df_schema = "mileage DOUBLE,make STRING,model STRING, \
                fuel STRING,gear STRING,offerType STRING, \
                price DOUBLE,hp INT,year INT"

car_sales_df = car_df \
                .select(from_csv(col("value"), car_df_schema) \
                .alias("car_details"), "timestamp")

car_sales_df = car_sales_df.select("car_details.*", "timestamp")

car_category_column = expr("IF(price > 20000, 'premium', 'regular')")

filtered_car_df = car_sales_df.select(car_sales_df.make,
                                      car_sales_df.model,
                                      car_sales_df.year,
                                      car_sales_df.price,
                                      car_sales_df.mileage) \
                                .where(car_sales_df.year >= 2013) \
                                .withColumn("car_category", car_category_column)


car_df_query = filtered_car_df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("numRows", 10) \
                    .start() \
                    .awaitTermination()
