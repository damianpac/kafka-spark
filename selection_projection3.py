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

car_category_column = expr("IF(price > 9000, 'premium', 'regular')")

categorized_car = car_sales_df.select(car_sales_df.make,
                                      car_sales_df.model,
                                      car_sales_df.year,
                                      car_sales_df.price,
                                      car_sales_df.mileage) \
                                .withColumn("car_category", car_category_column)

def combine_fields_fn(make, model, year, price, mileage, category):
    # returns csv-like string
    return str(make) + "," \
            + str(model) + "," \
            + str(year) + "," \
            + str(price) + "," \
            + str(mileage) + "," \
            + str(category) + "," \

combine_fields_udf = udf(combine_fields_fn, StringType())

output_df = categorized_car.withColumn('value',
                                       combine_fields_udf(categorized_car.make,
                                                          categorized_car.model,
                                                          categorized_car.year,
                                                          categorized_car.price,
                                                          categorized_car.mileage,
                                                          categorized_car.car_category)
)

car_df_query = output_df.selectExpr("CAST(value AS STRING)") \
                    .writeStream \
                    .outputMode("append") \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("checkpointLocation", "checkpoint_dir") \
                    .option("topic", "car_sales_category") \
                    .start() \
                    .awaitTermination()
