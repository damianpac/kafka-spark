from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

car_df_query = car_df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("truncate", "false") \
                    .start() \
                    .awaitTermination()