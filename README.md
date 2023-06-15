# kafka-spark
 
### Create a new topic

```
kafka-topics.sh --create --topic car_sales --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

### Create a spark consumer

```python
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
```

### Run standard producer

```
bin/kafka-console-producer.sh --topic car_sales --bootstrap-server localhost:9092
```
### Run spark consumer
![image.png](https://boostnote.io/api/teams/AZ8Q0azIJ/files/76233ce9c399997e02a4d5a4d5f72c7aa1300da782d350d14bf5481e74a0b971-image.png)


```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 consumer-spark.py
```
![image.png](https://boostnote.io/api/teams/AZ8Q0azIJ/files/d6cd8a5fe6acfb1885dc3323a6f4c34650ea114d1c064b3ca79c63e5c33208c5-image.png)

---
# List of steps
---
## 1. Create a new topic

```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic car_sales
```

## 2. Create a spark kafka consumer (reading_from_kafka.py)

```python
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
```

## 3. Run spark consumer using spark-submit command to create spark cluster

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 reading_from_kafka.py
```

## 4.  Run a kafka-console-producer.sh to insert a few messages

```
./kafka-console-producer.sh --topic car_sales --bootstrap-server localhost:9092
```

Everything what was inserted from kafka in-built producer will show up in spark consumer

## 5. Kill both servers

## 6. Update consumer to apply schema (reading_from_kafka_schema.py)

```python
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

car_df_schema = "mileage DOUBLE,make STRING,model STRING, \
                fuel STRING,gear STRING,offerType STRING, \
                price DOUBLE,hp INT,year INT"

car_sales_df = car_df \
                .select(from_csv(col("value"), car_df_schema) \
                .alias("car_details"), "timestamp")

car_sales_df = car_sales_df.select("car_details.*", "timestamp")

car_df_query = car_sales_df.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("truncate", "false") \
                    .start() \
                    .awaitTermination()
```

## 7. Run spark consumer using spark-submit command to create spark cluster

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 reading_from_kafka_schema.py
```

## 8. Run a kafka-console-producer.sh to insert a few messages

```
./kafka-console-producer.sh --topic car_sales --bootstrap-server localhost:9092
```

```
44800,Kia,Picanto,Gasoline,Automatic,Used,7850,86,2012
81970,Volkswagen,Golf,Gasoline,Manual,Used,7850,105,2012
24000,Dacia,Duster,Gasoline,Manual,Used,7900,105,2012
193333,Volkswagen,Cross Touran,Diesel,Manual,Used,7900,105,2012
```

## 9. Update the consumer to apply where statement and save results to file (reading_from_kafka_schema_where.py)

```python
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

car_df_schema = "mileage DOUBLE,make STRING,model STRING, \
                fuel STRING,gear STRING,offerType STRING, \
                price DOUBLE,hp INT,year INT"

car_sales_df = car_df \
                .select(from_csv(col("value"), car_df_schema) \
                .alias("car_details"), "timestamp")

car_sales_df = car_sales_df.select("car_details.*", "timestamp")

gasoline_car = car_sales_df.select("*").where("fuel == 'Gasoline'")

car_df_query = gasoline_car.writeStream \
                    .outputMode("append") \
                    .format("json") \
                    .option("path", "resoults") \
                    .option("checkpointLocation", 'checkpoint_dir') \
                    .start() \
                    .awaitTermination()
```

## 10. Run spark consumer using spark-submit command to create spark cluster

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 reading_from_kafka_schema_where.py
```

## 11. Insert again multiple rows

```
44800,Kia,Picanto,Gasoline,Automatic,Used,7850,86,2012
81970,Volkswagen,Golf,Gasoline,Manual,Used,7850,105,2012
24000,Dacia,Duster,Gasoline,Manual,Used,7900,105,2012
193333,Volkswagen,Cross Touran,Diesel,Manual,Used,7900,105,2012
```

As a result, instead of having output in console, we have two additional folders created in app directory: results and checkpoint_dir.

the JSON file indeed contains only there rows, where fuel == 'Gasoline'

```json
{"mileage":44800.0,"make":"Kia","model":"Picanto","fuel":"Gasoline","gear":"Automatic","offerType":"Used","price":7850.0,"hp":86,"year":2012,"timestamp":"2023-06-03T19:45:32.720+02:00"}
{"mileage":81970.0,"make":"Volkswagen","model":"Golf","fuel":"Gasoline","gear":"Manual","offerType":"Used","price":7850.0,"hp":105,"year":2012,"timestamp":"2023-06-03T19:45:32.721+02:00"}
{"mileage":24000.0,"make":"Dacia","model":"Duster","fuel":"Gasoline","gear":"Manual","offerType":"Used","price":7900.0,"hp":105,"year":2012,"timestamp":"2023-06-03T19:45:32.721+02:00"}
```

# Reading from multiple topics

To do so, we have to change readStrem configuration. Instead of

```python
car_df_raw = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "car_sales") \
                .load()
```

we have to update this option value

```python
	.option("subscribe", "car_sales") \  to be .option("subscribe", "topic_1,topic_2,topic_n") \
```

or another option is to use regexp, to consume from each topic that its name meets the pattern condition

```python
	.option("subscribe", "car_sales") \ to be .option("subscribePattern", "topic_.*") \
```

# Read from CSV, publish and consume messages that meet condition

## 1. Create a car_data_producer.py

```python
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import sys

"""
App reads content of csv file containing info about various vehicles and then it'll add it on
to a Kafka topic, one vehicle at a time and every three seconds. Basically it will constantly
produce new data for a consumer to pick up.

1. CSV content to pd.DataFrame
2. Publish on the topic
"""

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python <filename.py> <hostname> <port> <topic>", file=sys.stderr)
        exit(-1)
    
    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]

    print("Kafka Producer Application Started ...")

    kafka_producer = KafkaProducer(bootstrap_servers=host + ":" + port,
                                   value_serializer=lambda x: x.encode('utf-8'))
    
    filepath = "./germany-dataset.csv"

    data = pd.read_csv(filepath)
    data_list = data.to_dict(orient="records")
    row = None

    for row in data_list:
        data_fields_value_list = []
        data_fields_value_list.append(row["mileage"])
        data_fields_value_list.append(row["make"])
        data_fields_value_list.append(row["model"])
        data_fields_value_list.append(row["fuel"])
        data_fields_value_list.append(row["gear"])
        data_fields_value_list.append(row["offerType"])
        data_fields_value_list.append(row["price"])
        data_fields_value_list.append(row["hp"])
        data_fields_value_list.append(row["year"])

        row = ','.join(str(v) for v in data_fields_value_list)

        print("Message Type: ", type(row))
        print("Message: ", row)

        kafka_producer.send(topic, row)

        time.sleep(3)

print("Kafka Producer Application Completed!")
```

# 2. Create a selection_projection.py consumer

```python
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

gasoline_car = car_sales_df.select("*") \
                            .where("make == 'Volkswagen'") \
                            .filter("fuel == 'Gasoline'")

car_df_query = gasoline_car.writeStream \
                    .outputMode("append") \
                    .format("console") \
                    .option("numRows", 10) \
                    .start() \
                    .awaitTermination()

```

## 3. Run consumer

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 selection_projection.py
```
## 4. Start producer

```
python3 car_data_producer.py localhost 9092 car_sales
```

Even though consumer reads messages that don't meet the conditions in where and filter, only the messages that meet these conditions are being printed out.

![image.png](https://boostnote.io/api/teams/AZ8Q0azIJ/files/2a0ab768cdb7fb95a52145390af8328c4faecb7ef2d0e3a254b7d99a9ae719e8-image.png)

# Read from topic and publish to another topic

```python
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
```

`withColumn("column_name", expression)` - creates a new column (column_name) that was calculated based on spark SQL expression expr("condition")

```sql
car_category_column = expr("IF(price > 9000, 'premium', 'regular')")
.withColumn("car_category", car_category_column)   -- this row will create a new column "car_category" based on expression defined
												   -- in car_category_column; so if price > 9000 then 'premium', else 'regular'
```

This time the output_df frame will contain additional column 'value' being a contactenation of all passed columns. 
	
```sql
output_df = categorized_car.withColumn('value',
                                       combine_fields_udf(categorized_car.make,
                                                          categorized_car.model,
                                                          categorized_car.year,
                                                          categorized_car.price,
                                                          categorized_car.mileage,
                                                          categorized_car.car_category))
```

Instead of writing message into console or json file, we will append to another kafka topic "car_sales_category". We need to create a new 'value'
column and cast value as string to publish to topic. This step is required when streaming from a Spark DataFram to Kafka topic.

```sql
car_df_query = output_df.selectExpr("CAST(value AS STRING)") \
                    .writeStream \
                    .outputMode("append") \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("checkpointLocation", "checkpoint_dir") \
                    .option("topic", "car_sales_category") \
                    .start() \
                    .awaitTermination()
```

