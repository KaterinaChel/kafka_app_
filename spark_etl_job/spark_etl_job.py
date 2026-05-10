from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

# 1. Создаем Spark сессию с поддержкой Kafka
spark = SparkSession.builder \
    .appName("Kafka_to_HDFS_ETL") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# 2. Определяем схему для сообщения о товаре
product_schema = StructType() \
    .add("product_id", StringType()) \
    .add("name", StringType()) \
    .add("price_amount", DoubleType()) \
    # ... остальные поля

# 3. Читаем поток из Kafka (второй кластер)
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-4:8082") \
    .option("subscribe", "shop_products") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Парсим JSON и приводим типы
parsed_df = kafka_stream_df \
    .select(from_json(col("value").cast("string"), product_schema).alias("data")) \
    .select("data.*") \
    .withColumn("processed_at", to_timestamp(col("timestamp"))) # Добавляем служебное поле

# 5. Записываем в HDFS в виде партиционированного Parquet
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/data_lake/shop_products") \
    .option("checkpointLocation", "/checkpoints/shop_products_etl") \
    .partitionBy("category") \
    .trigger(processingTime="5 minutes") \
    .start()

query.awaitTermination()