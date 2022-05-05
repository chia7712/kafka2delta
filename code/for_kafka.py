from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, col, concat, lit

from utils import *

if __name__ == '__main__':
    _args = parse_arguments({"--bootstrap_servers": "address of kafka broker",
                             "--source": "topic source",
                             "--target": "topic target",
                             "--dedup": "true to dedup"})

    _schema = StructType(fields=[StructField("id", LongType()),
                                 StructField("created_timestamp", TimestampType()),
                                 StructField("modified_timestamp", TimestampType()),
                                 StructField("account", StringType()),
                                 StructField("money", LongType()),
                                 StructField("pt_date", DateType())])


    def write_to_kafka(df, brokers, target):
        df.orderBy("id", ascending=False) \
            .dropDuplicates(["id"]) \
            .withColumn("key", col("account")) \
            .withColumn("value", col("money")) \
            .selectExpr("CAST(key as STRING)", "CAST(value AS STRING)", "timestamp") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("kafka.compression.type", "zstd") \
            .option("topic", target) \
            .save()


    if _args.bootstrap_servers:
        if _args.dedup and _args.dedup.lower() == "true":
            ds = SparkSession.builder.getOrCreate().readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", _args.bootstrap_servers) \
                .option("startingOffsets", "earliest") \
                .option("subscribe", _args.source) \
                .load() \
                .withColumn("value", from_csv(col("value").cast("string"), _schema.simpleString())) \
                .select("value.*", "timestamp", "partition") \
                .writeStream \
                .trigger(processingTime='1 seconds') \
                .foreachBatch(lambda df, _: write_to_kafka(df, _args.bootstrap_servers, _args.target)) \
                .start()
        else:
            ds = SparkSession.builder.getOrCreate().readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", _args.bootstrap_servers) \
                .option("startingOffsets", "earliest") \
                .option("subscribe", _args.source) \
                .load() \
                .withColumn("value", from_csv(col("value").cast("string"), _schema.simpleString())) \
                .select("value.*", "timestamp", "partition") \
                .withColumn("key", col("account")) \
                .withColumn("value", col("money")) \
                .selectExpr("CAST(key as STRING)", "CAST(value AS STRING)") \
                .writeStream \
                .trigger(processingTime='1 seconds') \
                .format("kafka") \
                .option("kafka.bootstrap.servers", _args.bootstrap_servers) \
                .option("kafka.compression.type", "zstd") \
                .option("topic", _args.target) \
                .option("checkpointLocation", "/tmp/testKafka") \
                .start()
        ds.awaitTermination()
