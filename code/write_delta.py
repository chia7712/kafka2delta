import time

from confluent_kafka import Producer
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, col

from utils import *


def create_delta_table(spark, delta_path, metadata):
    # add the partition column (it is different from partition_by)
    DeltaTable.createIfNotExists(spark).location(delta_path) \
        .addColumns(metadata.struct_fields) \
        .addColumn(metadata.partition_column_name, metadata.partition_column_type, nullable=True) \
        .partitionedBy(metadata.partition_column_name, metadata.partition_column_name) \
        .execute()


def log(bootstrap_servers, metadata, partition_values, kafka_time, fetch_time, partition_time, delta_time, batch_size):
    p = Producer({'bootstrap.servers': bootstrap_servers})
    data = f"""
            {{
                "topic": "{metadata.topic}",
                "delta_folder": "{metadata.delta_folder}",
                "csv_folder": "{metadata.csv_folder}",
                "batch_size": {batch_size},
                "partition_values": "{partition_values}",
                "kafka_time": "{kafka_time}",
                "fetch_time": "{fetch_time}",
                "partition_time": "{partition_time}",
                "delta_time": "{delta_time}"
            }}
        """
    p.produce('log', value=data.encode('utf-8'))
    p.flush()


def merge(spark, metadata, delta_path, data_frame, bootstrap_servers):
    # the following generated query won't work with empty data frame, so we skip empty data frame
    if len(data_frame.head(1)) == 0:
        return
    _fetch_time = time.strftime("%Y-%m-%d %H:%M:%S")
    _partition_values = ",".join([f"'{row[metadata.partition_column_name]}'" for row in
                                  data_frame.select(metadata.partition_column_name).distinct().collect()])
    _partition_cond = f"previous.{metadata.partition_column_name} IN ({_partition_values})"
    _merge_cond = " AND ".join(
        ["%s.%s = %s.%s" % ("previous", _column, "updates", _column) for _column in metadata.pks])
    _cond = f"{_partition_cond} AND {_merge_cond}"
    _batch_size = data_frame.count()
    # the head row is the "first" record from kafka topic, but it may NOT the earliest record. However, it is too
    # expensive to seek the min timestamp of whole batch, so we take first record instead.
    _kafka_time = data_frame.head()["timestamp"]
    _partition_time = time.strftime("%Y-%m-%d %H:%M:%S")
    DeltaTable.forPath(spark, delta_path) \
        .alias("previous") \
        .merge(data_frame
               .drop("timestamp")
               .orderBy(metadata.order_by, ascending=False)
               .dropDuplicates(metadata.pks)
               .alias("updates"), _cond) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    _delta_time = time.strftime("%Y-%m-%d %H:%M:%S")
    # log the result for this batch
    log(bootstrap_servers, metadata, _partition_values, _kafka_time, _fetch_time, _partition_time, _delta_time, _batch_size)


def struct_type(metadata):
    # the partition column is already in kafka record (csv), so we have to add it now to parse csv correctly
    return StructType(metadata.struct_fields) \
        .add(metadata.partition_column_name, metadata.partition_column_type, nullable=True)


def run_topic_stream(spark, metadata, delta_path, bootstrap_servers):
    # we don't assign kafka partition, so using subscription is good now.
    spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", f"{metadata.max_offsets_per_trigger}") \
        .option("subscribe", metadata.topic) \
        .load() \
        .withColumn("value", from_csv(col("value").cast("string"), struct_type(metadata).simpleString())) \
        .select("value.*", "timestamp") \
        .writeStream \
        .trigger(processingTime=f'{metadata.processing_time} seconds') \
        .foreachBatch(lambda df, _: merge(spark, metadata, delta_path, df, bootstrap_servers)) \
        .start()


if __name__ == '__main__':

    _args = parse_arguments({"--bootstrap_servers": "kafka brokers",
                             "--schema_file": "xml file saving the table schema",
                             "--output": "output path to save all delta tables",
                             "--log_level": "log level"})

    if _args.bootstrap_servers and _args.schema_file and _args.output:
        _spark = SparkSession.builder.getOrCreate()
        _metadata = read_metadata(_args.schema_file)
        _existent_topics = topics(_args.bootstrap_servers)

        # INFO level is too verbose
        _log_level = "WARN"
        if _args.log_level:
            _log_level = _args.log_level
        _spark.sparkContext.setLogLevel(_log_level)

        for _, _table_meta in _metadata.items():
            if _table_meta.topic in _existent_topics:
                _delta_path = f"{_args.output}/{_table_meta.delta_folder}"
                create_delta_table(_spark, _delta_path, _table_meta)
                run_topic_stream(_spark, _table_meta, _delta_path, _args.bootstrap_servers)
        for s in _spark.streams.active:
            s.awaitTermination()
