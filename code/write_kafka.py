import os
import time

from confluent_kafka.cimpl import NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_join, lit, hash, abs, to_date

from utils import *


def create_topic(bootstrap_servers, metadata, recreate):
    # By default, Spark has a 1-1 mapping of topicPartitions to Spark partitions. Hence, we pre-partition the topic.
    _config = {"cleanup.policy": "compact", "max.compaction.lag.ms": "10000"} if metadata.compact else {}
    _new_topic = NewTopic(topic=metadata.topic,
                          num_partitions=metadata.partitions,
                          replication_factor=1,
                          config=_config)
    _admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    # recreate topic
    if metadata.topic in topics(bootstrap_servers) and recreate:
        for _, deleteFuture in _admin.delete_topics([metadata.topic]).items():
            deleteFuture.result()

    # wait cluster to sync deletion
    time.sleep(3)

    if metadata.topic not in topics(bootstrap_servers):
        for _, f in _admin.create_topics([_new_topic]).items():
            f.result()


def all_string_types(columns):
    return StructType([StructField(_column, StringType(), nullable=True) for _column in columns])


def write_to_kafka(df, metadata, brokers):
    cols = [col(_c) for _c in metadata.columns] + [col(metadata.partition_column_name)]
    pks = [col(_c) for _c in metadata.pks]
    # noted that we don't define kafka partition now. We use the date data to assign delta partition, so all data in
    # same date will be hosted by same broker if we assign kafka partition.
    df.orderBy(metadata.order_by, ascending=False) \
        .dropDuplicates(metadata.pks) \
        .withColumn("key", array(pks)) \
        .withColumn("key", array_join(col("key"), ",")) \
        .withColumn("value", array(cols)) \
        .withColumn("value", array_join(col("value"), ",", null_replacement="")) \
        .selectExpr("CAST(key as STRING)", "CAST(value AS STRING)") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("kafka.compression.type", "zstd") \
        .option("topic", metadata.topic) \
        .save()


def run_csv_stream(spark, csv_source, archive_path, metadata, brokers):
    # we addd partition data and column according to partition_by now. It will add some cost (larger kafka record),
    # but it can save the time of calculating partition data when merging delta data. Also, the schema in kafka
    # can be consistent with delta table.
    spark.readStream \
        .schema(all_string_types(metadata.columns)) \
        .option("recursiveFileLookup", "true") \
        .option("cleanSource", "archive") \
        .option("sourceArchiveDir", archive_path) \
        .option("spark.sql.streaming.fileSource.cleaner.numThreads", "3") \
        .csv(csv_source) \
        .withColumn(metadata.partition_column_name, to_date(col(metadata.partition_by))) \
        .writeStream \
        .foreachBatch(lambda df, _: write_to_kafka(df, metadata, brokers)) \
        .start()


if __name__ == '__main__':
    _args = parse_arguments({"--bootstrap_servers": "address of kafka broker",
                             "--schema_file": "xml file saving the table schema",
                             "--csv_folder": "folder saving all csv data",
                             "--recreate": "true if you want to recreate topic",
                             "--log_level": "log level"})

    if _args.bootstrap_servers and _args.schema_file and _args.csv_folder:
        _metadata = read_metadata(_args.schema_file)

        _source_and_meta = {}
        _nonexistent_folders = []
        for _, _table_meta in _metadata.items():
            _csv_source = f"{_args.csv_folder}/{_table_meta.csv_folder}"
            _archive = f"{_args.csv_folder}/{_table_meta.archive_folder}"
            if not os.path.isdir(_archive):
                os.mkdir(_archive)
            if os.path.isdir(_csv_source):
                _source_and_meta[_csv_source] = _table_meta
            else:
                _nonexistent_folders.append(_csv_source)

        if len(_source_and_meta) == 0:
            folders = ",".join(_nonexistent_folders)
            print(f"the csv folders: {folders} are nonexistent")
        else:
            _spark = SparkSession.builder.getOrCreate()

            # INFO level is too verbose
            _log_level = "WARN"
            if _args.log_level:
                _log_level = _args.log_level
            _spark.sparkContext.setLogLevel(_log_level)

            for _csv_source, _table_meta in _source_and_meta.items():
                _archive = f"{_args.csv_folder}/{_table_meta.archive_folder}"
                create_topic(_args.bootstrap_servers, _table_meta, _args.recreate)
                run_csv_stream(_spark, _csv_source, _archive, _table_meta, _args.bootstrap_servers)
            for s in _spark.streams.active:
                s.awaitTermination()
