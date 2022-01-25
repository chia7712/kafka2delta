import os
import tempfile
import time

from confluent_kafka.cimpl import NewTopic
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, array, array_join, lit, hash, abs
from pyspark.sql.types import StructType

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
    _schema = StructType()
    for _column in columns:
        _schema.add(_column, StringType(), nullable=True)
    return _schema


def run_csv_stream(spark, csv_source, metadata, brokers):
    # we don't care for recovery currently, so using a random folder is fine
    _checkpoint_folder = tempfile.mkdtemp()
    _cols = [col(_c) for _c in metadata.columns]
    _pks = [col(_c) for _c in metadata.pks]

    _df = spark.readStream \
        .schema(all_string_types(metadata.columns)) \
        .option("recursiveFileLookup", "true") \
        .csv(csv_source) \
        .withColumn("key", array(_pks)) \
        .withColumn("key", array_join(col("key"), ",")) \
        .withColumn("value", array(_cols)) \
        .withColumn("value", array_join(col("value"), ",", null_replacement=""))

    if metadata.group_by is not None:
        _df = _df.withColumn("partition", abs(hash(col(metadata.group_by))) % lit(metadata.partitions)) \
            .selectExpr("CAST(key as STRING)", "CAST(value AS STRING)", "partition")
    else:
        _df = _df.selectExpr("CAST(key as STRING)", "CAST(value AS STRING)")

    _df.writeStream \
        .format("kafka") \
        .option("checkpointLocation", _checkpoint_folder) \
        .option("kafka.bootstrap.servers", brokers) \
        .option("kafka.compression.type", "zstd") \
        .option("topic", metadata.topic) \
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
                create_topic(_args.bootstrap_servers, _table_meta, _args.recreate)
                run_csv_stream(_spark, _csv_source, _table_meta, _args.bootstrap_servers)
            for s in _spark.streams.active:
                s.awaitTermination()
