from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, monotonically_increasing_id, col

from utils import *


def create_delta_table(spark, delta_path, metadata):
    _table = DeltaTable.createIfNotExists(spark).location(delta_path)
    for _column in metadata.columns:
        if _column in metadata.data_types:
            _table.addColumn(_column, metadata.data_types[_column], nullable=True)
        else:
            # if the type of column is undefined, we assign it to string type.
            _table.addColumn(_column, StringType(), nullable=True)
    # Delta gives this ugly API in 1.0.0 ...
    _table.partitionedBy(metadata.partition_by, metadata.partition_by).execute()


def merge(spark, metadata, delta_path, data_frame):
    _partition_values = ",".join([f"'{row[metadata.partition_by]}'" for row in data_frame.select(metadata.partition_by)
                                 .distinct().collect()])
    _partition_cond = f"previous.{metadata.partition_by} IN ({_partition_values})"
    _merge_cond = " AND ".join(["%s.%s = %s.%s" % ("previous", _column, "updates", _column) for _column in metadata.pks])
    _cond = f"{_partition_cond} AND {_merge_cond}"

    DeltaTable.forPath(spark, delta_path) \
        .alias("previous") \
        .merge(data_frame
               .drop("partition")
               .orderBy(metadata.order_by, ascending=False)
               .dropDuplicates(metadata.pks)
               .alias("updates"), _cond) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()


def run_topic_stream(spark, metadata, delta_path, bootstrap_servers):
    def create_stream(subscribe_key, subscribe_value):
        spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", "5000000") \
            .option(subscribe_key, subscribe_value) \
            .load() \
            .selectExpr("CAST(value AS STRING)", "timestamp") \
            .withColumn("value", from_csv("value", struct_type(metadata).simpleString())) \
            .select("value.*") \
            .writeStream \
            .trigger(processingTime='1 seconds') \
            .foreachBatch(lambda df, _: merge(spark, metadata, delta_path, df)) \
            .start()

    for i in range(0, metadata.partitions):
        create_stream("assign", "{\"" + metadata.topic + "\":[" + str(i) + "]}")


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
