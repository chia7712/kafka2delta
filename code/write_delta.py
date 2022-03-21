from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, monotonically_increasing_id, col

from utils import *


def condition_of_merge(source, target, pks):
    return " AND ".join(["%s.%s = %s.%s" % (source, _column, target, _column) for _column in pks])


def create_delta_table(spark, delta_path, metadata):
    _table = DeltaTable.createIfNotExists(spark).location(delta_path)
    for _column in metadata.columns:
        if _column in metadata.data_types:
            _table.addColumn(_column, metadata.data_types[_column], nullable=True)
        else:
            # if the type of column is undefined, we assign it to string type.
            _table.addColumn(_column, StringType(), nullable=True)
    # add append timestamp
    _table.addColumn("APPEND_TIME", TimestampType(), nullable=True)
    if metadata.partition_by is not None:
        # Delta gives this ugly API in 1.0.0 ...
        _table.partitionedBy(metadata.partition_by, metadata.partition_by)
    _table.execute()


def merge(spark, metadata, delta_path, data_frame, use_merge):
    if use_merge:
        _cond = condition_of_merge("previous", "updates", metadata.pks)
        if metadata.partition_by is not None:
            _values = ",".join([f"'{row[metadata.partition_by]}'" for row in data_frame
                               .select(metadata.partition_by).distinct().collect()])
            _cond = f"previous.{metadata.partition_by} IN ({_values}) AND {_cond}"

        DeltaTable.forPath(spark, delta_path) \
            .alias("previous") \
            .merge(data_frame
                   .drop("partition")
                   .withColumn("increasing_id", monotonically_increasing_id())
                   .orderBy("increasing_id", ascending=False)
                   .drop("increasing_id")
                   .dropDuplicates(metadata.pks)
                   .alias("updates"), _cond) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        if metadata.partition_by is not None:
            data_frame.write.format("delta").mode("append").partitionBy(metadata.partition_by).save(delta_path)
        else:
            data_frame.write.format("delta").mode("append").save(delta_path)


def run_topic_stream(spark, metadata, delta_path, bootstrap_servers, use_merge):
    def create_stream(subscribe_key, subscribe_value):
        spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("startingOffsets", "earliest") \
            .option("maxOffsetsPerTrigger", "5000000") \
            .option(subscribe_key, subscribe_value) \
            .load() \
            .selectExpr("CAST(value AS STRING)", "timestamp") \
            .withColumn("APPEND_TIME", col("timestamp")) \
            .withColumn("value", from_csv("value", struct_type(metadata).simpleString())) \
            .select("value.*", "APPEND_TIME") \
            .writeStream \
            .trigger(processingTime='1 seconds') \
            .foreachBatch(lambda df, _: merge(spark, metadata, delta_path, df, use_merge)) \
            .start()

    if metadata.partition_by is None:
        create_stream("subscribe", metadata.topic)

    else:
        for i in range(0, metadata.partitions):
            create_stream("assign", "{\"" + metadata.topic + "\":[" + str(i) + "]}")


if __name__ == '__main__':

    _args = parse_arguments({"--bootstrap_servers": "kafka brokers",
                             "--schema_file": "xml file saving the table schema",
                             "--output": "output path to save all delta tables",
                             "--merge": "use MERGE to replace append",
                             "--log_level": "log level"})

    if _args.bootstrap_servers and _args.schema_file and _args.output:
        _spark = SparkSession.builder.getOrCreate()
        _metadata = read_metadata(_args.schema_file)
        _existent_topics = topics(_args.bootstrap_servers)

        # check delta mode
        _use_merge = False
        if _args.merge and _args.merge.lower() == "true":
            _use_merge = True

        # INFO level is too verbose
        _log_level = "WARN"
        if _args.log_level:
            _log_level = _args.log_level
        _spark.sparkContext.setLogLevel(_log_level)

        for _, _table_meta in _metadata.items():
            if _table_meta.topic in _existent_topics:
                _delta_path = f"{_args.output}/{_table_meta.delta_folder}"
                create_delta_table(_spark, _delta_path, _table_meta)
                run_topic_stream(_spark, _table_meta, _delta_path, _args.bootstrap_servers, _use_merge)
        for s in _spark.streams.active:
            s.awaitTermination()
