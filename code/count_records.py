import time

from pyspark.sql import SparkSession

from utils import *


def until(expected, actual):
    _start = time.time()
    while True:
        _actual = actual()
        _elapsed = (time.time() - _start)
        if _actual == expected:
            print(f"elapsed: {_elapsed} for {_actual} records")
            break
        elif _actual > expected:
            raise ValueError(f"actual: {_actual} is bigger than expected: {expected}")
        else:
            print(f"elapsed: {_elapsed} actual: {_actual} expected: {expected}")
            time.sleep(5)


if __name__ == '__main__':

    _args = parse_arguments({"--topic": "the topic to trace",
                             "--bootstrap_servers": "broker address",
                             "--path": "the root folder of all delta tables",
                             "--records": "the expected number of records",
                             "--display": "display all data in topic"})

    if _args.path:
        _df = SparkSession.builder \
            .appName('read_delta') \
            .getOrCreate() \
            .read \
            .format("delta") \
            .load(_args.path)

        if _args.records:
            until(int(_args.records), lambda: _df.count())
        else:
            print(f"there are {_df.count()} records in {_args.path}")

    if _args.topic and _args.bootstrap_servers:
        _spark = SparkSession.builder.getOrCreate()
        _spark.sparkContext.setLogLevel("WARN")
        _df = _spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", _args.bootstrap_servers) \
            .option("subscribe", _args.topic) \
            .option("startingOffsets", "earliest") \
            .load()

        if _args.display:
            _df.selectExpr("CAST(key as STRING)", "CAST(value AS STRING)", "timestamp").show(truncate=False)

        if _args.records:
            until(int(_args.records), lambda: _df.count())
        else:
            print(f"there are {_df.count()} records in {_args.topic}")
