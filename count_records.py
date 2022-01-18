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


args = parse_arguments({"--topic": "the topic to trace",
                        "--bootstrap_servers": "broker address",
                        "--path": "the root folder of all delta tables",
                        "--records": "the expected number of records"})

if args.path:
    _df = SparkSession.builder \
        .appName('read_delta') \
        .getOrCreate() \
        .read \
        .format("delta") \
        .load(args.path)

    if args.records:
        until(int(args.records), lambda: _df.count())
    else:
        print(f"there are {_df.count()} records in {args.path}")

if args.topic and args.bootstrap_servers:
    _spark_session = SparkSession.builder.getOrCreate()
    _spark_session.sparkContext.setLogLevel("WARN")
    _df = _spark_session\
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", args.bootstrap_servers) \
        .option("subscribe", args.topic) \
        .option("startingOffsets", "earliest") \
        .load()

    if args.records:
        until(int(args.records), lambda: _df.count())
    else:
        print(f"there are {_df.count()} records in {args.topic}")
