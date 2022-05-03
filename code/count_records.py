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
                             "--show_records": "the number of records to show",
                             "--loop": "loop to show",
                             "--interval": "time to show next loop"})

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

    show_records = 100
    if _args.show_records:
        show_records = int(_args.show_records)

    if _args.topic and _args.bootstrap_servers:
        _loop = 1
        if _args.loop:
            _loop = int(_args.loop)
        _interval = 1
        if _args.interval:
            _interval = int(_args.interval)
        _spark = SparkSession.builder.getOrCreate()
        _spark.sparkContext.setLogLevel("ERROR")
        _max_offset = -1
        for i in range(0, _loop):
            _sorted = _spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", _args.bootstrap_servers) \
                .option("subscribe", _args.topic) \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(key as STRING)", "CAST(value AS STRING)", "timestamp",
                            "offset", "topic", "partition") \
                .orderBy("offset", ascending=False)
            if len(_sorted.head()) > 0:
                _max = _sorted.head()["offset"]
                # show data only if there are new data
                if _max > _max_offset:
                    _max_offset = _max
                    _sorted.show(show_records, truncate=False)
                    print(f"there are {_sorted.count()} records in {_args.topic}")
            time.sleep(_interval)
