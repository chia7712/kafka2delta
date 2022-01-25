from pyspark.sql import SparkSession

from utils import *

if __name__ == '__main__':

    _args = parse_arguments({"--input": "path to read delta table",
                            "--output": "output path to save all delta tables"})

    if _args.input and _args.output:
        SparkSession.builder \
            .getOrCreate() \
            .read \
            .format("delta") \
            .load(_args.input) \
            .write \
            .format("csv") \
            .save(_args.output)
