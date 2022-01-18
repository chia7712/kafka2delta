from pyspark.sql import SparkSession

from utils import *

args = parse_arguments({"--input": "path to read delta table",
                        "--output": "output path to save all delta tables"})

if args.input and args.output:
    SparkSession.builder \
        .getOrCreate() \
        .read \
        .format("delta") \
        .load(args.input) \
        .write \
        .format("csv") \
        .save(args.output)
