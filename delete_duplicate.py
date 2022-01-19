from delta.tables import *

# -----------------<configs>-----------------#
pks = ['C0', 'C1']
order_by = 'APPEND_TIME'
delete_keys = pks + ['APPEND_TIME']
group_by = 'C1'
path = "/mnt/island/ikea/table/"
spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
# -----------------<main>-----------------#
df = spark.read.format("delta").load(path)
deltaTable = DeltaTable.forPath(spark, path)
for row in df.exceptAll(df.orderBy(order_by, ascending=False).drop_duplicates(pks)).collect():
    _conf = " AND ".join(["%s = '%s'" % (_column, row[_column]) for _column in delete_keys])
    if not group_by:
        deltaTable.delete(_conf)
    else:
        deltaTable.delete(f"{group_by} in ('{row[group_by]}') AND {_conf}")
