import time

from delta.tables import *

# -----------------<constants>-----------------#
pks = ['C0', 'C1']
order_by = 'APPEND_TIME'
delete_keys = pks + ['APPEND_TIME']
group_by = 'C1'
path = "/tmp/chia/table"
spark = SparkSession.builder \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
deltaTable = DeltaTable.forPath(spark, path)


# -----------------<helpers>-----------------#
def base_df():
    return spark.read.format("delta").load(path)


def groups():
    _all = base_df()
    _duplicate = _all.exceptAll(_all.orderBy(order_by, ascending=False).drop_duplicates(pks)).cache()
    return [_r[group_by] for _r in _duplicate.select(group_by).distinct().collect()] if group_by else []


# -----------------<main>-----------------#
groups = groups()
batch = 50

start = time.time()


def cond(row):
    return " AND ".join(["%s = '%s'" % (_column, row[_column]) for _column in delete_keys])


def delete(group):
    _final_cond = ""
    count = 0

    def do_delete():
        if _final_cond:
            deltaTable.delete(f"{group_by} in ('{group}') AND ({_final_cond})")

    for _r in base_df().filter(f"{group_by} = '{group}'").select(delete_keys).toLocalIterator(prefetchPartitions=True):
        if count >= batch:
            do_delete()
            count = 0
            _final_cond = ""

        if not _final_cond:
            _final_cond = cond(_r)
        else:
            _final_cond = f"({_final_cond}) OR ({cond(_r)})"
        count = count + 1
    do_delete()


if not group_by:
    for _row in base_df().toLocalIterator():
        deltaTable.delete(cond(_row))
else:
    for _g in groups:
        delete(_g)
end = time.time()
print(end - start)
