import time

from delta.tables import *
from utils import *


class Result:
    def __init__(self, time_to_delete, deleted_rows):
        self._time_to_delete = time_to_delete
        self._deleted_rows = deleted_rows

    @property
    def time_to_delete(self):
        return self._time_to_delete

    @property
    def deleted_rows(self):
        return self._deleted_rows


# -----------------<constants>-----------------#
order_by = 'APPEND_TIME'

# batch to delete the records in the same group. None means all deletes of same group are executed at once.
max_batch = 500


# -----------------<helpers>-----------------#

def create_spark_session():
    return SparkSession.builder \
        .master("spark://192.168.50.16:10134") \
        .config("spark.driver.memory", "4G") \
        .config("spark.executor.memory", "6G") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-azure:3.2.2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.azure.account.auth.type.islandadls.dfs.core.windows.net", "SharedKey") \
        .config("spark.hadoop.fs.azure.account.key.islandadls.dfs.core.windows.net",
                "LgkUTkCGZGzWrD2G/z0JigFf9Au6UiU5ByBMS8CEKAaYDJnVG5JjrLXqi7mZnykD4gA93Gz6lI4jLiCB3BAwuA==") \
        .getOrCreate()


def cond(row):
    return " AND ".join(["%s = '%s'" % (_column, row[_column]) for _column in row.asDict().keys()])


def delete(spark, delta_table, path, group, metadata):
    _final_cond = None
    _batched = 0
    _deleted_rows = 0
    _time_to_delete = 0
    _all_group = spark.read.format("delta").load(path).filter(f"{metadata.group_by} = '{group}'").cache()
    _duplicate_group = _all_group.exceptAll(_all_group.orderBy(order_by, ascending=False)
                                            .drop_duplicates(_metadata.pks))

    def do_delete():
        if _final_cond is not None:
            _s_delete = time.time()
            delta_table.delete(f"{metadata.group_by} in ('{group}') AND ({_final_cond})")
            return time.time() - _s_delete
        else:
            return 0

    for _r in _duplicate_group.toLocalIterator(prefetchPartitions=True):
        if _batched >= max_batch:
            _time_to_delete = _time_to_delete + do_delete()
            _batched = 0
            _final_cond = None

        if _final_cond is None:
            _final_cond = cond(_r)
        else:
            _final_cond = f"({_final_cond}) OR ({cond(_r)})"
        _batched = _batched + 1
        _deleted_rows = _deleted_rows + 1
    _time_to_delete = _time_to_delete + do_delete()
    _all_group.unpersist()
    return Result(_time_to_delete, _deleted_rows)


def requireSingleMetadata(file):
    _m = read_metadata(file)
    if len(_m) != 1:
        raise ValueError(f"the size of {file} should be 1")
    return list(_m.values())[0]


if __name__ == '__main__':
    args = parse_arguments({"--input": "root of delta table",
                            "--all": "the number of all subsets",
                            "--index": "the index of this subset",
                            "--schema_file": "the metadata file"})
    if args.input and args.schema_file:
        _start = time.time()
        _metadata = requireSingleMetadata(args.schema_file)
        _spark = SparkSession.builder.getOrCreate()
        _spark.sparkContext.setLogLevel("WARN")
        _path = f"{args.input}/{_metadata.delta_folder}"
        _all = _spark.read.format("delta").load(_path)
        _delta_table = DeltaTable.forPath(_spark, _path)
        _duplicate = _all.exceptAll(_all.orderBy(order_by, ascending=False).drop_duplicates(_metadata.pks)).cache()
        if _metadata.group_by is None or not args.all or not args.index:
            print("Start to delete duplicate")
            for _row in _duplicate.toLocalIterator():
                _delta_table.delete(cond(_row))
        else:
            print(f"Start to delete duplicate by group: {_metadata.group_by} - ({args.index}/{args.all})")
            _groups = [_r[_metadata.group_by] for _r in _duplicate.select(_metadata.group_by).distinct().collect()]
            _g_len = len(_groups)
            _index = 0
            for _g in _groups:
                if hash(_g) % int(args.all) == int(args.index):
                    _s = time.time()
                    _r = delete(_spark, _delta_table, _path, _g, _metadata)
                    _e = time.time() - _s
                    print(f"({_index}/{_g_len}) de-duplicate group: {_g} elapsed: {_e} "
                          f"time_to_delete {_r.time_to_delete} deleted_rows: {_r.deleted_rows}")
                else:
                    print(f"({_index}/{_g_len}) skip group: {_g}")
                _index = _index + 1

        _end_delete = time.time()
        print(f"time to delete: {_end_delete - _start}")
