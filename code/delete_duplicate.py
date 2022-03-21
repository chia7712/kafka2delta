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

def cond(row):
    return " AND ".join(["%s = '%s'" % (_column, row[_column]) for _column in row.asDict().keys()])


def delete(spark, delta_table, group, metadata, duplicate_csv_path):
    _final_cond = None
    _batched = 0
    _deleted_rows = 0
    _time_to_delete = 0
    _group_duplicate = spark.read \
        .schema(struct_type(metadata)) \
        .option("recursiveFileLookup", "true") \
        .csv(duplicate_csv_path)

    def do_delete():
        if _final_cond is not None:
            _s_delete = time.time()
            delta_table.delete(f"{metadata.partition_by} in ('{group}') AND ({_final_cond})")
            return time.time() - _s_delete
        else:
            return 0

    for _r in _group_duplicate.toLocalIterator(prefetchPartitions=True):
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
    return Result(_time_to_delete, _deleted_rows)


def requireSingleMetadata(file):
    _m = read_metadata(file)
    if len(_m) != 1:
        raise ValueError(f"the size of {file} should be 1")
    return list(_m.values())[0]


def save_duplicate(duplicate, groups):
    _folders = {}
    _i = 1
    for _g in groups:
        if hash(_g) % int(args.all) == int(args.index):
            _s = time.time()
            _folder = f"/tmp/{_metadata.partition_by}={_g}"
            duplicate.filter(f"{_metadata.partition_by} = '{_g}'") \
                .write \
                .format("csv") \
                .save(_folder)
            _folders[_g] = _folder
            print(f"({_i}/{len(groups)}) save duplicates of {_g} to {_folder} elapsed: {time.time() - _s}")
        else:
            print(f"({_i}/{len(groups)}) {_g} is skipped")
        _i = _i + 1
    return _folders


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
        if _metadata.partition_by is None or not args.all or not args.index:
            print("Start to delete duplicate")
            for _row in _duplicate.toLocalIterator():
                _delta_table.delete(cond(_row))
        else:
            print(f"Start to delete duplicate by group: {_metadata.partition_by} - ({args.index}/{args.all})")
            _groups = [_r[_metadata.partition_by] for _r in _duplicate.select(_metadata.partition_by).distinct().collect()]
            _csv_folders = save_duplicate(_duplicate, _groups)

            _index = 1
            for _group, _csv_folder in _csv_folders.items():
                _s_dedup = time.time()
                _r = delete(_spark, _delta_table, _group, _metadata, _csv_folder)
                print(f"({_index}/{len(_csv_folders)}) de-duplicate group: {_group} elapsed: {time.time() - _s_dedup} "
                      f"time_to_delete {_r.time_to_delete} deleted_rows: {_r.deleted_rows}")
                _index = _index + 1

        _end_delete = time.time()
        print(f"time to cleanup duplicates: {_end_delete - _start}")
