import argparse
from xml.etree import ElementTree

from confluent_kafka.admin import AdminClient
from pyspark.sql.types import StringType, TimestampType, DataType, NullType, BinaryType, BooleanType, \
    DateType, DecimalType, DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType, StructType


def parse_arguments(arg_description):
    parser = argparse.ArgumentParser()
    for name, desc in arg_description.items():
        parser.add_argument(name, help=desc)
    return parser.parse_args()


class TableMetadata:
    def __init__(self, table_name, csv_folder, topic, delta_folder, columns, pks, partition_by,
                 partitions, data_types, compact, order_by):
        self._table_name = table_name
        self._csv_folder = csv_folder
        self._topic = topic
        self._delta_folder = delta_folder
        self._columns = columns
        self._pks = pks
        self._partition_by = partition_by
        self._partitions = partitions
        self._data_types = data_types
        self._compact = compact
        self._order_by = order_by

    @property
    def compact(self): return self._compact

    @property
    def data_types(self): return self._data_types

    @property
    def partitions(self): return self._partitions

    # the column stored partition data
    @property
    def partition_column_name(self): return "pt_date"

    # the type of partition column
    @property
    def partition_column_type(self): return DateType()

    # this column offers the data for partition column
    @property
    def partition_by(self): return self._partition_by

    @property
    def pks(self): return self._pks

    @property
    def columns(self): return self._columns

    @property
    def table_name(self): return self._table_name

    @property
    def csv_folder(self): return self._csv_folder

    @property
    def topic(self): return self._topic

    @property
    def delta_folder(self): return self._delta_folder

    @property
    def order_by(self): return self._order_by

    def __str__(self):
        return f"name: {self._table_name} " \
               f"topic: {self._topic} " \
               f"csv folder: {self._csv_folder} " \
               f"delta folder: {self._delta_folder}"


def data_type(name):
    if name.strip().upper() == "DATA":
        return DataType()
    elif name.strip().upper() == "NULL":
        return NullType()
    elif name.strip().upper() == "STRING" or name.strip().upper() == "STR":
        return StringType()
    elif name.strip().upper() == "BINARY":
        return BinaryType()
    elif name.strip().upper() == "BOOLEAN":
        return BooleanType()
    elif name.strip().upper() == "DATE":
        return DateType()
    elif name.strip().upper() == "TIMESTAMP" or name.strip().upper() == "TIME":
        return TimestampType()
    elif name.strip().upper() == "DECIMAL":
        return DecimalType()
    elif name.strip().upper() == "DOUBLE":
        return DoubleType()
    elif name.strip().upper() == "FLOAT":
        return FloatType()
    elif name.strip().upper() == "BYTE":
        return ByteType()
    elif name.strip().upper() == "INTEGER" or name.strip().upper() == "INT":
        return IntegerType()
    elif name.strip().upper() == "LONG":
        return LongType()
    elif name.strip().upper() == "SHORT":
        return ShortType()
    else:
        raise ValueError(f"unsupported type: {name}")


def read_metadata(path):
    _schemas = {}
    for _child in ElementTree.parse(path).getroot():
        # for name
        _name = _child.get("name")
        if _name in _schemas:
            raise ValueError(f"duplicate name: {_name}")

        # for columns
        _columns = [_c.strip().lower() for _c in _child.find("columns").text.split(",")]

        # for primary keys
        _pks = [_c.strip().lower() for _c in _child.find("pks").text.split(",")]
        if len(_pks) == 0:
            raise ValueError("pks can't be empty")
        for _pk in _pks:
            if _pk not in _columns:
                raise ValueError(f"[{_name}]'pk: {_pk} is not existent in {_columns}")

        # for kafka partition
        _partitions = 10
        if _child.find("partitions") is not None and _child.find("partitions").text is not None:
            _partitions = int(_child.find("partitions").text.strip())

        # for data type
        if _child.find("types") is None or _child.find("types").text is None:
            raise ValueError(f"types is required")

        _data_types = {}
        for _i, _c in enumerate(_child.find("types").text.split(",")):
            if _i >= len(_columns):
                raise ValueError(f"length of types {_i} is not equal to columns ({len(_columns)})")
            _data_types[_columns[_i]] = data_type(_c)

        if len(_data_types) != 0 and len(_data_types) != len(_columns):
            raise ValueError(f"length of types {len(_data_types)} is not equal to columns ({len(_columns)})")

        # for delta table partition
        if _child.find("partitionBy") is None or _child.find("partitionBy").text is None:
            raise ValueError(f"partitionBy is required")
        _partition_by = _child.find("partitionBy").text.strip().lower()

        if _partition_by not in _columns:
            raise ValueError(f"partitionBy column: {_partition_by} is not in columns: {_columns}")

        if not isinstance(_data_types[_partition_by], TimestampType):
            raise ValueError(f"the type of {_partition_by} (partition column) should be Timestamp")

        # kafka compaction
        _compact = True
        if _child.find("compact") is not None and _child.find("compact").text is not None:
            _compact = _child.find("compact").text.lower() == "true"

        # for remove duplicate from csv files
        if _child.find("orderBy") is None or _child.find("orderBy").text is None:
            raise ValueError(f"orderBy is required")
        _order_by = [_c.strip().lower() for _c in _child.find("orderBy").text.split(",")]

        # build metadata
        _schemas[_name] = TableMetadata(_name,
                                        _child.find("csvFolder").text,
                                        _child.find("topic").text,
                                        _child.find("deltaFolder").text,
                                        _columns,
                                        _pks,
                                        _partition_by,
                                        _partitions,
                                        _data_types,
                                        _compact,
                                        _order_by)
    return _schemas


def topics(bootstrap_servers):
    _admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    return _admin.list_topics().topics.keys()
