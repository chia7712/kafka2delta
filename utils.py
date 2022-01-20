import argparse
from xml.etree import ElementTree

from confluent_kafka.admin import AdminClient
from pyspark.sql.types import StringType, TimestampType, DataType, NullType, BinaryType, BooleanType, \
    DateType, DecimalType, DoubleType, FloatType, ByteType, IntegerType, LongType, ShortType


def parse_arguments(arg_description):
    parser = argparse.ArgumentParser()
    for name, desc in arg_description.items():
        parser.add_argument(name, help=desc)
    return parser.parse_args()


class TableMetadata:
    def __init__(self, table_name, csv_folder, topic, delta_folder, columns, pks, group_by,
                 partitions, data_types, compact):
        self._table_name = table_name
        self._csv_folder = csv_folder
        self._topic = topic
        self._delta_folder = delta_folder
        self._columns = columns
        self._pks = pks
        self._group_by = group_by
        self._partitions = partitions
        self._data_types = data_types
        self._compact = compact

    @property
    def compact(self): return self._compact

    @property
    def data_types(self): return self._data_types

    @property
    def partitions(self): return self._partitions

    @property
    def group_by(self): return self._group_by

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
        _columns = [_c.strip().upper() for _c in _child.find("columns").text.split(",")]

        # for primary keys
        _pks = [_c.strip().upper() for _c in _child.find("pks").text.split(",")]
        for _pk in _pks:
            if _pk not in _columns:
                raise ValueError(f"[{_name}]'pk: {_pk} is not existent in {_columns}")

        # for delta table partition
        _group_by = None
        if _child.find("groupBy") is not None and _child.find("groupBy").text is not None:
            _group_by = _child.find("groupBy").text.strip().upper()
        if _group_by is not None and _group_by not in _pks:
            raise ValueError(f"[{_name}]'s partition: {_group_by} is not existent in {_pks}")

        # for kafka partition
        _partitions = 10
        if _child.find("partitions") is not None and _child.find("partitions").text is not None:
            _partitions = int(_child.find("partitions").text.strip())

        # for data type
        _data_types = {}
        if _child.find("types") is not None and _child.find("types").text is not None:
            for _i, _c in enumerate(_child.find("types").text.split(",")):
                if _i >= len(_columns):
                    raise ValueError(f"length of types {_i} is not equal to columns ({len(_columns)})")
                _data_types[_columns[_i]] = data_type(_c)
        else:
            for _i, _c in enumerate(_columns):
                _data_types[_columns[_i]] = StringType()

        if len(_data_types) != 0 and len(_data_types) != len(_columns):
            raise ValueError(f"length of types {len(_data_types)} is not equal to columns ({len(_columns)})")

        _compact = True
        if _child.find("compact") is not None and _child.find("compact").text is not None:
            _compact = _child.find("compact").text.lower() == "true"

        # build metadata
        _schemas[_name] = TableMetadata(_name,
                                        _child.find("csvFolder").text,
                                        _child.find("topic").text,
                                        _child.find("deltaFolder").text,
                                        _columns,
                                        _pks,
                                        _group_by,
                                        _partitions,
                                        _data_types,
                                        _compact)
    return _schemas


def topics(bootstrap_servers):
    _admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    return _admin.list_topics().topics.keys()

