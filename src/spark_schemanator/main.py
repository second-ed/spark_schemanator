import decimal
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType, BinaryType, BooleanType, DateType, TimestampType,
    DoubleType, FloatType, ByteType, ShortType, IntegerType, LongType, DecimalType,
    ArrayType, MapType, StructType, StructField
)


FAKE = Faker()


def _get_decimal(inp_decimal: str):
    pres_scale = re.search(r"\((.*?)\)", inp_decimal)
    precision, scale = map(int, pres_scale.group(1).split(","))
    n_digits = np.random.randint(low=1,high=precision)
    digits = tuple(np.random.randint(low=0, high=10, size=n_digits).tolist())
    sign = int(np.random.choice([0, 1]))
    return decimal.Decimal(decimal.DecimalTuple(exponent=-scale, digits=digits, sign=sign))


def get_data_type(data_type):
    discrete_type_ranges = {
        "boolean": bool(np.random.choice([True, False])),
        "byte": np.random.randint(low=-128, high=127),
        "short": np.random.randint(low=-32768, high=32767),
        "integer": np.random.randint(low=-2147483648, high=2147483647),
        "long": np.random.randint(low=-9223372036854775808, high=9223372036854775807),
        "float": float(np.random.uniform(low=-128, high=127)),
        "double": float(np.random.uniform(low=-128, high=127)),
        "string": FAKE.word(),
    }

    if isinstance(data_type, str):
        if data_type in discrete_type_ranges:
            return discrete_type_ranges.get(data_type)
        if "decimal" in data_type:
            return _get_decimal(data_type)

    if isinstance(data_type, dict):
        type_name = data_type["type"]
        
        if type_name == "struct":
            return [get_data_type(t.get("type")) for t in data_type["fields"]]

        if type_name == "array":
            element_type = data_type["elementType"]
            if isinstance(element_type, dict) and element_type["type"] == "struct":
                return [[get_data_type(t) for t in element_type["fields"]] for _ in range(3)]
            else:
                return [get_data_type(element_type) for _ in range(3)]

        if type_name == "map":
            key_type = data_type["keyType"]
            value_type = data_type["valueType"]
            return {
                get_data_type(key_type): get_data_type(value_type)
                for _ in range(3)
            }
        
        return get_data_type(type_name)


def create_data(spark: SparkSession, schema: StructType, n: int = 10):
    rows = []

    for _ in range(n):
        row = []
        for field in schema.fields:
            json_val = field.jsonValue()
            row.append(get_data_type(json_val.get("type")) )
        rows.append(row)
    return spark.createDataFrame(data=rows, schema=schema)
