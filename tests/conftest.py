import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def get_spark_session():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("Pytest Spark Session")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()


@pytest.fixture
def get_valid_schema():
    return StructType(
        [
            StructField("type_string", StringType()),
            StructField("type_boolean", BooleanType()),
            StructField("type_byte", ByteType()),
            StructField("type_short", ShortType()),
            StructField("type_integer", IntegerType()),
            StructField("type_long", LongType()),
            StructField("type_float", FloatType()),
            StructField("type_double", DoubleType()),
            StructField("type_decimal", DecimalType(8, 3)),
            StructField(
                "type_struct",
                StructType(
                    [
                        StructField("member_a", StringType()),
                        StructField("member_b", IntegerType()),
                    ]
                ),
            ),
            StructField("array", ArrayType(BooleanType())),
            StructField(
                "type_array_struct",
                ArrayType(
                    StructType(
                        [
                            StructField("member_a", StringType()),
                            StructField("member_b", IntegerType()),
                            StructField("extras", ArrayType(ByteType())),
                        ]
                    )
                ),
            ),
            StructField("type_map", MapType(ByteType(), StringType())),
        ]
    )
