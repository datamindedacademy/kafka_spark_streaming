import findspark
from pyspark.sql.types import LongType, StringType, StructField, StructType

findspark.init()
from pyspark.sql import SparkSession

from exercises.a_spark_streaming_socket_source.word_count import transform

from .comparers import assert_frames_functionally_equivalent

spark = SparkSession.builder.getOrCreate()


def test_word_count():
    input_str = "I love Spark , Spark Structure Streaming. I am learning Spark !"
    df = spark.createDataFrame(
        [[input_str]], schema=StructType([StructField("value", StringType(), True)])
    )

    res_df = transform(input_df=df)

    expected_df = spark.createDataFrame(
        [
            ("!", 1),
            ("love", 1),
            ("learning", 1),
            ("I", 2),
            (",", 1),
            ("Spark", 3),
            ("am", 1),
            ("Structure", 1),
            ("Streaming.", 1),
        ],
        schema=StructType(
            [
                StructField("word", StringType(), True),
                StructField("count", LongType(), False),
            ]
        ),
    )

    assert_frames_functionally_equivalent(res_df, expected_df)
