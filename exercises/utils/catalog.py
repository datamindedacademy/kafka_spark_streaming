"""
A simple implementation of a data catalog, used to illustrate how it abstracts
details like location, format, and format-specific options.
"""

from pathlib import Path
from typing import Mapping, NamedTuple

import findspark

findspark.init()
from pyspark.sql import DataFrame, SparkSession

RESOURCES_DIR = Path(__file__).parents[1] / "resources"
TARGET_DIR = Path(__file__).parents[1] / "target"
TARGET_DIR.mkdir(exist_ok=True)


class DataLink(NamedTuple):
    format: str
    path: Path  # for our use cases, a path to a local file system. You will want a string when using AWS S3 or Azure Datalake Storage.
    options: Mapping[str, str]


csv_options = {"header": "true", "inferschema": "false"}
json_options = {
    "maxFilesPerTrigger": "1",
    "cleanSource": "delete",
    "header": "true",
    "inferschema": "true",
}
catalog = {
    "sample": DataLink("csv", RESOURCES_DIR / "sample.csv", csv_options),
    "invoices_json": DataLink("csv", RESOURCES_DIR / "invoices-json", json_options),
    "invoices_parquet": DataLink(
        "csv", RESOURCES_DIR / "invoices-parquet", csv_options
    ),
}


def load_frame_from_catalog(
    spark: SparkSession,
    format: str,
    dataset_name: str,
    catalog: Mapping[str, DataLink] = catalog,
) -> DataFrame:
    """Given the name of a dataset, load that dataset (with the options and
    whereabouts specified in the catalog) as a Spark DataFrame.
    """
    # note: the default argument in the function parameters is mutable, which is
    # not recommended practice. In a production setting, you would have the
    # catalog become a class (instead of a dictionary) and this function
    # become one of its methods.
    link = catalog[dataset_name]
    return spark.readStream.format(format).options(**link.options).load(str(link.path))


if __name__ == "__main__":
    # to illustrate how this can be used:
    spark = (
        SparkSession.builder.appName("Test")
        .master("local[3]")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )
    frame = load_frame_from_catalog(
        spark=spark, format="json", dataset_name="invoices_json"
    )
    frame.printSchema()
