import argparse
from operator import add

from pyspark.broadcast import Broadcast
from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from utopia.process_event.variables import Env


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process detection events")
    parser.add_argument(
        "--file1",
        required=True,
        help=("Input path for detection events in parquet format"),
    )
    parser.add_argument(
        "--file2",
        required=True,
        help=("Input path for geographical location dimension in parquet format"),
    )
    parser.add_argument(
        "--output_path", required=True, help=("Output path for top x result in parquet")
    )
    parser.add_argument(
        "--top_x",
        required=True,
        type=int,
        help="Number of top (item_name, location) pairs to return",
    )
    parser.add_argument(
        "--env",
        choices=[Env.LOCAL.value, Env.PROD.value],
        default=Env.LOCAL.value,
        help="Env affects the settings for pyspark config",
    )
    return parser.parse_args()


def build_spark_session(env: str) -> SparkSession:
    if env == Env.LOCAL.value:
        return (
            SparkSession.builder.appName("process_event locally")
            .master("local[*]")
            .getOrCreate()
        )
    elif env == Env.PROD.value:
        return (
            SparkSession.builder.appName("process_event in production")
            .config("spark.sql.adaptive.enabled", True)
            .config("spark.sql.adaptive.skewJoin.enabled", True)
            .getOrCreate()
        )


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def count_unique_detections(rdd: RDD) -> RDD:
    """
    Args:
         rdd: RDD[Row] with fields  geographical_location_oid, video_camera_oid
         , detection_oid, item_name, timestamp_detected.

    Returns:
         RDD of ((item_name, geographical_location_oid), int representing count
         ) — deduplicated
         by detection_oid before counting.
    """
    return (
        rdd.map(lambda row: (row.detection_oid, row))
        .reduceByKey(lambda a, b: a)
        .map(lambda kv: ((kv[1].item_name, kv[1].geographical_location_oid), 1))
        .reduceByKey(add)
    )


def get_top_x_ranked(counted_rdd: RDD, top_x: int) -> RDD:
    """
    Args:
        counted_rdd: RDD of ((item_name, geographical_location_oid), count)
        top_x: number of top-ranked items to return (by descending count)
    Returns:
        RDD of (geographical_location_oid, (item_name, rank)) where rank is 1-indexed
    """
    return (
        counted_rdd.sortBy(lambda kv: kv[1], ascending=False)
        .zipWithIndex()
        .filter(lambda kv_idx: kv_idx[1] < top_x)
        .map(lambda kv_idx: (kv_idx[0][0][1], (kv_idx[0][0][0], kv_idx[1] + 1)))
    )


def build_location_broadcast(rdd: RDD, sc: SparkContext) -> Broadcast:
    """         
    Args:
        rdd: RDD[Row] with fields geographical_location_oid, geographical_location.                       
        sc: SparkContext used to broadcast the collected dictionary.

    Returns:
        Broadcast variable wrapping a dict of {geographical_location_oid:geographical_location}.
  """
    location_dict = rdd.map(
        lambda row: (row.geographical_location_oid, row.geographical_location)
    ).collectAsMap()
    return sc.broadcast(location_dict)


def enrich_with_location(top_x_rdd: RDD, bcast: Broadcast) -> RDD:
    return top_x_rdd.map(
        lambda kv: Row(
            geographical_location=bcast.value.get(kv[0]),
            item_rank=kv[1][1],
            item_name=kv[1][0],
        )
    )


OUTPUT_SCHEMA = StructType(
    [
        StructField("geographical_location", StringType(), True),
        StructField("item_rank", IntegerType(), False),
        StructField("item_name", StringType(), True),
    ]
)


def write_output(rdd: RDD, spark: SparkSession, output_path: str) -> None:
    spark.createDataFrame(rdd, OUTPUT_SCHEMA).write.mode("overwrite").parquet(
        output_path
    )


def main() -> None:
    args = parse_args()
    try:
        spark = build_spark_session(args.env)

        file1_rdd = read_parquet(spark, args.file1).rdd
        file2_rdd = read_parquet(spark, args.file2).rdd

        counted_rdd = count_unique_detections(file1_rdd)
        top_x_rdd = get_top_x_ranked(counted_rdd, args.top_x)
        location_bcast = build_location_broadcast(file2_rdd, spark.sparkContext)
        enriched_rdd = enrich_with_location(top_x_rdd, location_bcast)

        write_output(enriched_rdd, spark, args.output_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
