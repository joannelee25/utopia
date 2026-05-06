import argparse
from operator import add

from pyspark import SparkContext
from pyspark.broadcast import Broadcast
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from utopia.process_event.variables import DEFAULT_CONFIG, Env, PipelineConfig


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


def count_unique_detections(rdd: RDD, config: PipelineConfig = DEFAULT_CONFIG) -> RDD:
    """
    Args:
         rdd: RDD[Row] with fields geographical_location_oid, video_camera_oid
         , detection_oid, item_name, timestamp_detected.

    Returns:
         RDD of ((item_name, geographical_location_oid), int representing count
         ) — deduplicated
         by detection_oid before counting.
    """
    dedup_key = config.dedup_key
    item_key = config.item_key
    location_oid_key = config.location_oid_key
    return (
        rdd.map(lambda row: (getattr(row, dedup_key), row))
        .reduceByKey(lambda a, b: a)
        .map(
            lambda kv: ((getattr(kv[1], item_key), getattr(kv[1], location_oid_key)), 1)
        )
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


def build_location_broadcast(
    rdd: RDD, sc: SparkContext, config: PipelineConfig = DEFAULT_CONFIG
) -> Broadcast:
    """
    Args:
        rdd: RDD[Row] with fields geographical_location_oid, geographical_location
        sc: SparkContext used to broadcast the collected dictionary

    Returns:
        Broadcast variable wrapping a dict of
        {geographical_location_oid: geographical_location}
    """
    location_oid_key = config.location_dim_oid_key
    location_name_key = config.location_name_key
    location_dict = rdd.map(
        lambda row: (getattr(row, location_oid_key), getattr(row, location_name_key))
    ).collectAsMap()
    return sc.broadcast(location_dict)


def enrich_with_location(
    top_x_rdd: RDD, bcast: Broadcast, config: PipelineConfig = DEFAULT_CONFIG
) -> RDD:
    """
    Args:
        top_x_rdd: RDD of (geographical_location_oid, (item_name, rank))
        bcast: Broadcast variable wrapping a dict of
        {geographical_location_oid: geographical_location}
    Returns:
        RDD[row] with fields geographical_location, item_rank, item_name
    """
    location_col = config.output_location_col
    rank_col = config.output_rank_col
    item_col = config.output_item_col
    return top_x_rdd.map(
        lambda kv: Row(
            **{
                location_col: bcast.value.get(kv[0]),
                rank_col: kv[1][1],
                item_col: kv[1][0],
            }
        )
    )


def write_output(
    rdd: RDD,
    spark: SparkSession,
    output_path: str,
    config: PipelineConfig = DEFAULT_CONFIG,
) -> None:
    OUTPUT_SCHEMA = StructType(
        [
            StructField(config.output_location_col, StringType(), True),
            StructField(config.output_rank_col, IntegerType(), False),
            StructField(config.output_item_col, StringType(), True),
        ]
    )

    spark.createDataFrame(rdd, OUTPUT_SCHEMA).write.mode("overwrite").parquet(
        output_path
    )


def run_pipeline(
    spark: SparkSession,
    file1_path: str,
    file2_path: str,
    output_path: str,
    top_x: int,
    config: PipelineConfig = DEFAULT_CONFIG,
) -> None:
    file1_rdd = read_parquet(spark, file1_path).rdd
    file2_rdd = read_parquet(spark, file2_path).rdd

    counted_rdd = count_unique_detections(file1_rdd, config)
    top_x_rdd = get_top_x_ranked(counted_rdd, top_x)
    location_bcast = build_location_broadcast(file2_rdd, spark.sparkContext, config)
    enriched_rdd = enrich_with_location(top_x_rdd, location_bcast, config)

    write_output(enriched_rdd, spark, output_path, config)


def main() -> None:
    args = parse_args()
    try:
        spark = build_spark_session(args.env)
        run_pipeline(spark, args.file1, args.file2, args.output_path, args.top_x)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
