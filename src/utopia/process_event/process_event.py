import argparse
from operator import add

from pyspark.broadcast import Broadcast
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process detection events")
    parser.add_argument(
        "--file1", required=True, help="Input path for detection events in parquet format"
    )
    parser.add_argument(
        "--file2",
        required=True,
        help="Input path for geographical location dimension in parquet format",
    )
    parser.add_argument(
        "--output_file", required=True, help="Output path for top x result in parquet"
    )
    parser.add_argument(
        "--top_x",
        required=True,
        type=int,
        help="Number of top (item_name, location) pairs to return",
    )
    return parser.parse_args()


def build_spark_session() -> SparkSession:
    return SparkSession.builder.appName("process_event").getOrCreate()


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def count_unique_detections(rdd: RDD) -> RDD:
    return (
        rdd.map(
            lambda row: (
                (row.item_name, row.geographical_location_oid, row.detection_oid),
                None,
            )
        )
        .distinct()
        .map(lambda kv: ((kv[0][0], kv[0][1]), 1))
        .reduceByKey(add)
    )


def get_top_x_ranked(counted_rdd: RDD, top_x: int) -> RDD:
    return (
        counted_rdd.sortBy(lambda kv: kv[1], ascending=False)
        .zipWithIndex()
        .filter(lambda kv_idx: kv_idx[1] < top_x)
        .map(lambda kv_idx: (kv_idx[0][0][1], (kv_idx[0][0][0], kv_idx[1] + 1)))
    )


def build_location_broadcast(rdd: RDD, sc) -> Broadcast:
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
    spark = build_spark_session()

    file1_rdd = read_parquet(spark, args.file1).rdd
    file2_rdd = read_parquet(spark, args.file2).rdd

    counted_rdd = count_unique_detections(file1_rdd)
    top_x_rdd = get_top_x_ranked(counted_rdd, args.top_x)
    location_bcast = build_location_broadcast(file2_rdd, spark.sparkContext)
    enriched_rdd = enrich_with_location(top_x_rdd, location_bcast)

    write_output(enriched_rdd, spark, args.output_file)


if __name__ == "__main__":
    main()
