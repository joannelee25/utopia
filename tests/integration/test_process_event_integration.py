from pyspark.sql import Row
from pyspark.sql.types import LongType, StringType, StructField, StructType

from utopia.process_event.process_event import (
    build_location_broadcast,
    count_unique_detections,
    enrich_with_location,
    get_top_x_ranked,
    read_parquet,
    write_output,
)

FILE1_SCHEMA = StructType(
    [
        StructField("geographical_location_oid", LongType(), False),
        StructField("video_camera_oid", LongType(), False),
        StructField("detection_oid", LongType(), False),
        StructField("item_name", StringType(), True),
        StructField("timestamp_detected", LongType(), False),
    ]
)

FILE2_SCHEMA = StructType(
    [
        StructField("geographical_location_oid", LongType(), False),
        StructField("geographical_location", StringType(), True),
    ]
)


def _write_file1(spark, path, rows):
    spark.createDataFrame(rows, FILE1_SCHEMA).write.mode("overwrite").parquet(str(path))


def _write_file2(spark, path, rows):
    spark.createDataFrame(rows, FILE2_SCHEMA).write.mode("overwrite").parquet(str(path))


def test_full_pipeline_top1(spark, tmp_path):
    file1_rows = [
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=100,
            item_name="cat",
            timestamp_detected=1000,
        ),
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=101,
            item_name="cat",
            timestamp_detected=1001,
        ),
        Row(
            geographical_location_oid=2,
            video_camera_oid=20,
            detection_oid=200,
            item_name="dog",
            timestamp_detected=2000,
        ),
    ]
    file2_rows = [
        Row(geographical_location_oid=1, geographical_location="Paris"),
        Row(geographical_location_oid=2, geographical_location="Tokyo"),
    ]
    f1 = tmp_path / "file1"
    f2 = tmp_path / "file2"
    out = tmp_path / "output"
    _write_file1(spark, f1, file1_rows)
    _write_file2(spark, f2, file2_rows)

    file1_rdd = read_parquet(spark, str(f1)).rdd
    file2_rdd = read_parquet(spark, str(f2)).rdd
    counted = count_unique_detections(file1_rdd)
    top_x = get_top_x_ranked(counted, top_x=1)
    bcast = build_location_broadcast(file2_rdd, spark.sparkContext)
    enriched = enrich_with_location(top_x, bcast)
    write_output(enriched, spark, str(out))

    result = spark.read.parquet(str(out)).collect()
    assert len(result) == 1
    assert result[0].item_rank == 1


def test_full_pipeline_output_schema(spark, tmp_path):
    file1_rows = [
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=100,
            item_name="cat",
            timestamp_detected=1000,
        ),
    ]
    file2_rows = [
        Row(geographical_location_oid=1, geographical_location="Paris"),
    ]
    f1 = tmp_path / "file1"
    f2 = tmp_path / "file2"
    out = tmp_path / "output"
    _write_file1(spark, f1, file1_rows)
    _write_file2(spark, f2, file2_rows)

    file1_rdd = read_parquet(spark, str(f1)).rdd
    file2_rdd = read_parquet(spark, str(f2)).rdd
    counted = count_unique_detections(file1_rdd)
    top_x = get_top_x_ranked(counted, top_x=10)
    bcast = build_location_broadcast(file2_rdd, spark.sparkContext)
    enriched = enrich_with_location(top_x, bcast)
    write_output(enriched, spark, str(out))

    df = spark.read.parquet(str(out))
    assert set(df.columns) == {"geographical_location", "item_rank", "item_name"}


def test_full_pipeline_rank_ordering(spark, tmp_path):
    file1_rows = [
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=100,
            item_name="cat",
            timestamp_detected=1000,
        ),
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=101,
            item_name="cat",
            timestamp_detected=1001,
        ),
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=102,
            item_name="cat",
            timestamp_detected=1002,
        ),
        Row(
            geographical_location_oid=2,
            video_camera_oid=20,
            detection_oid=200,
            item_name="dog",
            timestamp_detected=2000,
        ),
        Row(
            geographical_location_oid=2,
            video_camera_oid=20,
            detection_oid=201,
            item_name="dog",
            timestamp_detected=2001,
        ),
        Row(
            geographical_location_oid=3,
            video_camera_oid=30,
            detection_oid=300,
            item_name="bird",
            timestamp_detected=3000,
        ),
    ]
    file2_rows = [
        Row(geographical_location_oid=1, geographical_location="Paris"),
        Row(geographical_location_oid=2, geographical_location="Tokyo"),
        Row(geographical_location_oid=3, geographical_location="Berlin"),
    ]
    f1 = tmp_path / "file1"
    f2 = tmp_path / "file2"
    out = tmp_path / "output"
    _write_file1(spark, f1, file1_rows)
    _write_file2(spark, f2, file2_rows)

    file1_rdd = read_parquet(spark, str(f1)).rdd
    file2_rdd = read_parquet(spark, str(f2)).rdd
    counted = count_unique_detections(file1_rdd)
    top_x = get_top_x_ranked(counted, top_x=3)
    bcast = build_location_broadcast(file2_rdd, spark.sparkContext)
    enriched = enrich_with_location(top_x, bcast)
    write_output(enriched, spark, str(out))

    ranks = sorted([row.item_rank for row in spark.read.parquet(str(out)).collect()])
    assert ranks == [1, 2, 3]


def test_full_pipeline_deduplication(spark, tmp_path):
    file1_rows = [
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=100,
            item_name="cat",
            timestamp_detected=1000,
        ),
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=100,
            item_name="cat",
            timestamp_detected=1001,
        ),
        Row(
            geographical_location_oid=1,
            video_camera_oid=10,
            detection_oid=100,
            item_name="cat",
            timestamp_detected=1002,
        ),
    ]
    file2_rows = [
        Row(geographical_location_oid=1, geographical_location="Paris"),
    ]
    f1 = tmp_path / "file1"
    f2 = tmp_path / "file2"
    _write_file1(spark, f1, file1_rows)
    _write_file2(spark, f2, file2_rows)

    file1_rdd = read_parquet(spark, str(f1)).rdd
    counted = count_unique_detections(file1_rdd)

    result = dict(counted.collect())
    assert result[("cat", 1)] == 1
