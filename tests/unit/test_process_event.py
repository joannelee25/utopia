import pytest
from pyspark.sql import Row

from utopia.process_event.process_event import (
    build_location_broadcast,
    count_unique_detections,
    enrich_with_location,
    get_top_x_ranked,
)


@pytest.mark.parametrize(
    "rows, item_name, location_oid, expected_count", [
        [
            (
                Row(geographical_location_oid=1,
                    video_camera_oid=1,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704111134679),
                Row(geographical_location_oid=1,
                    video_camera_oid=2,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704108277078),
                Row(geographical_location_oid=1,
                    video_camera_oid=3,
                    detection_oid=20,
                    item_name="cat",
                    timestamp_detected=1704108277078),
                Row(geographical_location_oid=2,
                    video_camera_oid=4,
                    detection_oid=30,
                    item_name="dog",
                    timestamp_detected=1704108277078),
            ),
            "cat",
            1,
            2
        ],
        [
            (
                Row(geographical_location_oid=1,
                    video_camera_oid=1,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704111134679),
                Row(geographical_location_oid=1,
                    video_camera_oid=2,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704108277078),
                Row(geographical_location_oid=1,
                    video_camera_oid=3,
                    detection_oid=20,
                    item_name="cat",
                    timestamp_detected=1704108277078),
                Row(geographical_location_oid=2,
                    video_camera_oid=4,
                    detection_oid=30,
                    item_name="dog",
                    timestamp_detected=1704108277078),
            ),
            "dog",
            2,
            1
        ],
        [
            (
                Row(geographical_location_oid=1,
                    detection_oid=10,
                    item_name="cat"),
                Row(geographical_location_oid=1,
                    detection_oid=10,
                    item_name="cat"),
                Row(geographical_location_oid=1,
                    detection_oid=20,
                    item_name="cat"),
                Row(geographical_location_oid=1,
                    detection_oid=30,
                    item_name="dog"),
            ),
            "cat",
            1,
            2
        ],
        [
            (
                Row(geographical_location_oid=1,
                    detection_oid=10,
                    item_name="cat"),
                Row(geographical_location_oid=1,
                    detection_oid=10,
                    item_name="cat"),
                Row(geographical_location_oid=1,
                    detection_oid=20,
                    item_name="cat"),
                Row(geographical_location_oid=1,
                    detection_oid=30,
                    item_name="dog"),
            ),
            "dog",
            1,
            1
        ],
        [
            (
                Row(geographical_location_oid=1,
                    detection_oid=10,
                    item_name="cat"),
            ),
            "cat",
            1,
            1
        ]
    ]
)
def test_count_unique_detections_basic(
        rows,
        item_name,
        location_oid,
        expected_count,
        spark
    ):
    rdd = spark.sparkContext.parallelize(rows)
    result = dict(count_unique_detections(rdd).collect())
    assert result[(item_name, location_oid)] == expected_count

@pytest.mark.parametrize(
    "rows, expected_count", [
        [
            (
                Row(item_name="cat", geographical_location_oid=1, detection_oid=10),
                Row(item_name="dog", geographical_location_oid=2, detection_oid=10),
            ),
            1
        ],
        [
            (
                Row(item_name="cat", geographical_location_oid=1, detection_oid=10),
                Row(item_name="dog", geographical_location_oid=2, detection_oid=10),
                Row(item_name="dog", geographical_location_oid=2, detection_oid=11)
            ),
            2
        ]
    ]
)
def test_count_unique_detections_deduplicates_on_detection_oid_only(
        rows,
        expected_count,
        spark
    ):
    rdd = spark.sparkContext.parallelize(rows)
    result = dict(count_unique_detections(rdd).collect())
    assert sum(result.values()) == expected_count

@pytest.mark.parametrize(
        "data, top_x, expected_result", [
            (
                [
                    (("a", 1), 5),
                    (("b", 2), 4),
                    (("c", 3), 3),
                    (("d", 4), 2),
                    (("e", 5), 1),
                ],
                2,
                2
            ),
            (
                [
                    (("a", 1), 5),
                    (("b", 2), 4),
                    (("c", 3), 4),
                    (("d", 4), 2),
                    (("e", 5), 1),
                ],
                2,
                2
            )
        ]
)
def test_get_top_x_ranked_returns_correct_count(data, top_x, expected_result, spark):
    rdd = spark.sparkContext.parallelize(data)
    result = get_top_x_ranked(rdd, top_x).collect()
    assert len(result) == expected_result


def test_get_top_x_ranked_rank_order(spark):
    data = [
        (("a", 1), 5),
        (("b", 2), 10),
        (("c", 3), 3),
    ]
    rdd = spark.sparkContext.parallelize(data)
    result = {
        geo_oid: (name, rank)
        for geo_oid, (name, rank) in get_top_x_ranked(rdd, top_x=3).collect()
    }
    assert result[2][1] == 1  # geo_oid=2 ("b") has highest count → rank 1
    assert result[1][1] == 2  # geo_oid=1 ("a") is second → rank 2
    assert result[3][1] == 3  # geo_oid=3 ("c") is third → rank 3

@pytest.mark.parametrize(
        "data, top_x, geo_id, rank", [
            (
                [
                    (("a", 1), 5),
                    (("b", 2), 10),
                    (("c", 3), 3),
                ],
                3,
                2,
                1,
            ),
        ]
)
def test_get_top_x_ranked_rank_order(data, top_x, geo_id, rank, spark):
    rdd = spark.sparkContext.parallelize(data)
    result = {
        geo_oid: (name, rank)
        for geo_oid, (name, rank) in get_top_x_ranked(rdd, top_x).collect()
    }
    assert result[geo_id][1] == rank  # geo_oid=2 ("b") has highest count → rank 1


def test_get_top_x_ranked_top_x_larger_than_data(spark):
    data = [
        (("a", 1), 5),
        (("b", 2), 3),
    ]
    rdd = spark.sparkContext.parallelize(data)
    result = get_top_x_ranked(rdd, top_x=100).collect()
    assert len(result) == 2


def test_get_top_x_ranked_top_x_smaller_than_data(spark):
    data = [
        (("a", 1), 5),
        (("b", 2), 10),
        (("c", 3), 3),
    ]
    rdd = spark.sparkContext.parallelize(data)
    result = get_top_x_ranked(rdd, top_x=2).collect()
    assert len(result) == 2


def test_build_location_broadcast(spark):
    rows = [
        Row(geographical_location_oid=1, geographical_location="Paris"),
        Row(geographical_location_oid=2, geographical_location="Tokyo"),
    ]
    rdd = spark.sparkContext.parallelize(rows)
    bcast = build_location_broadcast(rdd, spark.sparkContext)
    assert bcast.value[1] == "Paris"
    assert bcast.value[2] == "Tokyo"


def test_enrich_with_location(spark):
    top_x_rdd = spark.sparkContext.parallelize([(1, ("cat", 1))])
    rows = [Row(geographical_location_oid=1, geographical_location="Paris")]
    bcast = build_location_broadcast(
        spark.sparkContext.parallelize(rows), spark.sparkContext
    )
    result = enrich_with_location(top_x_rdd, bcast).collect()
    assert len(result) == 1
    assert result[0].geographical_location == "Paris"
    assert result[0].item_rank == 1
    assert result[0].item_name == "cat"


def test_enrich_with_location_missing_key(spark):
    top_x_rdd = spark.sparkContext.parallelize([(999, ("cat", 1))])
    rows = [Row(geographical_location_oid=1, geographical_location="Paris")]
    bcast = build_location_broadcast(
        spark.sparkContext.parallelize(rows), spark.sparkContext
    )
    result = enrich_with_location(top_x_rdd, bcast).collect()
    assert len(result) == 1
    assert result[0].geographical_location is None
