import pytest
from pyspark.sql import Row

from utopia.process_event.process_event import (
    build_location_broadcast,
    count_unique_detections,
    enrich_with_location,
    get_top_x_ranked,
)


@pytest.mark.parametrize(
    "rows, item_name, location_oid, expected_count",
    [
        [
            (
                Row(
                    geographical_location_oid=1,
                    video_camera_oid=1,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704111134679,
                ),
                Row(
                    geographical_location_oid=1,
                    video_camera_oid=2,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704108277078,
                ),
                Row(
                    geographical_location_oid=1,
                    video_camera_oid=3,
                    detection_oid=20,
                    item_name="cat",
                    timestamp_detected=1704108277078,
                ),
                Row(
                    geographical_location_oid=2,
                    video_camera_oid=4,
                    detection_oid=30,
                    item_name="dog",
                    timestamp_detected=1704108277078,
                ),
            ),
            "cat",
            1,
            2,
        ],
        [
            (
                Row(
                    geographical_location_oid=1,
                    video_camera_oid=1,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704111134679,
                ),
                Row(
                    geographical_location_oid=1,
                    video_camera_oid=2,
                    detection_oid=10,
                    item_name="cat",
                    timestamp_detected=1704108277078,
                ),
                Row(
                    geographical_location_oid=1,
                    video_camera_oid=3,
                    detection_oid=20,
                    item_name="cat",
                    timestamp_detected=1704108277078,
                ),
                Row(
                    geographical_location_oid=2,
                    video_camera_oid=4,
                    detection_oid=30,
                    item_name="dog",
                    timestamp_detected=1704108277078,
                ),
            ),
            "dog",
            2,
            1,
        ],
        [
            (
                Row(geographical_location_oid=1, detection_oid=10, item_name="cat"),
                Row(geographical_location_oid=1, detection_oid=10, item_name="cat"),
                Row(geographical_location_oid=1, detection_oid=20, item_name="cat"),
                Row(geographical_location_oid=1, detection_oid=30, item_name="dog"),
            ),
            "cat",
            1,
            2,
        ],
        [
            (
                Row(geographical_location_oid=1, detection_oid=10, item_name="cat"),
                Row(geographical_location_oid=1, detection_oid=10, item_name="cat"),
                Row(geographical_location_oid=1, detection_oid=20, item_name="cat"),
                Row(geographical_location_oid=1, detection_oid=30, item_name="dog"),
            ),
            "dog",
            1,
            1,
        ],
        [
            (Row(geographical_location_oid=1, detection_oid=10, item_name="cat"),),
            "cat",
            1,
            1,
        ],
    ],
)
def test_count_unique_detections_basic(
    rows,
    item_name,
    location_oid,
    expected_count,
    spark,
):
    rdd = spark.sparkContext.parallelize(rows)
    result = dict(count_unique_detections(rdd).collect())
    assert result[(item_name, location_oid)] == expected_count


@pytest.mark.parametrize(
    "rows, expected_count",
    [
        [
            (
                Row(item_name="cat", geographical_location_oid=1, detection_oid=10),
                Row(item_name="dog", geographical_location_oid=2, detection_oid=10),
            ),
            1,
        ],
        [
            (
                Row(item_name="cat", geographical_location_oid=1, detection_oid=10),
                Row(item_name="dog", geographical_location_oid=2, detection_oid=10),
                Row(item_name="dog", geographical_location_oid=2, detection_oid=11),
            ),
            2,
        ],
    ],
)
def test_count_unique_detections_deduplicates_on_detection_oid_only(
    rows,
    expected_count,
    spark,
):
    rdd = spark.sparkContext.parallelize(rows)
    result = dict(count_unique_detections(rdd).collect())
    assert sum(result.values()) == expected_count


@pytest.mark.parametrize(
    "data, top_x, expected_result",
    [
        (
            [
                (("a", 1), 5),
                (("b", 2), 4),
                (("c", 3), 3),
                (("d", 4), 2),
                (("e", 5), 1),
            ],
            2,
            2,
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
            2,
        ),
    ],
)
def test_get_top_x_ranked_returns_correct_count(data, top_x, expected_result, spark):
    rdd = spark.sparkContext.parallelize(data)
    result = get_top_x_ranked(rdd, top_x).collect()
    assert len(result) == expected_result


@pytest.mark.parametrize(
    "data, top_x, geo_id, rank",
    [
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
        (
            [
                (("a", 1), 5),
                (("b", 2), 10),
                (("c", 3), 3),
            ],
            3,
            1,
            2,
        ),
        (
            [
                (("a", 1), 5),
                (("b", 2), 10),
                (("c", 3), 3),
            ],
            3,
            3,
            3,
        ),
    ],
)
def test_get_top_x_ranked_rank_order(data, top_x, geo_id, rank, spark):
    rdd = spark.sparkContext.parallelize(data)
    result = {
        geo_oid: (name, rank)
        for geo_oid, (name, rank) in get_top_x_ranked(rdd, top_x).collect()
    }
    assert result[geo_id][1] == rank


@pytest.mark.parametrize(
    "data, top_x, expected_count",
    [
        (
            [
                (("a", 1), 5),
                (("b", 2), 3),
            ],
            100,
            2,
        ),
        (
            [
                (("a", 1), 5),
                (("b", 2), 10),
                (("c", 3), 3),
            ],
            2,
            2,
        ),
    ],
)
def test_get_top_x_ranked_top_x_filter(data, top_x, expected_count, spark):
    rdd = spark.sparkContext.parallelize(data)
    result = get_top_x_ranked(rdd, top_x).collect()
    assert len(result) == expected_count


@pytest.mark.parametrize(
    "rows, key, expected_value",
    [
        (
            [
                Row(geographical_location_oid=1, geographical_location="Paris"),
                Row(geographical_location_oid=2, geographical_location="Tokyo"),
            ],
            1,
            "Paris",
        ),
        (
            [
                Row(geographical_location_oid=1, geographical_location="Paris"),
                Row(geographical_location_oid=2, geographical_location="Tokyo"),
            ],
            2,
            "Tokyo",
        ),
    ],
)
def test_build_location_broadcast(rows, key, expected_value, spark):
    rdd = spark.sparkContext.parallelize(rows)
    bcast = build_location_broadcast(rdd, spark.sparkContext)
    assert bcast.value[key] == expected_value


@pytest.mark.parametrize(
    "data, broadcast_row, expected_geographical_location, expected_item_rank, "
    "expected_item_name",
    [
        (
            [(1, ("cat", 1))],
            [Row(geographical_location_oid=1, geographical_location="Paris")],
            "Paris",
            1,
            "cat",
        ),
        (
            [(999, ("cat", 1))],
            [Row(geographical_location_oid=1, geographical_location="Paris")],
            None,
            1,
            "cat",
        ),
    ],
)
def test_enrich_with_location(
    data,
    broadcast_row,
    expected_geographical_location,
    expected_item_rank,
    expected_item_name,
    spark,
):
    top_x_rdd = spark.sparkContext.parallelize(data)
    bcast = build_location_broadcast(
        spark.sparkContext.parallelize(broadcast_row), spark.sparkContext
    )
    result = enrich_with_location(top_x_rdd, bcast).collect()
    assert result[0].geographical_location == expected_geographical_location
    assert result[0].item_rank == expected_item_rank
    assert result[0].item_name == expected_item_name
