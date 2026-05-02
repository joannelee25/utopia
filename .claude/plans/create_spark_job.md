# Plan: process_event PySpark Job

## Context

The project needs a new PySpark RDD job that reads two parquet files (detection events + geographical location dimension), counts unique detection events per (item_name, geographical_location_oid) pair, returns the top X ranked results, enriches them with the location name, and writes the output parquet. This follows the existing `create_dataset` module conventions: pure functions, full type hints, functional composition, src layout.

## Files to Create / Modify

- **Create** `src/utopia/process_event/__init__.py` — empty, marks the package
- **Create** `src/utopia/process_event/process_event.py` — job entry point
- **Create** `tests/unit/test_process_event.py` — unit tests (pure function logic, no Spark)
- **Create** `tests/integration/test_process_event_integration.py` — integration tests (real SparkSession)
- **No changes** to `pyproject.toml` — pyspark is already a core runtime dependency; pytest added to `dev` group if not present

## Module Structure

```
src/utopia/process_event/
├── __init__.py
└── process_event.py
```

## process_event.py Structure

Functions in declaration order (each pure, single-responsibility):

```
parse_args()                             → argparse.Namespace
build_spark_session()                    → SparkSession
read_parquet(spark, path)                → DataFrame        # Spark DataFrame API only
count_unique_detections(rdd)             → RDD[((str, int), int)]
get_top_x_ranked(counted_rdd, top_x)     → RDD[(int, tuple[str, int])]
build_location_broadcast(rdd, sc)        → Broadcast[dict[int, str]]
enrich_with_location(top_x_rdd, bcast)  → RDD[Row]
write_output(rdd, spark, output_path)    → None             # Spark DataFrame API only
main()                                   → None
```

**Read/write boundary rule:**
- `read_parquet` uses `spark.read.parquet(path)` → returns DataFrame, then `.rdd` is extracted inside `main()` before passing to processing functions
- `write_output` converts the final RDD back to DataFrame via `spark.createDataFrame(rdd, schema)` then calls `.write.mode("overwrite").parquet(path)`
- All functions between read and write operate exclusively on RDDs

## argparse Parameters (all required)

| Flag | Type | Description |
|------|------|-------------|
| `--file1` | `str` | Input path for detection events parquet |
| `--file2` | `str` | Input path for geographical location dimension parquet |
| `--output_file` | `str` | Output path for result parquet |
| `--top_x` | `int` | Number of top (item_name, location) pairs to return |

## RDD Transformation Pipeline

### Step 1 — Count unique detections per (item_name, geographical_location_oid)
```
file1_rdd
  .map(row → ((item_name, geographical_location_oid, detection_oid), None))
  .distinct()                           # deduplicate detection_oid per group
  .map(((name, loc, det), _) → ((name, loc), 1))
  .reduceByKey(add)                     # count uniques
```
Result type: `RDD[((str, int), int)]` — key=(item_name, geo_oid), value=unique_count

### Step 2 — Get top X ranked (1-based, rank 1 = highest count)
```
counted_rdd
  .sortBy(kv → kv[1], ascending=False)
  .zipWithIndex()                       # 0-based index
  .filter((kv, idx) → idx < top_x)
  .map(((name, loc), count), idx) → (loc, (name, idx + 1)))
```
Result type: `RDD[(int, tuple[str, int])]` — key=geo_oid, value=(item_name, rank)

### Step 3 — Build location broadcast from file2
```
location_dict = file2_rdd
  .map(row → (geographical_location_oid, geographical_location))
  .collectAsMap()

broadcast_locations = spark.sparkContext.broadcast(location_dict)
```
file2 is a small dimension table (~10,000 rows), so collecting to driver and broadcasting avoids a shuffle entirely.

### Step 4 — Enrich top X results with location name
```
top_x_rdd
  .map((geo_oid, (name, rank)) →
      Row(
          geographical_location=broadcast_locations.value.get(geo_oid),
          item_rank=rank,
          item_name=name,
      ))
```
Rows where `geo_oid` has no matching location will have `geographical_location=None` (safe given referential integrity guaranteed by dataset generator).

### Step 5 — Write output
Convert final RDD to DataFrame via `spark.createDataFrame(rows_rdd, schema)` and write as parquet.

## Output Schema (parquet file 3)

| Column | Type | Source |
|--------|------|--------|
| `geographical_location` | StringType | file2 |
| `item_rank` | IntegerType | computed (1 = highest unique detection count) |
| `item_name` | StringType | file1 |

`unique_detection_count` is used for ranking internally but not written to output.

## Code Style (matching existing conventions)

- Full type annotations on all functions
- Implicit line continuation inside parentheses for long signatures
- No shared mutable state; all state flows through arguments and return values
- SparkSession created once in `main()`, passed down
- Entry point: `if __name__ == "__main__": main()`
- Black-formatted (line length 88) + flake8 + isort compliant

## Tests

### Unit Tests — `tests/unit/test_process_event.py`

Test each pure RDD transformation function in isolation using a local SparkContext (`master("local")`). No file I/O.

| Test | What it covers |
|------|---------------|
| `test_count_unique_detections_basic` | Counts unique detection_oid per (item_name, geo_oid); verifies duplicates collapsed |
| `test_count_unique_detections_single_row` | Edge case: single row returns count of 1 |
| `test_get_top_x_ranked_returns_correct_count` | top_x=2 from 5 items returns exactly 2 rows |
| `test_get_top_x_ranked_rank_order` | item with highest count gets rank=1 |
| `test_get_top_x_ranked_top_x_larger_than_data` | top_x > dataset size returns all rows |
| `test_build_location_lookup` | Maps geo_oid → location string correctly |
| `test_enrich_with_location` | Join produces correct Row with all 3 output fields |
| `test_enrich_with_location_missing_key` | Rows with no matching location are dropped (inner join) |

### Integration Tests — `tests/integration/test_process_event_integration.py`

Test full pipeline end-to-end using `tmp_path` (pytest fixture) for I/O. Write small in-memory DataFrames to temp parquet, run full pipeline, read output and assert.

| Test | What it covers |
|------|---------------|
| `test_full_pipeline_top1` | top_x=1 returns exactly 1 output row with correct rank |
| `test_full_pipeline_output_schema` | Output parquet has exactly 3 columns: geographical_location, item_rank, item_name |
| `test_full_pipeline_rank_ordering` | With top_x=3, ranks are 1,2,3 with no gaps or duplicates |
| `test_full_pipeline_deduplication` | Duplicate detection_oid for same (item_name, geo_oid) counted as 1 |

### pytest fixtures (shared via `tests/conftest.py`)

```python
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("test").getOrCreate()
```

Session-scoped so SparkSession is created once across all tests.

## spark-submit Design Notes

- `SparkSession` is created without `.master(...)` — the cluster/local master is supplied by `spark-submit` at runtime
- Script is invoked as:
  ```bash
  spark-submit src/utopia/process_event/process_event.py \
    --file1 data/dataset_A.parquet \
    --file2 data/dataset_B.parquet \
    --output data/dataset_C.parquet \
    --top_x 10
  ```
- No `python -m` entry point needed; `if __name__ == "__main__": main()` is sufficient

## Verification (lint only — do not run)

```bash
black src/utopia/process_event/ tests/
flake8 src/utopia/process_event/ tests/
```
