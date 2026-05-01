# Plan: Generate dataset_A.parquet and dataset_B.parquet Test Datasets

## Context

Production datasets need lightweight local test versions for development and testing. A single script `create_dataset.py` will generate both datasets deterministically using functional programming principles.

---

## dataset_A

### Schema

| Column | Type | Description |
|---|---|---|
| `geographical_location_oid` | BIGINT | ID from a pool of geographical locations (non-unique per row) |
| `video_camera_oid` | BIGINT | ID from a pool of video cameras (non-unique per row) |
| `detection_oid` | BIGINT | Unique detection event ID — **20% of rows are duplicates** |
| `item_name` | STRING | One of 40 possible item names detected by surveillance cameras |
| `timestamp_detected` | BIGINT | Unix timestamp in ms — **dataset sorted ascending by this column** |

### Generation logic

1. **`geographical_location_oid`**: Sample 100 IDs from dataset_B's `geographical_location_oid` column; randomly assign across all 20,000 rows.
2. **`video_camera_oid`**: Pick from a pool of 500 unique BIGINTs, randomly assigned across all 20,000 rows.
3. **`detection_oid`**: Generate 16,000 unique BIGINTs; sample 4,000 duplicates from them; shuffle all 20,000.
4. **`item_name`**: 40 distinct Faker words, one randomly assigned per row.
5. **`timestamp_detected`**: 20,000 ascending BIGINT ms timestamps from 2024-01-01, incremented 1–5 s each step.
6. Sort by `timestamp_detected` ascending before writing.

---

## dataset_B

### Schema

| Column | Type | Description |
|---|---|---|
| `geographical_location_oid` | BIGINT | Unique identifier per row — no duplicates |
| `geographical_local` | STRING (varchar 500) | Name of the geographical location |

### Generation logic

- 10,000 rows, one unique `geographical_location_oid` per row.
- `geographical_location_oid`: 10,000 unique BIGINTs.
- `geographical_local`: Faker-generated location names (e.g. `faker.city()`), one per row.
- **Generated first** — dataset_A's 100-location pool is sampled from dataset_B's `geographical_location_oid` values, guaranteeing full referential integrity.

### Referential integrity with dataset_A

- After generating dataset_B, sample 100 `geographical_location_oid` values from it.
- Pass that sample as the pool to `generate_location_ids()` for dataset_A.
- This ensures every `geographical_location_oid` in dataset_A exists in dataset_B.

---

## Implementation

### Files to create

- `src/utopia_project/create_dataset/create_dataset.py` — single generator script for both datasets
- `data/dataset_A.parquet` — 20,000 rows
- `data/dataset_B.parquet` — 10,000 rows

### Functional programming principles

- **Pure functions**: Each generation step is a pure function — no side effects, no shared mutable state.
- **Single responsibility**: One function per column/concern.
- **Composition**: `build_dataset_a()` and `build_dataset_b()` each compose their column generators; `main()` calls both and writes both files.
- **No global mutation**: Constants are module-level; all state flows through function arguments and return values.

### Script structure

```python
from pathlib import Path
import numpy as np
import pandas as pd
from faker import Faker

SEED = 42

# dataset_A constants
NUM_ROWS_A = 20_000
DUPLICATE_RATIO = 0.20
NUM_LOCATIONS = 100
NUM_CAMERAS = 500
NUM_ITEMS = 40

# dataset_B constants
NUM_ROWS_B = 10_000


# --- dataset_A generators ---
def generate_location_ids(num_rows: int, location_pool: np.ndarray, rng: np.random.Generator) -> np.ndarray: ...
def generate_camera_ids(num_rows: int, pool_size: int, rng: np.random.Generator) -> np.ndarray: ...
def generate_detection_ids(num_rows: int, duplicate_ratio: float, rng: np.random.Generator) -> np.ndarray: ...
def generate_item_names(num_rows: int, num_items: int, seed: int) -> list[str]: ...
def generate_timestamps(num_rows: int, rng: np.random.Generator) -> np.ndarray: ...
def build_dataset_a(num_rows: int, rng: np.random.Generator, seed: int, location_pool: np.ndarray) -> pd.DataFrame: ...

# --- dataset_B generators ---
def generate_geo_oids(num_rows: int, rng: np.random.Generator) -> np.ndarray: ...
def generate_geo_names(num_rows: int, seed: int) -> list[str]: ...
def build_dataset_b(num_rows: int, rng: np.random.Generator, seed: int) -> pd.DataFrame: ...

# --- shared ---
def write_parquet(df: pd.DataFrame, output_path: Path) -> None: ...

def main() -> None:
    rng = np.random.default_rng(SEED)
    data_dir = Path(__file__).parents[3] / "data"
    df_b = build_dataset_b(NUM_ROWS_B, rng, SEED)
    write_parquet(df_b, data_dir / "dataset_B.parquet")
    df_a = build_dataset_a(NUM_ROWS_A, rng, SEED, df_b["geographical_location_oid"].values)
    write_parquet(df_a, data_dir / "dataset_A.parquet")

if __name__ == "__main__":
    main()
```

## Verification

Review `src/utopia/create_dataset/create_dataset.py` to confirm:
- All column generator functions are present and pure
- `build_dataset_b()` is called before `build_dataset_a()`
- `build_dataset_a()` receives `df_b["geographical_location_oid"].values` as its location pool
- Every `geographical_location_oid` in dataset_A is a subset of dataset_B's values
- `write_parquet()` creates the `data/` directory and writes both parquet files
- `main()` wires everything together with a fixed seed
- All lines are ≤ 88 characters per `setup.cfg`

The script is **not** executed as part of this task — running it is a separate step.
