import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

SEED = 42

# dataset_A constants
NUM_ROWS_A = 20_000
NUM_LOCATIONS = 100
NUM_CAMERAS = 500
NUM_ITEMS = 40

# dataset_B constants
NUM_ROWS_B = 10_000


# --- dataset_B generators ---


def generate_geo_oids(num_rows: int, rng: np.random.Generator) -> np.ndarray:
    # Drawing 10k from ~9B makes collisions negligible; unique() ensures no dupes
    candidates = rng.integers(1_000_000_000, 9_999_999_999, size=num_rows + 100)
    return np.unique(candidates)[:num_rows]


def generate_geo_names(num_rows: int, seed: int) -> list[str]:
    faker = Faker()
    faker.seed_instance(seed)
    return [faker.unique.city() for _ in range(num_rows)]


def build_dataset_b(num_rows: int, rng: np.random.Generator, seed: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "geographical_location_oid": generate_geo_oids(num_rows, rng),
            "geographical_location": generate_geo_names(num_rows, seed),
        }
    )


# --- dataset_A generators ---


def generate_location_ids(
    num_rows: int, location_pool: np.ndarray, rng: np.random.Generator
) -> np.ndarray:
    sampled_pool = rng.choice(location_pool, size=NUM_LOCATIONS, replace=False)
    return rng.choice(sampled_pool, size=num_rows)


def generate_camera_ids(
    num_rows: int, pool_size: int, rng: np.random.Generator
) -> np.ndarray:
    pool = rng.integers(10_000_000, 99_999_999, size=pool_size)
    return rng.choice(pool, size=num_rows)


def generate_detection_ids(
    num_rows: int, duplicate_ratio: float, rng: np.random.Generator
) -> np.ndarray:
    num_unique = int(num_rows * (1 - duplicate_ratio))
    num_dupes = num_rows - num_unique
    unique_ids = rng.integers(100_000_000, 999_999_999, size=num_unique)
    dupe_ids = rng.choice(unique_ids, size=num_dupes, replace=True)
    all_ids = np.concatenate([unique_ids, dupe_ids])
    rng.shuffle(all_ids)
    return all_ids


def generate_item_names(num_rows: int, num_items: int, seed: int) -> list[str]:
    faker = Faker()
    faker.seed_instance(seed)
    items: list[str] = []
    while len(items) < num_items:
        word = faker.word()
        if word not in items:
            items.append(word)
    rng = np.random.default_rng(seed)
    indices = rng.integers(0, num_items, size=num_rows)
    return [items[i] for i in indices]


def generate_timestamps(num_rows: int, rng: np.random.Generator) -> np.ndarray:
    start_ms = 1_704_067_200_000  # 2024-01-01 00:00:00 UTC in milliseconds
    increments = rng.integers(1_000, 5_000, size=num_rows)  # 1–5 seconds in ms
    return start_ms + np.cumsum(increments)


def build_dataset_a(
    num_rows: int,
    rng: np.random.Generator,
    seed: int,
    location_pool: np.ndarray,
    duplicate_ratio: float,
) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "geographical_location_oid": generate_location_ids(
                num_rows, location_pool, rng
            ),
            "video_camera_oid": generate_camera_ids(num_rows, NUM_CAMERAS, rng),
            "detection_oid": generate_detection_ids(num_rows, duplicate_ratio, rng),
            "item_name": generate_item_names(num_rows, NUM_ITEMS, seed),
            "timestamp_detected": generate_timestamps(num_rows, rng),
        }
    )
    return df.sort_values("timestamp_detected").reset_index(drop=True)


# --- shared ---


def write_parquet(df: pd.DataFrame, output_path: str) -> None:
    target_dir = os.path.dirname(output_path)
    os.makedirs(target_dir, exist_ok=True)
    df.to_parquet(output_path, index=False, engine="pyarrow")


def parse_arg() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Script to create a set of simulation data for pyspark processing")
    )
    parser.add_argument(
        "--output_file_prefix",
        required=True,
        help="Parquet output file prefix for the simulated data",
    )
    parser.add_argument(
        "--duplicate_ratio",
        type=float,
        default=0.2,
        help=(
            "Takes float value from [0.0 - 1.0]. 0.0 meaning no duplicate"
            " and 1.0 meaning all values are the same. Used for dataset A"
            " detection_oid column"
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arg()
    rng = np.random.default_rng(SEED)
    data_dir = os.path.join(Path(__file__).parents[3], "data", "raw")

    df_b = build_dataset_b(NUM_ROWS_B, rng, SEED)
    write_parquet(
        df_b, os.path.join(data_dir, args.output_file_prefix + "dataset_B.parquet")
    )

    df_a = build_dataset_a(
        NUM_ROWS_A,
        rng,
        SEED,
        df_b["geographical_location_oid"].values,
        args.duplicate_ratio,
    )
    write_parquet(
        df_a, os.path.join(data_dir, args.output_file_prefix + "dataset_A.parquet")
    )


if __name__ == "__main__":
    main()
