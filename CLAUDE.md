# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install all dependency groups (dev + create_data)
pip install -e ".[dev,create_data]"
# or with Poetry:
poetry install --with dev --extras create_data

# Format
black src/ tests/

# Lint
flake8 src/ tests/

# Generate datasets (writes data/dataset_A.parquet and data/dataset_B.parquet)
python -m utopia.create_dataset.create_dataset
# or directly:
python src/utopia/create_dataset/create_dataset.py
```

## Architecture

This is a PySpark data processing project. The package lives under `src/utopia/` (src layout).

**Dependency groups** (defined in `pyproject.toml`):
- `dev` — flake8, flake8-bugbear, flake8-isort (linting only)
- `create_data` — pandas, faker, pyarrow (used only by the dataset generator)
- Core runtime dependency: pyspark ≥ 4.1

**`create_dataset` module** (`src/utopia/create_dataset/create_dataset.py`):
- Generates two deterministic Parquet test datasets from fixed seed 42
- `dataset_B` is generated first: 10,000 rows of unique `geographical_location_oid` + city names
- `dataset_A` is generated second: 20,000 rows with FK references into dataset_B's location IDs, 20% duplicate `detection_oid` values, sorted ascending by `timestamp_detected`
- Referential integrity is guaranteed: dataset_A's location pool is sampled from dataset_B's IDs
- Outputs go to `data/` at the repo root (created if absent)
- Pure functions only — no shared mutable state; all state flows through arguments and return values

## Code Style

- All code must pass both black (formatter, line length 88) and flake8 (linter). Run both before committing.
- Long signatures and chained calls use implicit line continuation inside parentheses
- isort enforced via flake8-isort
