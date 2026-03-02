# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Two-script Python tool that exports TDV (TIBCO Data Virtualization) database tables to Parquet files and uploads them to a Databricks Unity Catalog Volume:

1. `export_to_parquet.py` — queries TDV via ODBC and writes partitioned Parquet files under `output/`. Requires an ODBC DSN named **TDV PRD** on the host machine.
2. `upload_to_volume.py` — uploads the `output/` files to a UC Volume. Requires a `~/.databrickscfg` entry for the configured profile.

## Setup & Commands

```bash
# Create the conda environment
conda env create -f environment.yml

# Update an existing environment after dependency changes
conda env update -f environment.yml --prune

conda activate tdvdata

# Step 1 — export from TDV to local Parquet
python export_to_parquet.py

# Step 2 — upload local Parquet files to Databricks UC Volume
python upload_to_volume.py

# Run unit tests (no Databricks connection required)
pytest tests/
```

## Configuration

### export_to_parquet.py

All configuration lives at the top of the file:

- `REPORT_DATE` — integer in `yyyymmdd` format; only dates >= this value are exported
- `OUTPUT_DIR` — root output directory (default: `output/`)
- `QUERIES` — dict mapping export names to table metadata:
  - `table` — fully-qualified TDV table name
  - `date_col` — column used for date partitioning
  - `date_type` — `"int"` or `"date"` (controls how the date param is passed to ODBC)
  - `filename_prefix` — used for the output subdirectory and file names

### upload_to_volume.py

All configuration lives at the top of the file:

- `PROFILE` — `~/.databrickscfg` profile name (the Databricks VS Code extension creates this automatically)
- `CATALOG` — Unity Catalog catalog name
- `SCHEMA` — Unity Catalog schema name
- `VOLUME` — UC Volume name (must already exist)

## Architecture

### export_to_parquet.py

The script runs a two-step pipeline per query in `QUERIES`:

1. **Discover dates** — queries `DISTINCT {date_col}` values from the table filtered by `REPORT_DATE`, returning a list of dates to process.
2. **Export partitions** — for each date, fetches all rows matching that date and writes them to `output/{filename_prefix}/{filename_prefix}_{YYYY-MM-DD}.parquet`. Existing files are skipped (idempotent).

Output Parquet files are stored under `output/` with one file per date per query, e.g. `output/performance_net/performance_net_2026-01-31.parquet`.

### upload_to_volume.py

For each subfolder in `output/`, the script:

1. **Lists remote files** — checks which filenames are already present in the matching Volume subfolder (`/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{subfolder}`). If the subfolder does not yet exist, it is treated as empty.
2. **Uploads new files** — uploads only the Parquet files not already in the Volume. Files already present are skipped, making re-runs safe (idempotent).

## Authentication

`upload_to_volume.py` authenticates via the Databricks SDK using the profile named in `PROFILE`. This profile must have a corresponding entry in `~/.databrickscfg`. The Databricks VS Code extension creates and manages this file automatically when you sign in.

## Tests

`tests/test_upload_to_volume.py` contains 4 unit tests for `upload_to_volume.py` helper functions. They use mocks and do not require a Databricks connection:

```bash
pytest tests/
```

## Dependencies

| Package         | Purpose                              |
|-----------------|--------------------------------------|
| pyodbc          | ODBC connection to TDV               |
| pandas          | DataFrame handling                   |
| pyarrow         | Parquet serialization                |
| tqdm            | Progress bar during export/upload    |
| databricks-sdk  | UC Volume file operations            |
| pytest          | Unit tests                           |
