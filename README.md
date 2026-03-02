# tdvdata

Export TDV database data to Parquet files and upload them to a Databricks Unity Catalog Volume.

## Prerequisites

- An ODBC DSN named **TDV PRD** must be configured and accessible.
- A Databricks workspace with an existing UC Volume.
- A `~/.databrickscfg` profile for your Databricks workspace (the Databricks VS Code extension creates this automatically).

## Setup

```bash
conda env create -f environment.yml
conda activate tdvdata
```

To update an existing environment after dependency changes:

```bash
conda env update -f environment.yml --prune
```

## Usage

**Step 1 — Export from TDV to local Parquet files:**

```bash
python export_to_parquet.py
```

Writes Parquet files to `output/{prefix}/{prefix}_{date}.parquet`. Skips dates already exported.

**Step 2 — Upload to Databricks UC Volume:**

```bash
python upload_to_volume.py
```

Uploads every file in `output/` to the configured Volume. Skips files already present — safe to re-run.

## Configuration

### export_to_parquet.py

Edit the constants at the top of the file:

- `REPORT_DATE` — start date in `yyyymmdd` format (e.g. `20231231`); only dates >= this value are exported
- `OUTPUT_DIR` — local output directory (default: `output/`)
- `QUERIES` — dict of datasets to export; add a new entry here to export an additional table

### upload_to_volume.py

Edit the constants at the top of the file:

- `PROFILE` — `~/.databrickscfg` profile name
- `CATALOG` — Unity Catalog catalog name
- `SCHEMA` — Unity Catalog schema name
- `VOLUME` — UC Volume name (must already exist)

## Tests

```bash
pytest tests/
```

Unit tests for `upload_to_volume.py`. No Databricks connection required.
