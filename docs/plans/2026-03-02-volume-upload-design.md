# Design: TDV → Databricks Volume Upload

**Date:** 2026-03-02

## Goal

Add a second script (`upload_to_volume.py`) that takes the Parquet files produced by `export_to_parquet.py` and lands them in an existing Databricks Unity Catalog Volume using the Databricks SDK Files API.

## Two-script workflow

```
export_to_parquet.py   →   output/{prefix}/{prefix}_{date}.parquet
upload_to_volume.py    →   /Volumes/{catalog}/{schema}/{volume}/{prefix}/{prefix}_{date}.parquet
```

`export_to_parquet.py` is unchanged. Adding a new dataset requires only a new entry in its `QUERIES` dict; `upload_to_volume.py` discovers subfolders dynamically and needs no changes.

## Environment

Managed with conda via `environment.yml`. `databricks-sdk` is added under a `pip:` block (PyPI only):

```yaml
name: tdvdata
dependencies:
  - python=3.11
  - pandas
  - pyodbc
  - pyarrow
  - pip
  - pip:
    - databricks-sdk
```

## `upload_to_volume.py` — configuration

Constants at the top of the script, same style as `export_to_parquet.py`:

```python
PROFILE    = "DEV"       # ~/.databrickscfg profile
CATALOG    = "main"
SCHEMA     = "default"
VOLUME     = "my_volume" # must already exist in UC
OUTPUT_DIR = Path("output")
```

Authentication is handled entirely by `WorkspaceClient(profile=PROFILE)`.

## Upload logic

For each subfolder found under `OUTPUT_DIR`:

1. Call `client.files.list_directory_contents(volume_subfolder_path)` to get the set of filenames already in the volume.
   - If the directory does not exist (`NotFound`), treat the remote set as empty. The directory is created implicitly on first upload.
2. Compare local `.parquet` files against the remote set.
3. Upload only files not already present (`overwrite=False`).
4. Print a per-file status line and a per-subfolder summary (uploaded / skipped counts).

## Volume path mapping

Local structure mirrors volume structure exactly:

```
output/performance_net/performance_net_2026-01-31.parquet
  → /Volumes/main/default/my_volume/performance_net/performance_net_2026-01-31.parquet
```

## Out of scope

- Delta table creation (`CREATE TABLE ... AS SELECT * FROM read_files(...)`) — files are landed as raw Parquet only.
- The `DatabricksIngestionSDK/` subfolder is not used at runtime and can be removed or kept as reference.
