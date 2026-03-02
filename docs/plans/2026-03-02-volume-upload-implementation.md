# Volume Upload Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `upload_to_volume.py` that uploads Parquet files from `output/` into an existing Databricks Unity Catalog Volume, skipping files already present.

**Architecture:** Two flat scripts — `export_to_parquet.py` (unchanged) produces `output/{prefix}/*.parquet`; `upload_to_volume.py` walks `output/`, checks what is already in the Volume via the SDK Files API, and uploads only missing files. No package, no helper modules — self-contained single file matching the style of `export_to_parquet.py`.

**Tech Stack:** Python 3.11, `databricks-sdk` (Files API), `tqdm`, `pytest` + `unittest.mock` for tests.

---

### Task 1: Update environment.yml

**Files:**
- Modify: `environment.yml`

**Step 1: Edit `environment.yml`**

Replace the entire file with:

```yaml
name: tdvdata
dependencies:
  - python=3.11
  - pandas
  - pyodbc
  - pyarrow
  - tqdm
  - pip
  - pip:
    - databricks-sdk
    - pytest
```

Note: `tqdm` moves from pip (it was missing) to conda. `databricks-sdk` and `pytest` are pip-only.

**Step 2: Update the conda environment**

```bash
conda env update -f environment.yml --prune
conda activate tdvdata
```

Expected: environment updates without errors, `databricks-sdk` and `pytest` installed.

**Step 3: Verify**

```bash
python -c "from databricks.sdk import WorkspaceClient; print('OK')"
python -c "import pytest; print('OK')"
```

Expected: both print `OK`.

**Step 4: Commit**

```bash
git add environment.yml
git commit -m "chore: add databricks-sdk, tqdm, pytest to environment"
```

---

### Task 2: Write failing tests

**Files:**
- Create: `tests/__init__.py` (empty)
- Create: `tests/test_upload_to_volume.py`

**Step 1: Create `tests/__init__.py`**

Create an empty file at `tests/__init__.py`.

**Step 2: Write the tests**

Create `tests/test_upload_to_volume.py` with this content:

```python
from unittest.mock import MagicMock, patch
import pytest

# --- helpers we will import from the script once it exists ---
# We import them here so the test file fails until the script exists.
from upload_to_volume import get_remote_files, volume_dir_path


# ── get_remote_files ──────────────────────────────────────────────────────────

def test_get_remote_files_returns_filenames_when_dir_exists():
    """Returns just the filenames (not full paths) for each non-directory item."""
    mock_item = MagicMock()
    mock_item.path = "/Volumes/main/default/myvol/perf/perf_2026-01-31.parquet"
    mock_item.is_directory = False

    client = MagicMock()
    client.files.list_directory_contents.return_value = [mock_item]

    result = get_remote_files(client, "/Volumes/main/default/myvol/perf")

    assert result == {"perf_2026-01-31.parquet"}


def test_get_remote_files_excludes_subdirectories():
    """Subdirectory entries are excluded from the returned set."""
    mock_file = MagicMock()
    mock_file.path = "/Volumes/main/default/myvol/perf/file.parquet"
    mock_file.is_directory = False

    mock_dir = MagicMock()
    mock_dir.path = "/Volumes/main/default/myvol/perf/subdir"
    mock_dir.is_directory = True

    client = MagicMock()
    client.files.list_directory_contents.return_value = [mock_file, mock_dir]

    result = get_remote_files(client, "/Volumes/main/default/myvol/perf")

    assert result == {"file.parquet"}


def test_get_remote_files_returns_empty_set_when_not_found():
    """Returns an empty set when the volume directory does not exist yet."""
    from databricks.sdk.errors import NotFound

    client = MagicMock()
    client.files.list_directory_contents.side_effect = NotFound("not found")

    result = get_remote_files(client, "/Volumes/main/default/myvol/perf")

    assert result == set()


# ── volume_dir_path ───────────────────────────────────────────────────────────

def test_volume_dir_path_builds_correct_path():
    path = volume_dir_path("main", "default", "myvol", "performance_net")
    assert path == "/Volumes/main/default/myvol/performance_net"
```

**Step 3: Run tests to verify they fail**

```bash
pytest tests/test_upload_to_volume.py -v
```

Expected: `ModuleNotFoundError: No module named 'upload_to_volume'` — correct, the script doesn't exist yet.

**Step 4: Commit**

```bash
git add tests/
git commit -m "test: add failing tests for upload_to_volume helpers"
```

---

### Task 3: Implement upload_to_volume.py

**Files:**
- Create: `upload_to_volume.py`

**Step 1: Create the script**

Create `upload_to_volume.py` with this content:

```python
"""
upload_to_volume.py — Upload local Parquet files from output/ to a UC Volume.

Run after export_to_parquet.py. For each subfolder in OUTPUT_DIR:
  1. Lists files already in the matching Volume subfolder.
  2. Uploads only files not already there — existing files are skipped.

Authentication uses the ~/.databrickscfg profile set in PROFILE.
"""

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from tqdm import tqdm


# =========================
# === CONFIGURATION
# =========================
PROFILE    = "DEV"          # ~/.databrickscfg profile name
CATALOG    = "main"         # Unity Catalog catalog
SCHEMA     = "default"      # Unity Catalog schema
VOLUME     = "my_volume"    # UC Volume — must already exist
OUTPUT_DIR = Path("output") # local root written by export_to_parquet.py
# =========================

os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", PROFILE)


# =========================
# === HELPERS
# =========================
def volume_dir_path(catalog: str, schema: str, volume: str, subfolder: str) -> str:
    """Build the /Volumes/... path for a subfolder."""
    return f"/Volumes/{catalog}/{schema}/{volume}/{subfolder}"


def get_remote_files(client: WorkspaceClient, volume_dir: str) -> set:
    """
    Return the set of filenames already present in volume_dir.
    Returns an empty set if the directory does not exist yet.
    """
    try:
        items = client.files.list_directory_contents(volume_dir)
        return {Path(item.path).name for item in items if not item.is_directory}
    except NotFound:
        return set()


# =========================
# === MAIN PIPELINE
# =========================
def main():
    client = WorkspaceClient(profile=PROFILE)

    subfolders = sorted(d for d in OUTPUT_DIR.iterdir() if d.is_dir())

    if not subfolders:
        print(f"No subfolders found in {OUTPUT_DIR}. Run export_to_parquet.py first.")
        return

    for subdir in subfolders:
        vol_dir = volume_dir_path(CATALOG, SCHEMA, VOLUME, subdir.name)

        print(f"\n=== {subdir.name} ===")
        print(f"  Volume path : {vol_dir}")

        remote = get_remote_files(client, vol_dir)
        local_files = sorted(subdir.glob("*.parquet"))

        to_upload = [f for f in local_files if f.name not in remote]
        to_skip   = [f for f in local_files if f.name in remote]

        print(f"  Local: {len(local_files)} | Remote: {len(remote)} | "
              f"To upload: {len(to_upload)} | To skip: {len(to_skip)}")

        for f in tqdm(to_upload, desc=f"  Uploading", unit="file", leave=False):
            with open(f, "rb") as fh:
                client.files.upload(f"{vol_dir}/{f.name}", fh, overwrite=False)
            tqdm.write(f"  UPLOADED : {f.name}")

        for f in to_skip:
            print(f"  SKIPPED  : {f.name} (already exists)")

        print(f"  Done — {len(to_upload)} uploaded, {len(to_skip)} skipped.")


# =========================
# === ENTRY POINT
# =========================
if __name__ == "__main__":
    main()
```

**Step 2: Run tests to verify they pass**

```bash
pytest tests/test_upload_to_volume.py -v
```

Expected output:
```
PASSED tests/test_upload_to_volume.py::test_get_remote_files_returns_filenames_when_dir_exists
PASSED tests/test_upload_to_volume.py::test_get_remote_files_excludes_subdirectories
PASSED tests/test_upload_to_volume.py::test_get_remote_files_returns_empty_set_when_not_found
PASSED tests/test_upload_to_volume.py::test_volume_dir_path_builds_correct_path
4 passed
```

**Step 3: Commit**

```bash
git add upload_to_volume.py
git commit -m "feat: add upload_to_volume.py — land output/ Parquet files in UC Volume"
```

---

### Task 4: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Add the new script to CLAUDE.md**

Add a section for `upload_to_volume.py` under the existing "Usage" section. The key facts to document:

- Run `python upload_to_volume.py` after `export_to_parquet.py`
- Configure `PROFILE`, `CATALOG`, `SCHEMA`, `VOLUME` at the top of the script
- Requires `~/.databrickscfg` with the named profile
- Skips files already present in the Volume; safe to re-run
- Tests: `pytest tests/`

**Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md with upload_to_volume usage"
```

---

### Task 5: Manual smoke test (requires Databricks access)

This task cannot be automated — it verifies the real upload works end-to-end.

**Step 1: Configure the script**

Open `upload_to_volume.py` and set `CATALOG`, `SCHEMA`, `VOLUME` to your actual values. Verify the profile name in `PROFILE` matches an entry in `~/.databrickscfg`.

**Step 2: Ensure output/ has data**

If `output/` is empty, run:
```bash
# note: requires TDV ODBC connection
python export_to_parquet.py
```
Or manually place a test Parquet file: `output/test_folder/test_2026-01-31.parquet`.

**Step 3: Run the upload**

```bash
python upload_to_volume.py
```

Expected output (first run):
```
=== performance_net ===
  Volume path : /Volumes/main/default/my_volume/performance_net
  Local: 5 | Remote: 0 | To upload: 5 | To skip: 0
  UPLOADED : performance_net_2026-01-31.parquet
  ...
  Done — 5 uploaded, 0 skipped.
```

Expected output (second run, same files):
```
=== performance_net ===
  ...
  SKIPPED  : performance_net_2026-01-31.parquet (already exists)
  Done — 0 uploaded, 5 skipped.
```

**Step 4: Verify in Databricks UI**

Navigate to: `Catalog > {CATALOG} > {SCHEMA} > Volumes > {VOLUME}` and confirm the files are present.
