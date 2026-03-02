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

    if not OUTPUT_DIR.exists():
        print(f"{OUTPUT_DIR} does not exist. Run export_to_parquet.py first.")
        return

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
