from unittest.mock import MagicMock
import pytest

from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import VolumeType

from upload_to_volume import ensure_volume_exists, get_remote_files, volume_dir_path


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
    path = volume_dir_path("main", "default", "performance_net")
    assert path == "/Volumes/main/default/performance_net"


# ── ensure_volume_exists ──────────────────────────────────────────────────────

def test_ensure_volume_exists_does_not_create_when_volume_exists():
    """If volumes.read succeeds, volumes.create should NOT be called."""
    client = MagicMock()

    ensure_volume_exists(client, "main", "default", "perf")

    client.volumes.read.assert_called_once_with("main.default.perf")
    client.volumes.create.assert_not_called()


def test_ensure_volume_exists_creates_volume_when_not_found():
    """If volumes.read raises NotFound, volumes.create should be called."""
    client = MagicMock()
    client.volumes.read.side_effect = NotFound("not found")

    ensure_volume_exists(client, "main", "default", "perf")

    client.volumes.create.assert_called_once_with(
        catalog_name="main",
        schema_name="default",
        name="perf",
        volume_type=VolumeType.MANAGED,
    )
