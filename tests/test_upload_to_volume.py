from unittest.mock import MagicMock
import pytest

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
