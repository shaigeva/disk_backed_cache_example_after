"""Tests for SQLite connection setup."""

import os
import sqlite3
from pathlib import Path

import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SimpleModel(CacheableModel):
    """Simple model for connection tests."""

    schema_version: str = "1.0.0"
    data: str


def test_creates_database_file(db_path: str) -> None:
    """Database file should be created on initialization."""
    # Skip for in-memory database
    if db_path == ":memory:":
        return

    cache = DiskBackedCache(
        db_path=db_path,
        model=SimpleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    assert os.path.exists(db_path)
    cache.close()


def test_creates_table_with_schema(db_path: str) -> None:
    """Table should be created with correct schema."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SimpleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Query table schema
    cursor = cache._conn.execute("PRAGMA table_info(cache)")  # type: ignore[attr-defined]
    columns = {row[1]: row[2] for row in cursor.fetchall()}

    assert "key" in columns
    assert "value" in columns
    assert "timestamp" in columns
    assert "schema_version" in columns
    assert "size" in columns

    assert columns["key"] == "TEXT"
    assert columns["value"] == "TEXT"
    assert columns["timestamp"] == "REAL"
    assert columns["schema_version"] == "TEXT"
    assert columns["size"] == "INTEGER"

    cache.close()


def test_wal_mode_enabled(db_path: str) -> None:
    """WAL mode should be enabled."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SimpleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cursor = cache._conn.execute("PRAGMA journal_mode")  # type: ignore[attr-defined]
    mode = cursor.fetchone()[0]

    assert mode.lower() == "wal"

    cache.close()


def test_close_connection_works(db_path: str) -> None:
    """close() method should close the database connection."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SimpleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.close()

    # Attempting to execute a query after close should fail
    with pytest.raises(sqlite3.ProgrammingError):
        cache._conn.execute("SELECT 1")  # type: ignore[attr-defined]


def test_creates_parent_directory(tmp_path: Path) -> None:
    """Parent directory should be created if it doesn't exist."""
    nested_path = str(tmp_path / "nested" / "dir" / "cache.db")

    cache = DiskBackedCache(
        db_path=nested_path,
        model=SimpleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    assert os.path.exists(os.path.dirname(nested_path))
    assert os.path.exists(nested_path)

    cache.close()
