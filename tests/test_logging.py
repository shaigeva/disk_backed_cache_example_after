"""Tests for TRACE level logging."""

import logging
from typing import TYPE_CHECKING

from disk_backed_cache_example.disk_backed_cache import TRACE, CacheableModel, DiskBackedCache

if TYPE_CHECKING:
    import pytest


class LogModel(CacheableModel):
    """Model for logging tests."""

    schema_version: str = "1.0.0"
    value: int


def test_trace_level_is_defined() -> None:
    """TRACE level should be defined below DEBUG."""
    assert TRACE == 5
    assert TRACE < logging.DEBUG
    assert logging.getLevelName(TRACE) == "TRACE"


def test_get_operation_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """get() should log at TRACE level."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.put("key1", LogModel(value=1))
        cache.get("key1")

    # Verify logs were emitted
    assert any("get(key='key1')" in record.message for record in caplog.records)

    cache.close()


def test_put_operation_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """put() should log at TRACE level."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.put("key1", LogModel(value=1))

    # Verify logs were emitted
    assert any("put(key='key1')" in record.message for record in caplog.records)

    cache.close()


def test_delete_operation_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """delete() should log at TRACE level."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    cache.put("key1", LogModel(value=1))

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.delete("key1")

    # Verify logs were emitted
    assert any("delete(key='key1')" in record.message for record in caplog.records)

    cache.close()


def test_memory_hit_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """Memory hit should be logged."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    cache.put("key1", LogModel(value=1))

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.get("key1")

    # Verify memory hit was logged
    assert any("memory hit" in record.message for record in caplog.records)

    cache.close()


def test_disk_hit_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """Disk hit should be logged."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    cache.put("key1", LogModel(value=1))

    # Remove from memory to force disk hit
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_timestamps.clear()  # type: ignore[attr-defined]

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.get("key1")

    # Verify disk hit was logged
    assert any("disk hit" in record.message for record in caplog.records)

    cache.close()


def test_miss_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """Cache miss should be logged."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.get("nonexistent")

    # Verify miss was logged
    assert any("miss (not found)" in record.message for record in caplog.records)

    cache.close()


def test_ttl_expiration_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """TTL expiration should be logged."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=120.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Store item at timestamp 1000
    cache.put("key1", LogModel(value=1), timestamp=1000.0)

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        # Access at timestamp 1200 (expired from memory)
        cache.get("key1", timestamp=1200.0)

    # Verify TTL expiration was logged
    assert any("expired from memory" in record.message for record in caplog.records)

    cache.close()


def test_schema_mismatch_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """Schema version mismatch should be logged."""

    # Create cache with version 1.0.0
    class V1Model(CacheableModel):
        schema_version: str = "1.0.0"
        value: int

    cache_v1 = DiskBackedCache(
        db_path=db_path,
        model=V1Model,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache_v1.put("key1", V1Model(value=1))
    cache_v1.close()

    # Create cache with version 2.0.0
    class V2Model(CacheableModel):
        schema_version: str = "2.0.0"
        value: int

    cache_v2 = DiskBackedCache(
        db_path=db_path,
        model=V2Model,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Remove from memory to force disk read
    cache_v2._memory_cache.clear()  # type: ignore[attr-defined]
    cache_v2._memory_timestamps.clear()  # type: ignore[attr-defined]

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        result = cache_v2.get("key1")

    # Verify schema mismatch was logged
    assert result is None
    assert any("schema version mismatch" in record.message for record in caplog.records)

    cache_v2.close()


def test_eviction_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """Eviction should be logged."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Add items to trigger eviction
    cache.put("key1", LogModel(value=1))
    cache.put("key2", LogModel(value=2))

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.put("key3", LogModel(value=3))  # This should trigger eviction

    # Verify eviction was logged
    assert any("evicting from memory" in record.message for record in caplog.records)

    cache.close()


def test_batch_operations_log(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """Batch operations should log at TRACE level."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.put_many({"key1": LogModel(value=1), "key2": LogModel(value=2)})
        cache.get_many(["key1", "key2"])
        cache.delete_many(["key1", "key2"])

    # Verify batch operations were logged
    assert any("put_many(count=2)" in record.message for record in caplog.records)
    assert any("get_many(count=2)" in record.message for record in caplog.records)
    assert any("delete_many(count=2)" in record.message for record in caplog.records)

    cache.close()


def test_clear_operation_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """clear() should log at TRACE level."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    cache.put("key1", LogModel(value=1))

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.clear()

    # Verify clear was logged
    assert any("clear()" in record.message for record in caplog.records)

    cache.close()


def test_exists_operation_logs(db_path: str, caplog: "pytest.LogCaptureFixture") -> None:  # type: ignore[name-defined]
    """exists() should log at TRACE level."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LogModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Set logger to TRACE level
    logger = logging.getLogger("disk_backed_cache_example.disk_backed_cache")
    logger.setLevel(TRACE)

    cache.put("key1", LogModel(value=1))

    # Capture TRACE level logs
    with caplog.at_level(TRACE, logger="disk_backed_cache_example.disk_backed_cache"):
        cache.exists("key1")

    # Verify exists was logged
    assert any("exists(key='key1')" in record.message for record in caplog.records)

    cache.close()
