"""Tests for statistics tracking."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class StatsModel(CacheableModel):
    """Model for statistics tests."""

    schema_version: str = "1.0.0"
    value: int


def test_get_stats_initial_state(db_path: str) -> None:
    """Initial statistics should be all zeros."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=StatsModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    stats = cache.get_stats()

    assert stats["memory_hits"] == 0
    assert stats["disk_hits"] == 0
    assert stats["misses"] == 0
    assert stats["memory_evictions"] == 0
    assert stats["disk_evictions"] == 0
    assert stats["total_puts"] == 0
    assert stats["total_gets"] == 0
    assert stats["total_deletes"] == 0
    assert stats["current_memory_items"] == 0
    assert stats["current_disk_items"] == 0

    cache.close()


def test_stats_track_memory_hits(db_path: str) -> None:
    """Memory hits should be tracked."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=StatsModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", StatsModel(value=1))
    cache.get("key1")  # Memory hit
    cache.get("key1")  # Memory hit

    stats = cache.get_stats()
    assert stats["memory_hits"] == 2
    assert stats["disk_hits"] == 0
    assert stats["misses"] == 0

    cache.close()


def test_stats_track_disk_hits(db_path: str) -> None:
    """Disk hits should be tracked."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=StatsModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", StatsModel(value=1))
    cache._memory_cache.clear()  # type: ignore[attr-defined]

    cache.get("key1")  # Disk hit

    stats = cache.get_stats()
    assert stats["memory_hits"] == 0
    assert stats["disk_hits"] == 1
    assert stats["misses"] == 0

    cache.close()


def test_stats_track_misses(db_path: str) -> None:
    """Misses should be tracked."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=StatsModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.get("nonexistent1")
    cache.get("nonexistent2")

    stats = cache.get_stats()
    assert stats["memory_hits"] == 0
    assert stats["disk_hits"] == 0
    assert stats["misses"] == 2

    cache.close()


def test_stats_track_operation_counts(db_path: str) -> None:
    """Operation counts should be tracked."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=StatsModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", StatsModel(value=1))
    cache.put("key2", StatsModel(value=2))
    cache.get("key1")
    cache.get("key2")
    cache.get("key3")  # Miss
    cache.delete("key1")

    stats = cache.get_stats()
    assert stats["total_puts"] == 2
    assert stats["total_gets"] == 3
    assert stats["total_deletes"] == 1

    cache.close()


def test_stats_track_current_counts(db_path: str) -> None:
    """Current item counts should be tracked."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=StatsModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", StatsModel(value=1))
    cache.put("key2", StatsModel(value=2))

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 2
    assert stats["current_disk_items"] == 2

    cache.delete("key1")

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 1
    assert stats["current_disk_items"] == 1

    cache.close()
