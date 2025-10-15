"""Tests for clear operation."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class ClearModel(CacheableModel):
    """Model for clear tests."""

    schema_version: str = "1.0.0"
    value: int


def test_clear_removes_all_items_from_memory(db_path: str) -> None:
    """clear() should remove all items from memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ClearModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", ClearModel(value=1))
    cache.put("key2", ClearModel(value=2))
    cache.put("key3", ClearModel(value=3))

    # Verify items are in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" in cache._memory_cache  # type: ignore[attr-defined]

    # Clear
    cache.clear()

    # Memory should be empty
    assert len(cache._memory_cache) == 0  # type: ignore[attr-defined]
    assert len(cache._memory_timestamps) == 0  # type: ignore[attr-defined]

    cache.close()


def test_clear_removes_all_items_from_disk(db_path: str) -> None:
    """clear() should remove all items from disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ClearModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", ClearModel(value=1))
    cache.put("key2", ClearModel(value=2))
    cache.put("key3", ClearModel(value=3))

    # Verify items are on disk
    assert cache.get_count() == 3

    # Clear
    cache.clear()

    # Disk should be empty
    assert cache.get_count() == 0

    # Items should not be retrievable
    assert cache.get("key1") is None
    assert cache.get("key2") is None
    assert cache.get("key3") is None

    cache.close()


def test_clear_resets_counts(db_path: str) -> None:
    """clear() should reset memory counts."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ClearModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", ClearModel(value=1))
    cache.put("key2", ClearModel(value=2))

    stats_before = cache.get_stats()
    assert stats_before["current_memory_items"] == 2
    assert stats_before["current_disk_items"] == 2

    # Clear
    cache.clear()

    # Counts should be reset
    stats_after = cache.get_stats()
    assert stats_after["current_memory_items"] == 0
    assert stats_after["current_disk_items"] == 0

    cache.close()


def test_clear_on_empty_cache(db_path: str) -> None:
    """clear() on empty cache should be a no-op."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ClearModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Clear empty cache (should not raise)
    cache.clear()

    # Should still be empty
    assert cache.get_count() == 0
    stats = cache.get_stats()
    assert stats["current_memory_items"] == 0
    assert stats["current_disk_items"] == 0

    cache.close()


def test_operations_after_clear(db_path: str) -> None:
    """Can add items after clear."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ClearModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", ClearModel(value=1))
    cache.put("key2", ClearModel(value=2))

    # Clear
    cache.clear()

    # Add new items
    cache.put("key3", ClearModel(value=3))
    cache.put("key4", ClearModel(value=4))

    # New items should be retrievable
    assert cache.get("key3") == ClearModel(value=3)
    assert cache.get("key4") == ClearModel(value=4)

    # Old items should not be retrievable
    assert cache.get("key1") is None
    assert cache.get("key2") is None

    # Count should be 2
    assert cache.get_count() == 2

    cache.close()


def test_clear_does_not_affect_statistics(db_path: str) -> None:
    """clear() should not affect statistics counters."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ClearModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", ClearModel(value=1))
    cache.put("key2", ClearModel(value=2))

    # Get items (to increment hit counters)
    cache.get("key1")
    cache.get("key2")

    stats_before = cache.get_stats()
    total_puts_before = stats_before["total_puts"]
    total_gets_before = stats_before["total_gets"]
    memory_hits_before = stats_before["memory_hits"]

    # Clear
    cache.clear()

    # Statistics should not change
    stats_after = cache.get_stats()
    assert stats_after["total_puts"] == total_puts_before
    assert stats_after["total_gets"] == total_gets_before
    assert stats_after["memory_hits"] == memory_hits_before

    # But counts should be zero
    assert stats_after["current_memory_items"] == 0
    assert stats_after["current_disk_items"] == 0

    cache.close()
