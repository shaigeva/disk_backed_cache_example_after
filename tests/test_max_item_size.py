"""Tests for max item size (disk-only storage for large items)."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class LargeModel(CacheableModel):
    """Model for max item size tests."""

    schema_version: str = "1.0.0"
    data: str


def test_large_item_stored_on_disk_only(db_path: str) -> None:
    """Items larger than max_item_size_bytes should be stored on disk only."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,  # Low limit - forces disk-only storage
    )

    # Create a large item (will exceed 50 bytes when serialized)
    large_data = "x" * 100
    large_item = LargeModel(data=large_data)

    cache.put("large_key", large_item)

    # Item should not be in memory
    assert "large_key" not in cache._memory_cache  # type: ignore[attr-defined]

    # But should be on disk
    stats = cache.get_stats()
    assert stats["current_disk_items"] == 1

    # Should still be retrievable
    retrieved = cache.get("large_key")
    assert retrieved == large_item

    cache.close()


def test_small_item_stored_in_memory_and_disk(db_path: str) -> None:
    """Items smaller than max_item_size_bytes should be in both memory and disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=100,  # Higher limit
    )

    # Create a small item
    small_item = LargeModel(data="small")

    cache.put("small_key", small_item)

    # Item should be in memory
    assert "small_key" in cache._memory_cache  # type: ignore[attr-defined]

    # And on disk
    stats = cache.get_stats()
    assert stats["current_disk_items"] == 1

    cache.close()


def test_large_item_not_promoted_to_memory(db_path: str) -> None:
    """Large items retrieved from disk should not be promoted to memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,
    )

    # Store large item
    large_data = "x" * 100
    large_item = LargeModel(data=large_data)
    cache.put("large_key", large_item)

    # Clear memory (simulating eviction or restart)
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_timestamps.clear()  # type: ignore[attr-defined]
    cache._memory_item_count = 0  # type: ignore[attr-defined]
    cache._memory_total_size = 0  # type: ignore[attr-defined]

    # Retrieve from disk
    retrieved = cache.get("large_key")
    assert retrieved == large_item

    # Should still not be in memory
    assert "large_key" not in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_overwrite_small_with_large_removes_from_memory(db_path: str) -> None:
    """Overwriting a small item with a large one should remove it from memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,
    )

    # Store small item
    small_item = LargeModel(data="small")
    cache.put("key1", small_item)

    # Should be in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]

    # Overwrite with large item
    large_data = "x" * 100
    large_item = LargeModel(data=large_data)
    cache.put("key1", large_item)

    # Should be removed from memory
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    # But still on disk
    assert cache.get("key1") == large_item

    cache.close()


def test_overwrite_large_with_small_adds_to_memory(db_path: str) -> None:
    """Overwriting a large item with a small one should add it to memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,
    )

    # Store large item
    large_data = "x" * 100
    large_item = LargeModel(data=large_data)
    cache.put("key1", large_item)

    # Should not be in memory
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    # Overwrite with small item
    small_item = LargeModel(data="small")
    cache.put("key1", small_item)

    # Should now be in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_put_many_respects_max_item_size(db_path: str) -> None:
    """put_many() should respect max_item_size_bytes."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,
    )

    # Mix of small and large items
    items = {
        "small1": LargeModel(data="s1"),
        "large1": LargeModel(data="x" * 100),
        "small2": LargeModel(data="s2"),
        "large2": LargeModel(data="y" * 100),
    }

    cache.put_many(items)

    # Small items should be in memory
    assert "small1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "small2" in cache._memory_cache  # type: ignore[attr-defined]

    # Large items should not be in memory
    assert "large1" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "large2" not in cache._memory_cache  # type: ignore[attr-defined]

    # All should be on disk
    assert cache.get_count() == 4

    cache.close()


def test_large_items_do_not_affect_memory_limits(db_path: str) -> None:
    """Large items should not count toward memory limits."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=2,  # Very low memory limit
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,
    )

    # Add 2 small items (fill memory)
    cache.put("small1", LargeModel(data="s1"))
    cache.put("small2", LargeModel(data="s2"))

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 2

    # Add large item (should not trigger memory eviction)
    cache.put("large1", LargeModel(data="x" * 100))

    # Memory should still have 2 items
    stats = cache.get_stats()
    assert stats["current_memory_items"] == 2
    assert stats["memory_evictions"] == 0

    # All 3 should be on disk
    assert cache.get_count() == 3

    cache.close()
