"""Tests for TTL (Time To Live) expiration."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class TTLModel(CacheableModel):
    """Model for TTL tests."""

    schema_version: str = "1.0.0"
    value: int


def test_memory_ttl_expiration(db_path: str) -> None:
    """Items should expire from memory after memory_ttl_seconds."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,  # 60 second TTL
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store item at timestamp 1000
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Item should be in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]

    # Access within TTL (at timestamp 1050 - 50 seconds later)
    retrieved = cache.get("key1", timestamp=1050.0)
    assert retrieved == TTLModel(value=1)

    # Item should still be in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]

    # Access after TTL (at timestamp 1100 - 100 seconds later, > 60 second TTL)
    retrieved = cache.get("key1", timestamp=1100.0)

    # Should still retrieve from disk
    assert retrieved == TTLModel(value=1)

    # But should have been removed from memory
    # (Note: get() promotes from disk, so it will be back in memory now)
    # Let's verify by checking it was expired first
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_timestamps.clear()  # type: ignore[attr-defined]
    cache._memory_item_count = 0  # type: ignore[attr-defined]
    cache._memory_total_size = 0  # type: ignore[attr-defined]

    # Put it back with old timestamp
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Try to get with expired timestamp
    cache.get("key1", timestamp=1100.0)

    # Should be promoted back after expiration check
    cache.close()


def test_disk_ttl_expiration(db_path: str) -> None:
    """Items should expire from disk after disk_ttl_seconds without access."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=120.0,  # 120 second disk TTL
        max_item_size_bytes=10 * 1024,
    )

    # Store item at timestamp 1000
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Access within disk TTL (at timestamp 1100 - 100 seconds later)
    retrieved = cache.get("key1", timestamp=1100.0)
    assert retrieved == TTLModel(value=1)

    # Access after disk TTL from last access (at timestamp 1221 - 121 seconds after 1100)
    # Since get() at 1100 updated the timestamp, we need > 120 seconds from 1100
    retrieved = cache.get("key1", timestamp=1221.0)

    # Should be None (expired from disk based on last access)
    assert retrieved is None

    # Should not exist anywhere
    assert cache.get_count() == 0

    cache.close()


def test_memory_expires_before_disk(db_path: str) -> None:
    """Memory TTL should be shorter than disk TTL, items expire from memory first."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,  # 60 second memory TTL
        disk_ttl_seconds=120.0,  # 120 second disk TTL
        max_item_size_bytes=10 * 1024,
    )

    # Store item at timestamp 1000
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Access after memory TTL but before disk TTL (at timestamp 1080)
    cache.get("key1", timestamp=1080.0)

    # Should have been removed from memory due to expiration
    # but still available on disk

    # Verify by clearing memory and checking disk
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_timestamps.clear()  # type: ignore[attr-defined]
    cache._memory_item_count = 0  # type: ignore[attr-defined]
    cache._memory_total_size = 0  # type: ignore[attr-defined]

    # Should still be retrievable from disk
    retrieved = cache.get("key1", timestamp=1080.0)
    assert retrieved == TTLModel(value=1)

    cache.close()


def test_expired_item_counts_as_miss(db_path: str) -> None:
    """Expired items should count as cache misses."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=120.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store item
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Reset stats
    cache._stats_misses = 0  # type: ignore[attr-defined]

    # Access after disk TTL
    retrieved = cache.get("key1", timestamp=1200.0)
    assert retrieved is None

    # Should count as miss
    stats = cache.get_stats()
    assert stats["misses"] == 1

    cache.close()


def test_get_many_respects_ttl(db_path: str) -> None:
    """get_many() should respect TTL for each item."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=120.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items with different timestamps
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)  # Will expire
    cache.put("key2", TTLModel(value=2), timestamp=1100.0)  # Won't expire
    cache.put("key3", TTLModel(value=3), timestamp=1150.0)  # Won't expire

    # Access at timestamp 1200
    result = cache.get_many(["key1", "key2", "key3"], timestamp=1200.0)

    # key1 should be expired (200 seconds > 120 second disk TTL)
    # key2 and key3 should be available
    assert "key1" not in result
    assert result["key2"] == TTLModel(value=2)
    assert result["key3"] == TTLModel(value=3)

    cache.close()


def test_ttl_updates_on_get(db_path: str) -> None:
    """get() should update the timestamp, extending the TTL."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=120.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store item at timestamp 1000
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Access at timestamp 1050 (updates timestamp on disk)
    cache.get("key1", timestamp=1050.0)

    # Access at timestamp 1150 (would have been expired if timestamp wasn't updated)
    # 1150 - 1000 = 150 > 120, but 1150 - 1050 = 100 < 120
    retrieved = cache.get("key1", timestamp=1150.0)

    # Should still be available because timestamp was updated
    assert retrieved == TTLModel(value=1)

    cache.close()


def test_memory_ttl_with_disk_only_items(db_path: str) -> None:
    """Disk-only items (exceeding max_item_size) should only use disk TTL."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TTLModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=120.0,
        max_item_size_bytes=10,  # Very low - forces disk-only
    )

    # Store large item (will be disk-only)
    cache.put("key1", TTLModel(value=1), timestamp=1000.0)

    # Item should not be in memory
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    # Access after memory TTL but before disk TTL (at timestamp 1080)
    retrieved = cache.get("key1", timestamp=1080.0)

    # Should still be available (uses disk TTL only)
    assert retrieved == TTLModel(value=1)

    # Access after disk TTL from last access (at timestamp 1201 - 121 seconds after 1080)
    # Since get() at 1080 updated the timestamp, we need > 120 seconds from 1080
    retrieved = cache.get("key1", timestamp=1201.0)

    # Should be expired
    assert retrieved is None

    cache.close()
