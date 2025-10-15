"""Tests for LRU eviction (memory and disk)."""

import time

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class EvictionModel(CacheableModel):
    """Model for eviction tests."""

    schema_version: str = "1.0.0"
    value: int


def test_memory_eviction_when_count_exceeds_limit(db_path: str) -> None:
    """Memory should evict items when count exceeds max_memory_items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items (at limit)
    cache.put("key1", EvictionModel(value=1))
    cache.put("key2", EvictionModel(value=2))
    cache.put("key3", EvictionModel(value=3))

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 3
    assert stats["memory_evictions"] == 0

    # Add 4th item (should trigger eviction)
    cache.put("key4", EvictionModel(value=4))

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 3  # Still at limit
    assert stats["memory_evictions"] == 1  # One eviction

    cache.close()


def test_memory_eviction_removes_lru_item(db_path: str) -> None:
    """Memory eviction should remove least recently used item."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items with different timestamps
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    time.sleep(0.01)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    time.sleep(0.01)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Add 4th item (should evict key1 - oldest)
    cache.put("key4", EvictionModel(value=4), timestamp=4000.0)

    # key1 should be evicted from memory (but still on disk)
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key4" in cache._memory_cache  # type: ignore[attr-defined]

    # key1 should still be on disk
    assert cache.exists("key1")

    cache.close()


def test_memory_eviction_updates_access_time_on_get(db_path: str) -> None:
    """get() should update memory timestamp for LRU tracking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    time.sleep(0.01)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    time.sleep(0.01)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Access key1 (should update its timestamp)
    cache.get("key1", timestamp=4000.0)

    # Add 4th item (should evict key2 - now oldest)
    cache.put("key4", EvictionModel(value=4), timestamp=5000.0)

    # key2 should be evicted (key1 was refreshed)
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key4" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_tie_breaking_alphabetical(db_path: str) -> None:
    """When timestamps equal, evict alphabetically smallest key."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items with same timestamp
    same_timestamp = 1000.0
    cache.put("key_c", EvictionModel(value=3), timestamp=same_timestamp)
    cache.put("key_a", EvictionModel(value=1), timestamp=same_timestamp)
    cache.put("key_b", EvictionModel(value=2), timestamp=same_timestamp)

    # Add 4th item (should evict key_a - alphabetically first)
    cache.put("key_d", EvictionModel(value=4), timestamp=2000.0)

    # key_a should be evicted
    assert "key_a" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key_b" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key_c" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key_d" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_multiple_items(db_path: str) -> None:
    """Should evict multiple items if needed to get under limit."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Use put_many to add 3 more items (should evict 3 items)
    cache.put_many(
        {
            "key4": EvictionModel(value=4),
            "key5": EvictionModel(value=5),
            "key6": EvictionModel(value=6),
        },
        timestamp=4000.0,
    )

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 3
    assert stats["memory_evictions"] == 3

    # Oldest 3 should be evicted
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key4" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key5" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key6" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_keeps_items_on_disk(db_path: str) -> None:
    """Evicted items should remain on disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items (one will be evicted)
    cache.put("key1", EvictionModel(value=1))
    cache.put("key2", EvictionModel(value=2))
    cache.put("key3", EvictionModel(value=3))

    # key1 should be evicted from memory
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    # But still on disk
    stats = cache.get_stats()
    assert stats["current_disk_items"] == 3

    # Can still retrieve it
    retrieved = cache.get("key1")
    assert retrieved == EvictionModel(value=1)

    cache.close()


def test_memory_eviction_after_promotion_from_disk(db_path: str) -> None:
    """Promoting item from disk to memory should trigger eviction if needed."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # key1 should be evicted from memory
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" in cache._memory_cache  # type: ignore[attr-defined]

    # Retrieve key1 (should promote to memory and evict key2)
    cache.get("key1", timestamp=4000.0)

    # key2 should now be evicted (key1 promoted with newest timestamp)
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_no_eviction_under_limit(db_path: str) -> None:
    """No eviction should occur when under limit."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 5 items (well under limit)
    for i in range(5):
        cache.put(f"key{i}", EvictionModel(value=i))

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 5
    assert stats["memory_evictions"] == 0

    # All should still be in memory
    for i in range(5):
        assert f"key{i}" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_when_size_exceeds_limit(db_path: str) -> None:
    """Memory should evict items when size exceeds max_memory_size_bytes."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,  # High count limit
        max_memory_size_bytes=100,  # Low size limit
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items until we exceed size limit
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)

    # Add more items (should trigger size-based eviction)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Should have evicted some items to stay under size limit
    assert cache._memory_total_size <= 100  # type: ignore[attr-defined]
    stats = cache.get_stats()
    assert stats["memory_evictions"] > 0

    cache.close()


def test_memory_eviction_size_removes_lru_item(db_path: str) -> None:
    """Size-based eviction should remove least recently used item."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,  # High count limit
        max_memory_size_bytes=80,  # Low size limit - 2 items will exceed
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items with different timestamps
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Oldest items should be evicted
    stats = cache.get_stats()
    assert stats["memory_evictions"] > 0

    # Should be under size limit
    assert cache._memory_total_size <= 80  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_size_tie_breaking(db_path: str) -> None:
    """Size-based eviction should use alphabetical tie-breaking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=100,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items with same timestamp
    same_timestamp = 1000.0
    cache.put("key_c", EvictionModel(value=3), timestamp=same_timestamp)
    cache.put("key_a", EvictionModel(value=1), timestamp=same_timestamp)
    cache.put("key_b", EvictionModel(value=2), timestamp=same_timestamp)

    # key_a should be evicted first (alphabetically first among same timestamps)
    assert "key_a" not in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_memory_eviction_size_keeps_items_on_disk(db_path: str) -> None:
    """Size-based eviction should keep items on disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=100,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Some items should be evicted from memory
    stats = cache.get_stats()
    assert stats["memory_evictions"] > 0

    # But all should still be on disk
    assert stats["current_disk_items"] == 3

    # Can still retrieve them (use timestamp within TTL)
    assert cache.get("key1", timestamp=4000.0) == EvictionModel(value=1)
    assert cache.get("key2", timestamp=4000.0) == EvictionModel(value=2)
    assert cache.get("key3", timestamp=4000.0) == EvictionModel(value=3)

    cache.close()


def test_memory_eviction_count_and_size_both_enforced(db_path: str) -> None:
    """Both count and size limits should be enforced."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=2,  # Low count limit
        max_memory_size_bytes=1024 * 1024,  # High size limit
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items (should trigger count-based eviction)
    cache.put("key1", EvictionModel(value=1))
    cache.put("key2", EvictionModel(value=2))
    cache.put("key3", EvictionModel(value=3))

    stats = cache.get_stats()
    assert stats["current_memory_items"] <= 2  # Count limit enforced
    assert cache._memory_total_size <= 1024 * 1024  # type: ignore[attr-defined]  # Size limit not exceeded

    cache.close()


def test_memory_eviction_no_eviction_when_under_size_limit(db_path: str) -> None:
    """No size-based eviction when under limit."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=10 * 1024 * 1024,  # Very high limit
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add several items
    for i in range(10):
        cache.put(f"key{i}", EvictionModel(value=i))

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 10
    assert stats["memory_evictions"] == 0

    cache.close()


def test_disk_eviction_when_count_exceeds_limit(db_path: str) -> None:
    """Disk should evict items when count exceeds max_disk_items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,  # Low disk limit
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items (at limit)
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    stats = cache.get_stats()
    assert stats["current_disk_items"] == 3
    assert stats["disk_evictions"] == 0

    # Add 4th item (should trigger disk eviction)
    cache.put("key4", EvictionModel(value=4), timestamp=4000.0)

    stats = cache.get_stats()
    assert stats["current_disk_items"] == 3  # Still at limit
    assert stats["disk_evictions"] == 1  # One eviction

    cache.close()


def test_disk_eviction_removes_lru_item(db_path: str) -> None:
    """Disk eviction should remove least recently used item."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items with different timestamps
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Add 4th item (should evict key1 - oldest on disk)
    cache.put("key4", EvictionModel(value=4), timestamp=4000.0)

    # key1 should be evicted from disk
    assert cache.get("key1", timestamp=5000.0) is None
    assert cache.get("key2", timestamp=5000.0) == EvictionModel(value=2)
    assert cache.get("key3", timestamp=5000.0) == EvictionModel(value=3)
    assert cache.get("key4", timestamp=5000.0) == EvictionModel(value=4)

    cache.close()


def test_disk_eviction_tie_breaking_alphabetical(db_path: str) -> None:
    """Disk eviction should use alphabetical tie-breaking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items with same timestamp
    same_timestamp = 1000.0
    cache.put("key_c", EvictionModel(value=3), timestamp=same_timestamp)
    cache.put("key_a", EvictionModel(value=1), timestamp=same_timestamp)
    cache.put("key_b", EvictionModel(value=2), timestamp=same_timestamp)

    # Add 4th item (should evict key_a - alphabetically first)
    cache.put("key_d", EvictionModel(value=4), timestamp=2000.0)

    # key_a should be evicted
    assert cache.get("key_a", timestamp=3000.0) is None
    assert cache.get("key_b", timestamp=3000.0) == EvictionModel(value=2)
    assert cache.get("key_c", timestamp=3000.0) == EvictionModel(value=3)
    assert cache.get("key_d", timestamp=3000.0) == EvictionModel(value=4)

    cache.close()


def test_disk_eviction_cascades_to_memory(db_path: str) -> None:
    """Disk eviction should also remove items from memory (cascading)."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,  # High memory limit
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=2,  # Low disk limit
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 2 items (will be in both memory and disk)
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)

    # Verify they're in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]

    # Add 3rd item (should evict key1 from disk AND memory)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # key1 should be evicted from both memory and disk
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]
    assert cache.get("key1") is None

    # key2 and key3 should still be in memory
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key3" in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_disk_eviction_multiple_items(db_path: str) -> None:
    """Should evict multiple items from disk if needed."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Use put_many to add 3 more items (should evict 3 items)
    cache.put_many(
        {
            "key4": EvictionModel(value=4),
            "key5": EvictionModel(value=5),
            "key6": EvictionModel(value=6),
        },
        timestamp=4000.0,
    )

    stats = cache.get_stats()
    assert stats["current_disk_items"] == 3
    assert stats["disk_evictions"] == 3

    # Oldest 3 should be evicted
    assert cache.get("key1", timestamp=5000.0) is None
    assert cache.get("key2", timestamp=5000.0) is None
    assert cache.get("key3", timestamp=5000.0) is None
    assert cache.get("key4", timestamp=5000.0) == EvictionModel(value=4)
    assert cache.get("key5", timestamp=5000.0) == EvictionModel(value=5)
    assert cache.get("key6", timestamp=5000.0) == EvictionModel(value=6)

    cache.close()


def test_disk_eviction_no_eviction_under_limit(db_path: str) -> None:
    """No disk eviction when under limit."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,  # High limit
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add several items
    for i in range(10):
        cache.put(f"key{i}", EvictionModel(value=i))

    stats = cache.get_stats()
    assert stats["current_disk_items"] == 10
    assert stats["disk_evictions"] == 0

    cache.close()


def test_disk_eviction_when_size_exceeds_limit(db_path: str) -> None:
    """Disk should evict items when size exceeds max_disk_size_bytes."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,  # High count limit
        max_disk_size_bytes=80,  # Low size limit - 2 items will exceed
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items until we exceed size limit
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)
    cache.put("key4", EvictionModel(value=4), timestamp=4000.0)

    # Should have evicted some items to stay under size limit
    assert cache.get_total_size() <= 80
    stats = cache.get_stats()
    assert stats["disk_evictions"] > 0

    cache.close()


def test_disk_eviction_size_removes_lru_item(db_path: str) -> None:
    """Size-based disk eviction should remove least recently used item."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=100,  # Low size limit
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items with different timestamps
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Oldest items should be evicted
    stats = cache.get_stats()
    assert stats["disk_evictions"] > 0

    # Should be under size limit
    assert cache.get_total_size() <= 100

    cache.close()


def test_disk_eviction_size_tie_breaking(db_path: str) -> None:
    """Size-based disk eviction should use alphabetical tie-breaking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=100,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items with same timestamp
    same_timestamp = 1000.0
    cache.put("key_c", EvictionModel(value=3), timestamp=same_timestamp)
    cache.put("key_a", EvictionModel(value=1), timestamp=same_timestamp)
    cache.put("key_b", EvictionModel(value=2), timestamp=same_timestamp)

    # key_a should be evicted first (alphabetically first)
    assert cache.get("key_a", timestamp=2000.0) is None

    cache.close()


def test_disk_eviction_size_cascades_to_memory(db_path: str) -> None:
    """Size-based disk eviction should cascade to memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=80,  # Low size limit
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items (will be in both memory and disk)
    cache.put("key1", EvictionModel(value=1), timestamp=1000.0)
    cache.put("key2", EvictionModel(value=2), timestamp=2000.0)
    cache.put("key3", EvictionModel(value=3), timestamp=3000.0)

    # Some items should be evicted from both
    stats = cache.get_stats()
    assert stats["disk_evictions"] > 0

    # Evicted items should not be in memory
    assert cache.get("key1", timestamp=4000.0) is None  # Evicted from both

    cache.close()


def test_disk_eviction_count_and_size_both_enforced(db_path: str) -> None:
    """Both disk count and size limits should be enforced."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EvictionModel,
        max_memory_items=100,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=2,  # Low count limit
        max_disk_size_bytes=10 * 1024 * 1024,  # High size limit
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add 3 items (should trigger count-based eviction)
    cache.put("key1", EvictionModel(value=1))
    cache.put("key2", EvictionModel(value=2))
    cache.put("key3", EvictionModel(value=3))

    stats = cache.get_stats()
    assert stats["current_disk_items"] <= 2  # Count limit enforced
    assert cache.get_total_size() <= 10 * 1024 * 1024  # Size limit not exceeded

    cache.close()
