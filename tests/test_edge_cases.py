"""Tests for edge cases and error handling."""

import os
import tempfile

import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class EdgeModel(CacheableModel):
    """Model for edge case tests."""

    schema_version: str = "1.0.0"
    value: int


def test_get_from_empty_cache(db_path: str) -> None:
    """get() on empty cache should return None."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Get from empty cache
    result = cache.get("nonexistent")

    # Should return None without error
    assert result is None

    # Statistics should show a miss
    stats = cache.get_stats()
    assert stats["misses"] == 1
    assert stats["memory_hits"] == 0
    assert stats["disk_hits"] == 0

    cache.close()


def test_delete_from_empty_cache(db_path: str) -> None:
    """delete() on empty cache should not raise error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Delete from empty cache (should not error)
    cache.delete("nonexistent")

    # Statistics should show a delete
    stats = cache.get_stats()
    assert stats["total_deletes"] == 1

    cache.close()


def test_delete_nonexistent_key(db_path: str) -> None:
    """delete() for key that doesn't exist should not raise error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add some items
    cache.put("key1", EdgeModel(value=1))
    cache.put("key2", EdgeModel(value=2))

    # Delete nonexistent key (should not error)
    cache.delete("key999")

    # Other items should still exist
    assert cache.exists("key1")
    assert cache.exists("key2")

    cache.close()


def test_put_when_exactly_at_count_limit(db_path: str) -> None:
    """Putting item when exactly at count limit should trigger eviction."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Fill cache to exactly the limit
    cache.put("key1", EdgeModel(value=1), timestamp=1000.0)
    cache.put("key2", EdgeModel(value=2), timestamp=2000.0)
    cache.put("key3", EdgeModel(value=3), timestamp=3000.0)

    # Verify at limit
    assert cache.get_count() == 3

    # Put one more item - should trigger eviction of key1 (LRU)
    cache.put("key4", EdgeModel(value=4), timestamp=4000.0)

    # Should have 3 items total
    assert cache.get_count() == 3

    # key1 should be evicted
    assert cache.get("key1", timestamp=4000.0) is None

    # Other keys should exist
    assert cache.get("key2", timestamp=4000.0) == EdgeModel(value=2)
    assert cache.get("key3", timestamp=4000.0) == EdgeModel(value=3)
    assert cache.get("key4", timestamp=4000.0) == EdgeModel(value=4)

    cache.close()


def test_put_when_exactly_at_size_limit(db_path: str) -> None:
    """Putting item when exactly at size limit should trigger eviction."""

    # Create a model with known serialization size
    class SizedModel(CacheableModel):
        schema_version: str = "1.0.0"
        data: str

    # Calculate size of one item
    test_item = SizedModel(data="x" * 100)
    item_size = len(test_item.model_dump_json())

    # Set size limit to exactly 3 items
    cache = DiskBackedCache(
        db_path=db_path,
        model=SizedModel,
        max_memory_items=100,
        max_memory_size_bytes=item_size * 3,
        max_disk_items=100,
        max_disk_size_bytes=item_size * 100,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=item_size * 10,
    )

    # Fill cache to exactly the size limit
    cache.put("key1", SizedModel(data="x" * 100), timestamp=1000.0)
    cache.put("key2", SizedModel(data="x" * 100), timestamp=2000.0)
    cache.put("key3", SizedModel(data="x" * 100), timestamp=3000.0)

    # Put one more item - should trigger size-based eviction
    cache.put("key4", SizedModel(data="x" * 100), timestamp=4000.0)

    # key1 should be evicted from memory (but still on disk)
    # key4 should be in memory
    assert cache._memory_item_count <= 3  # type: ignore[attr-defined]

    cache.close()


def test_directory_created_if_not_exists() -> None:
    """Cache should create parent directory if it doesn't exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create path with non-existent subdirectory
        db_path = os.path.join(tmpdir, "subdir", "cache.db")

        # Directory should not exist yet
        assert not os.path.exists(os.path.dirname(db_path))

        # Create cache
        cache = DiskBackedCache(
            db_path=db_path,
            model=EdgeModel,
            max_memory_items=10,
            max_memory_size_bytes=1024 * 1024,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=10 * 1024,
        )

        # Directory should now exist
        assert os.path.exists(os.path.dirname(db_path))

        # Should be able to use cache
        cache.put("key1", EdgeModel(value=1))
        assert cache.get("key1") == EdgeModel(value=1)

        cache.close()


def test_clear_on_empty_cache(db_path: str) -> None:
    """clear() on empty cache should not raise error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Clear empty cache (should not error)
    cache.clear()

    # Should still be empty
    assert cache.get_count() == 0

    cache.close()


def test_exists_on_empty_cache(db_path: str) -> None:
    """exists() on empty cache should return False."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Check existence on empty cache
    assert not cache.exists("nonexistent")

    cache.close()


def test_get_many_from_empty_cache(db_path: str) -> None:
    """get_many() on empty cache should return empty dict."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Get many from empty cache
    result = cache.get_many(["key1", "key2", "key3"])

    # Should return empty dict
    assert result == {}

    # Statistics should show misses
    stats = cache.get_stats()
    assert stats["misses"] == 3

    cache.close()


def test_delete_many_from_empty_cache(db_path: str) -> None:
    """delete_many() on empty cache should not raise error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Delete many from empty cache (should not error)
    cache.delete_many(["key1", "key2", "key3"])

    # Statistics should show deletes
    stats = cache.get_stats()
    assert stats["total_deletes"] == 3

    cache.close()


def test_put_many_empty_dict(db_path: str) -> None:
    """put_many() with empty dict should not error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Put empty dict (should not error)
    cache.put_many({})

    # Cache should still be empty
    assert cache.get_count() == 0

    # Statistics should not change
    stats = cache.get_stats()
    assert stats["total_puts"] == 0

    cache.close()


def test_get_many_empty_list(db_path: str) -> None:
    """get_many() with empty list should return empty dict."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Get many with empty list
    result = cache.get_many([])

    # Should return empty dict
    assert result == {}

    cache.close()


def test_delete_many_empty_list(db_path: str) -> None:
    """delete_many() with empty list should not error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Delete many with empty list
    cache.delete_many([])

    # Statistics should not change
    stats = cache.get_stats()
    assert stats["total_deletes"] == 0

    cache.close()


def test_double_close(db_path: str) -> None:
    """close() can be called multiple times without error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=EdgeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Close once
    cache.close()

    # Close again (should not error)
    cache.close()


def test_oversized_item_larger_than_disk_limit() -> None:
    """Item larger than max_disk_size_bytes should raise ValueError."""

    # Create a model that will be larger than disk limit when serialized
    class LargeModel(CacheableModel):
        schema_version: str = "1.0.0"
        data: str

    cache = DiskBackedCache(
        db_path=":memory:",
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=100,  # Very small disk limit
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10,  # Small item size limit
    )

    # Create item larger than max_disk_size_bytes
    large_item = LargeModel(data="x" * 200)

    # Should raise ValueError
    with pytest.raises(ValueError, match="Item size .* exceeds max_disk_size_bytes"):
        cache.put("large", large_item)

    cache.close()
