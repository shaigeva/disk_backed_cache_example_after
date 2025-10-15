"""Tests for batch operations (put_many, get_many, delete_many)."""

import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class BatchModel(CacheableModel):
    """Model for batch operations tests."""

    schema_version: str = "1.0.0"
    value: int


class OtherModel(CacheableModel):
    """Different model for type validation tests."""

    schema_version: str = "1.0.0"
    name: str


def test_put_many_stores_multiple_items(db_path: str) -> None:
    """put_many() should store multiple items that can be retrieved."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "key2": BatchModel(value=2),
        "key3": BatchModel(value=3),
    }

    cache.put_many(items)

    # All items should be retrievable
    assert cache.get("key1") == BatchModel(value=1)
    assert cache.get("key2") == BatchModel(value=2)
    assert cache.get("key3") == BatchModel(value=3)

    cache.close()


def test_put_many_validates_all_keys_first(db_path: str) -> None:
    """put_many() should validate all keys before storing any items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "": BatchModel(value=2),  # Invalid key (empty)
        "key3": BatchModel(value=3),
    }

    # Should raise ValueError for empty key
    with pytest.raises(ValueError, match="Key cannot be empty"):
        cache.put_many(items)

    # No items should have been stored
    assert cache.get("key1") is None
    assert cache.get("key3") is None
    assert cache.get_count() == 0

    cache.close()


def test_put_many_validates_all_values_first(db_path: str) -> None:
    """put_many() should validate all values before storing any items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "key2": OtherModel(name="test"),  # type: ignore[dict-item]  # Wrong model type
        "key3": BatchModel(value=3),
    }

    # Should raise TypeError for wrong model type
    with pytest.raises(TypeError, match="Value must be an instance of BatchModel"):
        cache.put_many(items)

    # No items should have been stored
    assert cache.get("key1") is None
    assert cache.get("key3") is None
    assert cache.get_count() == 0

    cache.close()


def test_put_many_is_atomic(db_path: str) -> None:
    """put_many() should be atomic - all succeed or all fail."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # First, store a valid item
    cache.put("existing", BatchModel(value=99))
    assert cache.get_count() == 1

    # Try to store batch with invalid key
    items = {
        "key1": BatchModel(value=1),
        "a" * 300: BatchModel(value=2),  # Invalid key (too long)
        "key3": BatchModel(value=3),
    }

    with pytest.raises(ValueError, match="exceeds maximum"):
        cache.put_many(items)

    # No new items should have been stored
    assert cache.get("key1") is None
    assert cache.get("key3") is None
    assert cache.get_count() == 1  # Only the original item

    cache.close()


def test_put_many_uses_same_timestamp(db_path: str) -> None:
    """put_many() should use the same timestamp for all items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "key2": BatchModel(value=2),
        "key3": BatchModel(value=3),
    }

    custom_timestamp = 1234567890.0
    cache.put_many(items, timestamp=custom_timestamp)

    # Verify all items have the same timestamp in disk
    cursor = cache._conn.execute("SELECT key, timestamp FROM cache ORDER BY key")  # type: ignore[attr-defined]
    rows = cursor.fetchall()

    assert len(rows) == 3
    assert rows[0] == ("key1", custom_timestamp)
    assert rows[1] == ("key2", custom_timestamp)
    assert rows[2] == ("key3", custom_timestamp)

    cache.close()


def test_put_many_updates_memory_and_disk(db_path: str) -> None:
    """put_many() should store items in both memory and disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "key2": BatchModel(value=2),
    }

    cache.put_many(items)

    # Check memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]

    # Check disk
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")  # type: ignore[attr-defined]
    assert cursor.fetchone()[0] == 2

    cache.close()


def test_put_many_increments_total_puts(db_path: str) -> None:
    """put_many() should increment total_puts by number of items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "key2": BatchModel(value=2),
        "key3": BatchModel(value=3),
    }

    cache.put_many(items)

    stats = cache.get_stats()
    assert stats["total_puts"] == 3

    cache.close()


def test_put_many_overwrites_existing(db_path: str) -> None:
    """put_many() should overwrite existing keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store initial items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Overwrite with put_many
    items = {
        "key1": BatchModel(value=10),
        "key2": BatchModel(value=20),
        "key3": BatchModel(value=30),
    }

    cache.put_many(items)

    # Values should be updated
    assert cache.get("key1") == BatchModel(value=10)
    assert cache.get("key2") == BatchModel(value=20)
    assert cache.get("key3") == BatchModel(value=30)

    # Count should be 3 (not 5)
    assert cache.get_count() == 3

    cache.close()


def test_put_many_empty_dict(db_path: str) -> None:
    """put_many() should handle empty dict gracefully."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Empty dict should be a no-op
    cache.put_many({})

    assert cache.get_count() == 0

    stats = cache.get_stats()
    assert stats["total_puts"] == 0

    cache.close()


def test_put_many_updates_size_tracking(db_path: str) -> None:
    """put_many() should correctly update memory size tracking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    items = {
        "key1": BatchModel(value=1),
        "key2": BatchModel(value=2),
    }

    cache.put_many(items)

    # Check that size tracking is correct
    stats = cache.get_stats()
    assert stats["current_memory_items"] == 2

    # Total size should be sum of serialized items
    total_size = cache.get_total_size()
    assert total_size > 0

    cache.close()


def test_get_many_retrieves_multiple_items(db_path: str) -> None:
    """get_many() should retrieve multiple items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))
    cache.put("key3", BatchModel(value=3))

    # Retrieve with get_many
    result = cache.get_many(["key1", "key2", "key3"])

    assert len(result) == 3
    assert result["key1"] == BatchModel(value=1)
    assert result["key2"] == BatchModel(value=2)
    assert result["key3"] == BatchModel(value=3)

    cache.close()


def test_get_many_validates_all_keys(db_path: str) -> None:
    """get_many() should validate all keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Try to get with invalid key
    with pytest.raises(ValueError, match="Key cannot be empty"):
        cache.get_many(["key1", "", "key2"])

    cache.close()


def test_get_many_omits_missing_keys(db_path: str) -> None:
    """get_many() should omit missing keys from result (not return None)."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store only one item
    cache.put("key1", BatchModel(value=1))

    # Request multiple keys, some missing
    result = cache.get_many(["key1", "key2", "key3"])

    # Only found keys should be in result
    assert len(result) == 1
    assert "key1" in result
    assert "key2" not in result
    assert "key3" not in result

    cache.close()


def test_get_many_checks_memory_first(db_path: str) -> None:
    """get_many() should check memory first for items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items (they'll be in both memory and disk)
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Reset stats
    cache._stats_memory_hits = 0  # type: ignore[attr-defined]
    cache._stats_disk_hits = 0  # type: ignore[attr-defined]
    cache._stats_total_gets = 0  # type: ignore[attr-defined]

    # Retrieve with get_many
    cache.get_many(["key1", "key2"])

    # Should be memory hits
    stats = cache.get_stats()
    assert stats["memory_hits"] == 2
    assert stats["disk_hits"] == 0
    assert stats["total_gets"] == 2

    cache.close()


def test_get_many_checks_disk_if_not_in_memory(db_path: str) -> None:
    """get_many() should check disk if items not in memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Clear memory
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_item_count = 0  # type: ignore[attr-defined]

    # Reset stats
    cache._stats_memory_hits = 0  # type: ignore[attr-defined]
    cache._stats_disk_hits = 0  # type: ignore[attr-defined]
    cache._stats_total_gets = 0  # type: ignore[attr-defined]

    # Retrieve with get_many
    result = cache.get_many(["key1", "key2"])

    # Should be disk hits
    assert len(result) == 2
    stats = cache.get_stats()
    assert stats["memory_hits"] == 0
    assert stats["disk_hits"] == 2
    assert stats["total_gets"] == 2

    cache.close()


def test_get_many_increments_total_gets(db_path: str) -> None:
    """get_many() should increment total_gets by number of keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Reset stats
    cache._stats_total_gets = 0  # type: ignore[attr-defined]

    # Retrieve with get_many
    cache.get_many(["key1", "key2", "key3"])  # key3 doesn't exist

    stats = cache.get_stats()
    assert stats["total_gets"] == 3  # All 3 keys count

    cache.close()


def test_get_many_updates_hit_miss_counters(db_path: str) -> None:
    """get_many() should update hit/miss counters for each key."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))  # Will be in memory
    cache.put("key2", BatchModel(value=2))  # Will be in memory

    # Clear memory for key2
    del cache._memory_cache["key2"]  # type: ignore[attr-defined]

    # Reset stats
    cache._stats_memory_hits = 0  # type: ignore[attr-defined]
    cache._stats_disk_hits = 0  # type: ignore[attr-defined]
    cache._stats_misses = 0  # type: ignore[attr-defined]
    cache._stats_total_gets = 0  # type: ignore[attr-defined]

    # Retrieve with get_many
    cache.get_many(["key1", "key2", "key3"])

    stats = cache.get_stats()
    assert stats["memory_hits"] == 1  # key1
    assert stats["disk_hits"] == 1  # key2
    assert stats["misses"] == 1  # key3
    assert stats["total_gets"] == 3

    cache.close()


def test_get_many_does_not_update_timestamps(db_path: str) -> None:
    """get_many() should not update access timestamps."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items with custom timestamp
    original_timestamp = 1000000.0
    cache.put("key1", BatchModel(value=1), timestamp=original_timestamp)

    # Clear memory so get_many will check disk
    cache._memory_cache.clear()  # type: ignore[attr-defined]

    # Retrieve with get_many (use same timestamp to avoid TTL expiration)
    cache.get_many(["key1"], timestamp=original_timestamp)

    # Check that timestamp hasn't changed
    cursor = cache._conn.execute("SELECT timestamp FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == original_timestamp

    cache.close()


def test_get_many_empty_list(db_path: str) -> None:
    """get_many() should handle empty list gracefully."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    result = cache.get_many([])

    assert result == {}

    stats = cache.get_stats()
    assert stats["total_gets"] == 0

    cache.close()


def test_get_many_mixed_results(db_path: str) -> None:
    """get_many() should handle mixed results (some found, some missing)."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store some items
    cache.put("key1", BatchModel(value=1))
    cache.put("key3", BatchModel(value=3))
    cache.put("key5", BatchModel(value=5))

    # Request mix of existing and non-existing
    result = cache.get_many(["key1", "key2", "key3", "key4", "key5", "key6"])

    # Only found keys should be in result
    assert len(result) == 3
    assert result["key1"] == BatchModel(value=1)
    assert result["key3"] == BatchModel(value=3)
    assert result["key5"] == BatchModel(value=5)

    cache.close()


def test_delete_many_removes_multiple_items(db_path: str) -> None:
    """delete_many() should remove multiple items."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))
    cache.put("key3", BatchModel(value=3))

    # Delete with delete_many
    cache.delete_many(["key1", "key2", "key3"])

    # All items should be deleted
    assert cache.get("key1") is None
    assert cache.get("key2") is None
    assert cache.get("key3") is None
    assert cache.get_count() == 0

    cache.close()


def test_delete_many_validates_all_keys(db_path: str) -> None:
    """delete_many() should validate all keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Try to delete with invalid key
    with pytest.raises(ValueError, match="Key cannot be empty"):
        cache.delete_many(["key1", "", "key2"])

    cache.close()


def test_delete_many_ignores_missing_keys(db_path: str) -> None:
    """delete_many() should silently ignore non-existent keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store only one item
    cache.put("key1", BatchModel(value=1))

    # Delete multiple keys, some missing (should not raise error)
    cache.delete_many(["key1", "key2", "key3"])

    # key1 should be deleted, others were never there
    assert cache.get("key1") is None
    assert cache.get_count() == 0

    cache.close()


def test_delete_many_removes_from_memory_and_disk(db_path: str) -> None:
    """delete_many() should remove items from both memory and disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))

    # Verify they're in both memory and disk
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" in cache._memory_cache  # type: ignore[attr-defined]
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")  # type: ignore[attr-defined]
    assert cursor.fetchone()[0] == 2

    # Delete with delete_many
    cache.delete_many(["key1", "key2"])

    # Should be removed from both
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]
    assert "key2" not in cache._memory_cache  # type: ignore[attr-defined]
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")  # type: ignore[attr-defined]
    assert cursor.fetchone()[0] == 0

    cache.close()


def test_delete_many_increments_total_deletes(db_path: str) -> None:
    """delete_many() should increment total_deletes by number of keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))
    cache.put("key3", BatchModel(value=3))

    # Reset stats
    cache._stats_total_deletes = 0  # type: ignore[attr-defined]

    # Delete with delete_many
    cache.delete_many(["key1", "key2", "key3"])

    stats = cache.get_stats()
    assert stats["total_deletes"] == 3

    cache.close()


def test_delete_many_updates_count_tracking(db_path: str) -> None:
    """delete_many() should correctly update count tracking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))
    cache.put("key3", BatchModel(value=3))
    assert cache.get_count() == 3

    # Delete with delete_many
    cache.delete_many(["key1", "key2"])

    assert cache.get_count() == 1

    stats = cache.get_stats()
    assert stats["current_memory_items"] == 1
    assert stats["current_disk_items"] == 1

    cache.close()


def test_delete_many_updates_size_tracking(db_path: str) -> None:
    """delete_many() should correctly update size tracking."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store items
    cache.put("key1", BatchModel(value=1))
    cache.put("key2", BatchModel(value=2))
    cache.put("key3", BatchModel(value=3))
    initial_size = cache.get_total_size()

    # Delete with delete_many
    cache.delete_many(["key1", "key2"])

    final_size = cache.get_total_size()
    assert final_size < initial_size
    assert final_size > 0  # key3 still there

    cache.close()


def test_delete_many_empty_list(db_path: str) -> None:
    """delete_many() should handle empty list gracefully."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store an item
    cache.put("key1", BatchModel(value=1))

    # Delete empty list (should be a no-op)
    cache.delete_many([])

    # Item should still exist
    assert cache.get("key1") == BatchModel(value=1)

    stats = cache.get_stats()
    assert stats["total_deletes"] == 0

    cache.close()


def test_delete_many_mixed_keys(db_path: str) -> None:
    """delete_many() should handle mix of existing and non-existing keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=BatchModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Store some items
    cache.put("key1", BatchModel(value=1))
    cache.put("key3", BatchModel(value=3))
    cache.put("key5", BatchModel(value=5))

    # Delete mix of existing and non-existing
    cache.delete_many(["key1", "key2", "key3", "key4", "key5", "key6"])

    # All existing items should be deleted
    assert cache.get("key1") is None
    assert cache.get("key3") is None
    assert cache.get("key5") is None
    assert cache.get_count() == 0

    cache.close()
