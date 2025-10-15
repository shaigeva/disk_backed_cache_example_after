"""Tests for delete operations."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class ItemModel(CacheableModel):
    """Model for delete operation tests."""

    schema_version: str = "1.0.0"
    data: str


def test_delete_removes_from_memory(db_path: str) -> None:
    """Delete should remove item from memory cache."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ItemModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ItemModel(data="test")
    cache.put("key1", obj)

    # Verify it's in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]

    # Delete it
    cache.delete("key1")

    # Should no longer be in memory
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_delete_nonexistent_key_succeeds(db_path: str) -> None:
    """Deleting a nonexistent key should not raise an error."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ItemModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Should not raise any error
    cache.delete("nonexistent")

    cache.close()


def test_delete_after_put_makes_get_return_none_from_memory(db_path: str) -> None:
    """After deleting from memory, get should not find it in memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ItemModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ItemModel(data="test")
    cache.put("key1", obj)

    # Delete from memory only (not disk yet)
    del cache._memory_cache["key1"]  # type: ignore[attr-defined]
    cache.delete("key1")  # Should not error even though not in memory

    # Get will still find it on disk (Step 8 will delete from disk)
    # For now, just verify delete doesn't error
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    cache.close()


def test_delete_removes_from_disk(db_path: str) -> None:
    """Delete should remove item from SQLite database."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ItemModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ItemModel(data="test")
    cache.put("key1", obj)

    # Verify it's on disk
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    count = cursor.fetchone()[0]
    assert count == 1

    # Delete it
    cache.delete("key1")

    # Should no longer be on disk
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    count = cursor.fetchone()[0]
    assert count == 0

    cache.close()


def test_delete_from_both_memory_and_disk(db_path: str) -> None:
    """Delete should remove item from both memory and disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ItemModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ItemModel(data="test")
    cache.put("key1", obj)

    # Verify it's in both places
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    assert cursor.fetchone()[0] == 1

    # Delete it
    cache.delete("key1")

    # Should be gone from both places
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    assert cursor.fetchone()[0] == 0

    cache.close()


def test_after_delete_get_returns_none(db_path: str) -> None:
    """After delete, get should return None."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ItemModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ItemModel(data="test")
    cache.put("key1", obj)

    # Delete it
    cache.delete("key1")

    # Get should return None
    result = cache.get("key1")
    assert result is None

    cache.close()
