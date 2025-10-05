from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_delete_removes_from_memory(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj)

    assert "key1" in cache._memory_cache
    cache.delete("key1")
    assert "key1" not in cache._memory_cache

    cache.close()


def test_delete_nonexistent_key_is_noop(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Should not raise an error
    cache.delete("nonexistent")

    cache.close()


def test_delete_removes_from_sqlite(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj)

    # Verify it's in SQLite
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is not None

    cache.delete("key1")

    # Verify it's removed from SQLite
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is None

    cache.close()


def test_delete_from_both_memory_and_disk(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj)

    # Verify it's in both
    assert "key1" in cache._memory_cache
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is not None

    cache.delete("key1")

    # Verify it's removed from both
    assert "key1" not in cache._memory_cache
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is None

    # get() should return None
    result = cache.get("key1")
    assert result is None

    cache.close()
