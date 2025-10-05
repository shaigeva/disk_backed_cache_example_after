from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_expired_memory_item_returns_from_disk(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=10.0,  # 10 second TTL
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Put item with timestamp
    obj = SampleModel(name="test")
    cache.put("key1", obj, timestamp=100.0)

    # Get immediately - should work
    result = cache.get("key1", timestamp=100.0)
    assert result is not None

    # Get after memory TTL expired but disk TTL still valid
    # Should return from disk
    result = cache.get("key1", timestamp=111.0)  # 11 seconds later
    assert result is not None  # Still available from disk

    cache.close()


def test_valid_memory_item_returns_object(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=10.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj, timestamp=100.0)

    # Get within TTL - should work
    result = cache.get("key1", timestamp=105.0)  # 5 seconds later
    assert result is not None
    assert isinstance(result, SampleModel)
    assert result.name == "test"

    cache.close()


def test_expired_item_removed_from_memory(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=10.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj, timestamp=100.0)

    # Verify it's in memory
    assert "key1" in cache._memory_cache

    # Get after memory expiration (but disk still valid)
    result = cache.get("key1", timestamp=111.0)
    assert result is not None  # Still returns from disk

    # After getting from disk, it should be promoted back to memory
    assert "key1" in cache._memory_cache

    cache.close()


def test_expired_disk_item_returns_none(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=100.0,  # 100 second TTL
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj, timestamp=100.0)

    # Clear memory to force disk retrieval
    cache._memory_cache.clear()
    cache._memory_count = 0
    cache._memory_total_size = 0

    # Get after disk TTL expired - should return None
    result = cache.get("key1", timestamp=201.0)  # 101 seconds later
    assert result is None

    cache.close()


def test_expired_item_removed_from_disk(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=100.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj, timestamp=100.0)

    # Clear memory
    cache._memory_cache.clear()
    cache._memory_count = 0
    cache._memory_total_size = 0

    # Verify it's in disk
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is not None

    # Get after expiration
    result = cache.get("key1", timestamp=201.0)
    assert result is None

    # Should be removed from disk
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is None

    cache.close()


def test_custom_timestamp_on_put(db_path: str) -> None:
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
    cache.put("key1", obj, timestamp=1234567890.0)

    # Verify timestamp is stored
    _, _, stored_timestamp, _ = cache._memory_cache["key1"]
    assert stored_timestamp == 1234567890.0

    cache.close()


def test_custom_timestamp_on_get_for_ttl(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=10.0,
        disk_ttl_seconds=50.0,  # Shorter disk TTL for this test
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModel(name="test")
    cache.put("key1", obj, timestamp=100.0)

    # Get with custom timestamp for TTL calculation
    result = cache.get("key1", timestamp=105.0)  # Within both TTLs
    assert result is not None

    result = cache.get("key1", timestamp=200.0)  # Outside both TTLs
    assert result is None

    cache.close()


def test_timestamp_none_uses_current_time(db_path: str) -> None:
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
    # Put without timestamp - should use current time
    cache.put("key1", obj)

    # Verify a timestamp was set
    _, _, stored_timestamp, _ = cache._memory_cache["key1"]
    assert stored_timestamp > 0

    cache.close()
