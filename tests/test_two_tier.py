from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


class LargeModel(CacheableModel):
    schema_version: str = "1.0.0"
    data: str


def test_put_stores_in_both_tiers(db_path: str) -> None:
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

    # Should be in both memory and disk
    assert "key1" in cache._memory_cache

    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is not None

    cache.close()


def test_get_checks_memory_first(db_path: str) -> None:
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

    # Item is in both tiers
    assert "key1" in cache._memory_cache

    # Get should return from memory
    result = cache.get("key1")
    assert result is not None
    assert isinstance(result, SampleModel)
    assert result.name == "test"

    cache.close()


def test_get_promotes_from_disk_to_memory(db_path: str) -> None:
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

    # Clear memory to simulate disk-only state
    cache._memory_cache.clear()
    cache._memory_count = 0
    cache._memory_total_size = 0

    # Get should promote to memory
    result = cache.get("key1")
    assert result is not None
    assert "key1" in cache._memory_cache

    cache.close()


def test_get_promotion_respects_memory_limits(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=10.0,  # Short TTL
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("mem1", SampleModel(name="mem1"), timestamp=1.0)
    cache.put("disk1", SampleModel(name="disk1"), timestamp=2.0)

    # Wait for disk1 to expire from memory but not disk
    # Get disk1 after memory TTL expired - will promote from disk
    result = cache.get("disk1", timestamp=20.0)
    assert result is not None
    assert "disk1" in cache._memory_cache
    assert cache._memory_count <= 2

    cache.close()


def test_large_item_stored_disk_only(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,  # Very small max item size
    )

    # Create a large item
    large_obj = LargeModel(data="x" * 100)
    cache.put("large_key", large_obj)

    # Should be on disk
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("large_key",))
    assert cursor.fetchone() is not None

    # Should NOT be in memory
    assert "large_key" not in cache._memory_cache

    cache.close()


def test_small_item_stored_both_tiers(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=1000,  # Large enough for small items
    )

    obj = SampleModel(name="small")
    cache.put("small_key", obj)

    # Should be in both tiers
    assert "small_key" in cache._memory_cache

    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("small_key",))
    assert cursor.fetchone() is not None

    cache.close()


def test_large_item_not_promoted_to_memory(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=LargeModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=50,  # Very small max item size
    )

    # Create and store a large item
    large_obj = LargeModel(data="x" * 100)
    cache.put("large_key", large_obj)

    # Verify it's disk-only
    assert "large_key" not in cache._memory_cache

    # Get it - should NOT be promoted to memory
    result = cache.get("large_key")
    assert result is not None
    assert isinstance(result, LargeModel)
    assert result.data == "x" * 100

    # Still should not be in memory
    assert "large_key" not in cache._memory_cache

    cache.close()
