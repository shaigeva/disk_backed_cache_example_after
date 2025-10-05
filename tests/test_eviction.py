from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_memory_evicts_oldest_on_count_limit(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items with different timestamps
    cache.put("key1", SampleModel(name="test1"), timestamp=1.0)
    cache.put("key2", SampleModel(name="test2"), timestamp=2.0)
    cache.put("key3", SampleModel(name="test3"), timestamp=3.0)

    # All 3 should be in memory
    assert cache._memory_count == 3

    # Add one more - should evict oldest (key1)
    cache.put("key4", SampleModel(name="test4"), timestamp=4.0)

    assert cache._memory_count == 3
    assert "key1" not in cache._memory_cache
    assert "key2" in cache._memory_cache
    assert "key3" in cache._memory_cache
    assert "key4" in cache._memory_cache

    cache.close()


def test_memory_lru_order_maintained(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add two items
    cache.put("key1", SampleModel(name="test1"), timestamp=1.0)
    cache.put("key2", SampleModel(name="test2"), timestamp=2.0)

    # Add third - evicts key1
    cache.put("key3", SampleModel(name="test3"), timestamp=3.0)
    assert "key1" not in cache._memory_cache
    assert "key2" in cache._memory_cache
    assert "key3" in cache._memory_cache

    cache.close()


def test_memory_eviction_makes_room_for_new(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=3,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Fill to limit
    for i in range(3):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"), timestamp=float(i))

    # Add new item
    cache.put("new_key", SampleModel(name="new"), timestamp=10.0)

    # Should have room for new item
    assert "new_key" in cache._memory_cache
    assert cache._memory_count == 3

    cache.close()


def test_memory_evicts_on_size_limit(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=100,
        max_memory_size_bytes=100,  # Very small size limit
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items that will exceed size limit
    cache.put("key1", SampleModel(name="test1"), timestamp=1.0)
    cache.put("key2", SampleModel(name="test2"), timestamp=2.0)

    # Should evict to stay under size limit
    assert cache._memory_total_size <= 100

    cache.close()


def test_memory_evicts_multiple_for_large_item(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=100,
        max_memory_size_bytes=150,  # Small size limit
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add small items
    cache.put("key1", SampleModel(name="a"), timestamp=1.0)
    cache.put("key2", SampleModel(name="b"), timestamp=2.0)
    cache.put("key3", SampleModel(name="c"), timestamp=3.0)

    initial_count = cache._memory_count

    # Add larger item - may evict multiple
    cache.put("large", SampleModel(name="x" * 100), timestamp=4.0)

    # Should maintain size limit
    assert cache._memory_total_size <= 150
    # May have evicted items
    assert cache._memory_count <= initial_count

    cache.close()


def test_disk_evicts_oldest_on_count_limit(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", SampleModel(name="test1"), timestamp=1.0)
    cache.put("key2", SampleModel(name="test2"), timestamp=2.0)
    cache.put("key3", SampleModel(name="test3"), timestamp=3.0)

    # Verify all in disk
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    assert cursor.fetchone()[0] == 3

    # Add one more - should evict oldest from disk
    cache.put("key4", SampleModel(name="test4"), timestamp=4.0)

    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    assert cursor.fetchone()[0] == 3

    # key1 should be evicted
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is None

    cache.close()


def test_disk_eviction_count_accurate(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=5,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add more items than disk limit
    for i in range(10):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"), timestamp=float(i))

    # Disk should have exactly max_disk_items
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    count = cursor.fetchone()[0]
    assert count <= 5

    cache.close()


def test_disk_evicts_on_size_limit(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=200,  # Small size limit
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    for i in range(5):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"), timestamp=float(i))

    # Check disk size
    cursor = cache._conn.execute("SELECT SUM(size) FROM cache")
    total_size = cursor.fetchone()[0]
    assert total_size <= 200

    cache.close()


def test_disk_size_tracking_after_eviction(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=3,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items to trigger eviction
    for i in range(5):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"), timestamp=float(i))

    # Verify size is tracked correctly
    cursor = cache._conn.execute("SELECT SUM(size) FROM cache")
    disk_size = cursor.fetchone()[0]
    assert disk_size > 0

    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    count = cursor.fetchone()[0]
    assert count <= 3

    cache.close()


def test_lru_tiebreak_uses_alphabetical_order(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items with same timestamp - alphabetically key_b comes before key_c
    cache.put("key_c", SampleModel(name="c"), timestamp=1.0)
    cache.put("key_b", SampleModel(name="b"), timestamp=1.0)

    # Add third item - should evict key_b (alphabetically first among ties)
    cache.put("key_a", SampleModel(name="a"), timestamp=2.0)

    assert "key_b" not in cache._memory_cache
    assert "key_c" in cache._memory_cache
    assert "key_a" in cache._memory_cache

    cache.close()


def test_disk_eviction_removes_from_memory(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=2,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", SampleModel(name="test1"), timestamp=1.0)
    cache.put("key2", SampleModel(name="test2"), timestamp=2.0)

    # Both in memory
    assert "key1" in cache._memory_cache
    assert "key2" in cache._memory_cache

    # Add third - triggers disk eviction which cascades to memory
    cache.put("key3", SampleModel(name="test3"), timestamp=3.0)

    # key1 should be evicted from both
    assert "key1" not in cache._memory_cache
    cursor = cache._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is None

    cache.close()
