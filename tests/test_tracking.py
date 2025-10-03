from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_memory_count_increases_on_put(db_path: str) -> None:
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

    assert cache._memory_count == 0

    obj = SampleModel(name="test1")
    cache.put("key1", obj)
    assert cache._memory_count == 1

    obj2 = SampleModel(name="test2")
    cache.put("key2", obj2)
    assert cache._memory_count == 2

    cache.close()


def test_memory_count_decreases_on_delete(db_path: str) -> None:
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

    obj1 = SampleModel(name="test1")
    obj2 = SampleModel(name="test2")
    cache.put("key1", obj1)
    cache.put("key2", obj2)
    assert cache._memory_count == 2

    cache.delete("key1")
    assert cache._memory_count == 1

    cache.delete("key2")
    assert cache._memory_count == 0

    cache.close()


def test_memory_count_accurate(db_path: str) -> None:
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

    # Add multiple items
    for i in range(5):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"))

    assert cache._memory_count == 5
    assert cache._memory_count == len(cache._memory_cache)

    cache.close()


def test_memory_size_tracking_on_put(db_path: str) -> None:
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

    assert cache._memory_total_size == 0

    obj = SampleModel(name="test")
    expected_size = len(obj.model_dump_json())

    cache.put("key1", obj)
    assert cache._memory_total_size == expected_size

    cache.close()


def test_memory_size_tracking_on_delete(db_path: str) -> None:
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
    initial_size = cache._memory_total_size

    cache.delete("key1")
    assert cache._memory_total_size == 0
    assert cache._memory_total_size < initial_size

    cache.close()


def test_memory_total_size_accurate(db_path: str) -> None:
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

    # Add multiple items and calculate expected size
    expected_size = 0
    for i in range(3):
        obj = SampleModel(name=f"test{i}")
        expected_size += len(obj.model_dump_json())
        cache.put(f"key{i}", obj)

    assert cache._memory_total_size == expected_size

    cache.close()


def test_sqlite_count_query_returns_correct_value(db_path: str) -> None:
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

    # Add items
    for i in range(3):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"))

    # Check disk count directly
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    disk_count = cursor.fetchone()[0]
    assert disk_count == 3

    cache.close()


def test_total_count_includes_both_tiers(db_path: str) -> None:
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

    # Add items (they'll be in both memory and disk)
    for i in range(3):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"))

    # Total count should be memory + disk
    # But since items are in both, we're counting duplicates for now
    # This is expected behavior until we implement proper two-tier coordination
    total_count = cache.get_count()
    assert total_count == 6  # 3 in memory + 3 on disk

    cache.close()


def test_sqlite_stores_size_metadata(db_path: str) -> None:
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
    expected_size = len(obj.model_dump_json())

    cache.put("key1", obj)

    # Check that size is stored in database
    cursor = cache._conn.execute("SELECT size FROM cache WHERE key = ?", ("key1",))
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == expected_size

    cache.close()


def test_sqlite_total_size_query(db_path: str) -> None:
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

    # Add items
    expected_disk_size = 0
    for i in range(3):
        obj = SampleModel(name=f"test{i}")
        expected_disk_size += len(obj.model_dump_json())
        cache.put(f"key{i}", obj)

    # Check disk size directly
    cursor = cache._conn.execute("SELECT SUM(size) FROM cache")
    disk_size = cursor.fetchone()[0]
    assert disk_size == expected_disk_size

    cache.close()


def test_combined_size_both_tiers(db_path: str) -> None:
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

    # Add items
    expected_size = 0
    for i in range(3):
        obj = SampleModel(name=f"test{i}")
        expected_size += len(obj.model_dump_json())
        cache.put(f"key{i}", obj)

    # Total size should be memory + disk
    # Items are in both, so we're counting duplicates for now
    total_size = cache.get_total_size()
    assert total_size == expected_size * 2  # Both memory and disk

    cache.close()
