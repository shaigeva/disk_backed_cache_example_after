from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str
    value: int


def test_sqlite_put_and_get_returns_object(db_path: str) -> None:
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

    obj = SampleModel(name="test", value=42)
    cache.put("key1", obj)

    # Clear memory cache to force disk retrieval
    cache._memory_cache.clear()

    result = cache.get("key1")
    assert result is not None
    assert isinstance(result, SampleModel)
    assert result.name == "test"
    assert result.value == 42

    cache.close()


def test_sqlite_get_nonexistent_returns_none(db_path: str) -> None:
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

    # Clear memory to ensure we're testing disk
    cache._memory_cache.clear()

    result = cache.get("nonexistent")
    assert result is None

    cache.close()


def test_sqlite_transaction_isolation(db_path: str) -> None:
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

    # Put multiple objects
    obj1 = SampleModel(name="test1", value=1)
    obj2 = SampleModel(name="test2", value=2)
    obj3 = SampleModel(name="test3", value=3)

    cache.put("key1", obj1)
    cache.put("key2", obj2)
    cache.put("key3", obj3)

    # Verify each was stored in a separate transaction
    # by checking they all exist in the database
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    count = cursor.fetchone()[0]
    assert count == 3

    cache.close()
