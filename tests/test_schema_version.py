from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModelV1(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


class SampleModelV2(CacheableModel):
    schema_version: str = "2.0.0"
    name: str
    new_field: str = "default"


def test_schema_version_extracted_from_model(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV1,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModelV1(name="test")
    assert obj.schema_version == "1.0.0"

    cache.close()


def test_schema_version_stored_in_cache(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV1,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModelV1(name="test")
    cache.put("key1", obj)

    # Check memory cache
    _, _, _, schema_version = cache._memory_cache["key1"]
    assert schema_version == "1.0.0"

    # Check disk cache
    cursor = cache._conn.execute("SELECT schema_version FROM cache WHERE key = ?", ("key1",))
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == "1.0.0"

    cache.close()


def test_get_with_wrong_schema_returns_none(db_path: str) -> None:
    # First create a cache with V1 model and store an object
    cache_v1 = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV1,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj_v1 = SampleModelV1(name="test")
    cache_v1.put("key1", obj_v1)
    cache_v1.close()

    # Now create a cache with V2 model pointing to the same database
    cache_v2 = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV2,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Trying to get with wrong schema version should return None
    result = cache_v2.get("key1")
    assert result is None

    cache_v2.close()


def test_wrong_schema_item_deleted_from_cache(db_path: str) -> None:
    # First create a cache with V1 model and store an object
    cache_v1 = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV1,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj_v1 = SampleModelV1(name="test")
    cache_v1.put("key1", obj_v1)
    cache_v1.close()

    # Now create a cache with V2 model
    cache_v2 = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV2,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Get with wrong schema should remove the item
    result = cache_v2.get("key1")
    assert result is None

    # Verify item was deleted from disk
    cursor = cache_v2._conn.execute("SELECT key FROM cache WHERE key = ?", ("key1",))
    assert cursor.fetchone() is None

    cache_v2.close()


def test_correct_schema_version_returns_object(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModelV1,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SampleModelV1(name="test")
    cache.put("key1", obj)

    # Clear memory to force disk retrieval
    cache._memory_cache.clear()

    result = cache.get("key1")
    assert result is not None
    assert isinstance(result, SampleModelV1)
    assert result.name == "test"

    cache.close()
