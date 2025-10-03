from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_exists_returns_true_for_memory_item(db_path: str) -> None:
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

    assert cache.exists("key1") is True

    cache.close()


def test_exists_returns_true_for_disk_item(db_path: str) -> None:
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

    # Clear memory to force disk-only check
    cache._memory_cache.clear()

    assert cache.exists("key1") is True

    cache.close()


def test_exists_returns_false_for_nonexistent(db_path: str) -> None:
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

    assert cache.exists("nonexistent") is False

    cache.close()
