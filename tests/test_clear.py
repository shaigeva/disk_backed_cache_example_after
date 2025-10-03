from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_clear_removes_all_items(db_path: str) -> None:
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
    for i in range(5):
        cache.put(f"key{i}", SampleModel(name=f"test{i}"))

    # Verify items exist
    assert len(cache._memory_cache) == 5
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    assert cursor.fetchone()[0] == 5

    # Clear cache
    cache.clear()

    # Verify everything is gone
    assert len(cache._memory_cache) == 0
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")
    assert cursor.fetchone()[0] == 0

    cache.close()


def test_clear_resets_counts_and_sizes(db_path: str) -> None:
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

    # Verify counts and sizes are non-zero
    assert cache._memory_count > 0
    assert cache._memory_total_size > 0

    # Clear cache
    cache.clear()

    # Verify counts and sizes are reset
    assert cache._memory_count == 0
    assert cache._memory_total_size == 0
    assert cache.get_count() == 0
    assert cache.get_total_size() == 0

    cache.close()


def test_clear_on_empty_cache(db_path: str) -> None:
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
    cache.clear()

    assert cache._memory_count == 0
    assert cache._memory_total_size == 0

    cache.close()
