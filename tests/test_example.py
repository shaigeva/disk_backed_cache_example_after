from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


def test_disk_backed_cache_init() -> None:
    """Test that DiskBackedCache can be initialized."""
    cache = DiskBackedCache(
        db_path=":memory:",
        model=CacheableModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )
    assert cache is not None
    cache.close()
