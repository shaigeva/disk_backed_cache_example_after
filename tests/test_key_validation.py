import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_put_with_invalid_key_type_raises_valueerror(db_path: str) -> None:
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
    with pytest.raises(ValueError, match="Key must be a string"):
        cache.put(123, obj)  # type: ignore


def test_put_with_too_long_key_raises_valueerror(db_path: str) -> None:
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
    long_key = "x" * 257
    with pytest.raises(ValueError, match="Key must be 256 characters or less"):
        cache.put(long_key, obj)


def test_valid_key_accepted(db_path: str) -> None:
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
    # Test various valid keys
    cache.put("key1", obj)
    cache.put("x" * 256, obj)  # Exactly 256 chars
    cache.put("key-with-dashes", obj)
    cache.put("key_with_underscores", obj)
    cache.put("key:with:colons", obj)

    # Verify they can be retrieved
    assert cache.get("key1") is not None
    assert cache.get("x" * 256) is not None
