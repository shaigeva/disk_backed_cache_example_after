"""Tests for exists/contains check."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class ExampleModel(CacheableModel):
    """Model for exists tests."""

    schema_version: str = "1.0.0"
    value: str


def test_exists_returns_true_when_in_memory(db_path: str) -> None:
    """exists() should return True when key is in memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ExampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ExampleModel(value="test")
    cache.put("key1", obj)

    assert cache.exists("key1") is True

    cache.close()


def test_exists_returns_true_when_on_disk(db_path: str) -> None:
    """exists() should return True when key is only on disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ExampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = ExampleModel(value="test")
    cache.put("key1", obj)

    # Remove from memory but keep on disk
    del cache._memory_cache["key1"]  # type: ignore[attr-defined]

    # Should still exist on disk
    assert cache.exists("key1") is True

    cache.close()


def test_exists_returns_false_when_not_found(db_path: str) -> None:
    """exists() should return False when key is not in cache."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ExampleModel,
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
