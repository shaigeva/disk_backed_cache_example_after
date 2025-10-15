"""Basic tests for thread safety infrastructure."""

from typing import cast

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class ThreadModel(CacheableModel):
    """Model for thread safety tests."""

    schema_version: str = "1.0.0"
    value: int


def test_lock_is_initialized(db_path: str) -> None:
    """Cache should have a lock initialized."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ThreadModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Verify lock exists and is an RLock
    assert hasattr(cache, "_lock")
    lock = cache._lock  # type: ignore[attr-defined]
    # RLock doesn't have a simple type check, verify it has expected methods
    assert hasattr(lock, "acquire")
    assert hasattr(lock, "release")

    cache.close()


def test_sequential_operations_work(db_path: str) -> None:
    """Test that basic operations work sequentially (baseline for thread safety)."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ThreadModel,
        max_memory_items=100,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Put items sequentially
    for i in range(10):
        key = f"key{i}"
        cache.put(key, ThreadModel(value=i))

    # Get items sequentially
    for i in range(10):
        key = f"key{i}"
        retrieved = cache.get(key)
        assert retrieved is not None
        retrieved_typed = cast(ThreadModel, retrieved)
        assert retrieved_typed.value == i

    cache.close()
