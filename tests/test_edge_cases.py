import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_empty_cache_operations(db_path: str) -> None:
    """Test operations on an empty cache."""
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

    # Get from empty cache
    assert cache.get("nonexistent") is None

    # Delete from empty cache (should be no-op)
    cache.delete("nonexistent")

    # Exists on empty cache
    assert not cache.exists("nonexistent")

    # Count and size on empty cache
    assert cache.get_count() == 0
    assert cache.get_total_size() == 0

    # Clear empty cache
    cache.clear()

    cache.close()


def test_exactly_at_limit(db_path: str) -> None:
    """Test behavior when exactly at memory and disk limits."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=2,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=2,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add exactly max_memory_items
    cache.put("key1", SampleModel(name="test1"), timestamp=1.0)
    cache.put("key2", SampleModel(name="test2"), timestamp=2.0)

    # Should be at limit
    assert cache._memory_count == 2

    # Add one more - should trigger eviction
    cache.put("key3", SampleModel(name="test3"), timestamp=3.0)

    # Should still be at limit
    assert cache._memory_count == 2
    assert "key1" not in cache._memory_cache
    assert "key2" in cache._memory_cache
    assert "key3" in cache._memory_cache

    cache.close()


def test_invalid_constructor_parameters(db_path: str) -> None:
    """Test that invalid constructor parameters are handled."""
    # Test with negative values
    with pytest.raises(ValueError, match="max_memory_items must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=-1,
            max_memory_size_bytes=1024,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=1024,
        )

    with pytest.raises(ValueError, match="max_memory_size_bytes must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=10,
            max_memory_size_bytes=-1,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=1024,
        )

    with pytest.raises(ValueError, match="max_disk_items must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=10,
            max_memory_size_bytes=1024,
            max_disk_items=-1,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=1024,
        )

    with pytest.raises(ValueError, match="max_disk_size_bytes must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=10,
            max_memory_size_bytes=1024,
            max_disk_items=100,
            max_disk_size_bytes=-1,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=1024,
        )

    with pytest.raises(ValueError, match="memory_ttl_seconds must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=10,
            max_memory_size_bytes=1024,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=-1.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=1024,
        )

    with pytest.raises(ValueError, match="disk_ttl_seconds must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=10,
            max_memory_size_bytes=1024,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=-1.0,
            max_item_size_bytes=1024,
        )

    with pytest.raises(ValueError, match="max_item_size_bytes must be positive"):
        DiskBackedCache(
            db_path=db_path,
            model=SampleModel,
            max_memory_items=10,
            max_memory_size_bytes=1024,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=-1,
        )


def test_model_without_default_schema_version_fails() -> None:
    """Test that models without default schema_version value are rejected."""
    from pydantic import BaseModel

    class InvalidModel(BaseModel):
        # No schema_version field at all
        name: str

    # Missing schema_version field should fail during initialization
    with pytest.raises(KeyError):
        DiskBackedCache(
            db_path=":memory:",
            model=InvalidModel,  # type: ignore[arg-type]
            max_memory_items=10,
            max_memory_size_bytes=1024,
            max_disk_items=100,
            max_disk_size_bytes=10 * 1024,
            memory_ttl_seconds=60.0,
            disk_ttl_seconds=3600.0,
            max_item_size_bytes=1024,
        )


def test_get_after_close_fails(db_path: str) -> None:
    """Test that operations after close raise an error."""
    import sqlite3

    cache = DiskBackedCache(
        db_path=db_path,
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=1024,
    )

    cache.close()

    # Operations after close should fail
    with pytest.raises(sqlite3.ProgrammingError):
        cache.get("key1")
