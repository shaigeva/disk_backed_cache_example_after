"""Tests for key validation."""

from typing import cast

import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    """Sample model for testing."""

    schema_version: str = "1.0.0"
    data: str


def test_empty_key_raises_valueerror(db_path: str) -> None:
    """Empty key should raise ValueError."""
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

    obj = SampleModel(data="test")

    with pytest.raises(ValueError, match="Key cannot be empty"):
        cache.put("", obj)

    with pytest.raises(ValueError, match="Key cannot be empty"):
        cache.get("")

    cache.close()


def test_key_too_long_raises_valueerror(db_path: str) -> None:
    """Key longer than 256 characters should raise ValueError."""
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

    obj = SampleModel(data="test")
    long_key = "a" * 257  # 257 characters

    with pytest.raises(ValueError, match="Key length 257 exceeds maximum of 256 characters"):
        cache.put(long_key, obj)

    with pytest.raises(ValueError, match="Key length 257 exceeds maximum of 256 characters"):
        cache.get(long_key)

    cache.close()


def test_key_max_length_works(db_path: str) -> None:
    """Key with exactly 256 characters should work."""
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

    obj = SampleModel(data="test")
    max_key = "a" * 256  # Exactly 256 characters

    # Should not raise
    cache.put(max_key, obj)
    retrieved = cache.get(max_key)

    assert retrieved is not None
    retrieved_typed = cast(SampleModel, retrieved)
    assert retrieved_typed.data == "test"

    cache.close()


def test_non_string_key_raises_typeerror(db_path: str) -> None:
    """Non-string key should raise TypeError."""
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

    obj = SampleModel(data="test")

    with pytest.raises(TypeError, match="Key must be a string"):
        cache.put(123, obj)  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="Key must be a string"):
        cache.get(456)  # type: ignore[arg-type]

    cache.close()
