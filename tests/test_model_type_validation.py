"""Tests for model type validation."""

from typing import cast

import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class ModelA(CacheableModel):
    """First test model."""

    schema_version: str = "1.0.0"
    field_a: str


class ModelB(CacheableModel):
    """Second test model."""

    schema_version: str = "1.0.0"
    field_b: int


class ModelASubclass(ModelA):
    """Subclass of ModelA."""

    extra_field: str = "extra"


def test_put_wrong_model_type_raises_typeerror(db_path: str) -> None:
    """Putting a value of the wrong model type should raise TypeError."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ModelA,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    wrong_obj = ModelB(field_b=42)

    with pytest.raises(TypeError, match="Value must be an instance of ModelA, got ModelB"):
        cache.put("key1", wrong_obj)  # type: ignore[arg-type]

    cache.close()


def test_put_correct_model_type_works(db_path: str) -> None:
    """Putting a value of the correct model type should work."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ModelA,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    correct_obj = ModelA(field_a="test")
    cache.put("key1", correct_obj)

    retrieved = cache.get("key1")
    assert retrieved is not None
    retrieved_typed = cast(ModelA, retrieved)
    assert retrieved_typed.field_a == "test"

    cache.close()


def test_put_subclass_works(db_path: str) -> None:
    """Putting a subclass of the declared model should work."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=ModelA,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    subclass_obj = ModelASubclass(field_a="test", extra_field="bonus")
    cache.put("key1", subclass_obj)

    retrieved = cache.get("key1")
    assert retrieved is not None
    retrieved_typed = cast(ModelASubclass, retrieved)
    assert retrieved_typed.field_a == "test"
    assert retrieved_typed.extra_field == "bonus"

    cache.close()
