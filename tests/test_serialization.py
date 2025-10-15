"""Tests for serialization and deserialization."""

import json
from typing import cast

import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SerializableModel(CacheableModel):
    """Model for serialization tests."""

    schema_version: str = "1.0.0"
    name: str
    count: int
    active: bool


def test_serialize_model_to_json(db_path: str) -> None:
    """Serializing a model should produce valid JSON."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SerializableModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SerializableModel(name="test", count=42, active=True)
    json_str = cache._serialize(obj)  # type: ignore[attr-defined]

    # Should be valid JSON
    data = json.loads(json_str)
    assert data["schema_version"] == "1.0.0"
    assert data["name"] == "test"
    assert data["count"] == 42
    assert data["active"] is True

    cache.close()


def test_deserialize_json_to_model(db_path: str) -> None:
    """Deserializing JSON should produce a valid model."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SerializableModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    json_str = '{"schema_version":"1.0.0","name":"test","count":42,"active":true}'
    obj = cache._deserialize(json_str)  # type: ignore[attr-defined]

    assert isinstance(obj, SerializableModel)
    obj_typed = cast(SerializableModel, obj)
    assert obj_typed.schema_version == "1.0.0"
    assert obj_typed.name == "test"
    assert obj_typed.count == 42
    assert obj_typed.active is True

    cache.close()


def test_roundtrip_serialization(db_path: str) -> None:
    """Serializing and deserializing should preserve data."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SerializableModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    original = SerializableModel(name="roundtrip", count=99, active=False)

    json_str = cache._serialize(original)  # type: ignore[attr-defined]
    restored = cache._deserialize(json_str)  # type: ignore[attr-defined]

    assert isinstance(restored, SerializableModel)
    restored_typed = cast(SerializableModel, restored)
    assert restored_typed.schema_version == original.schema_version
    assert restored_typed.name == original.name
    assert restored_typed.count == original.count
    assert restored_typed.active == original.active

    cache.close()


def test_serialization_size(db_path: str) -> None:
    """Size calculation should match JSON string length."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SerializableModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = SerializableModel(name="size_test", count=123, active=True)

    size = cache._calculate_size(obj)  # type: ignore[attr-defined]
    json_str = cache._serialize(obj)  # type: ignore[attr-defined]

    assert size == len(json_str)
    assert size > 0

    cache.close()


def test_deserialize_invalid_json_raises_error(db_path: str) -> None:
    """Deserializing invalid JSON should raise ValueError."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=SerializableModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Invalid JSON syntax
    with pytest.raises(ValueError, match="Failed to deserialize JSON"):
        cache._deserialize("not valid json")  # type: ignore[attr-defined]

    # Valid JSON but wrong schema
    with pytest.raises(ValueError, match="Failed to deserialize JSON"):
        cache._deserialize('{"wrong": "fields"}')  # type: ignore[attr-defined]

    cache.close()
