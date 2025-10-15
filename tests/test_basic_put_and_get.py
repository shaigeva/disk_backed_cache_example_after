"""Tests for basic in-memory put/get operations."""

from typing import cast

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class UserModel(CacheableModel):
    """Sample model for cache operations."""

    schema_version: str = "1.0.0"
    name: str
    value: int


def test_after_put_get_returns_object(db_path: str) -> None:
    """After putting an object, get should return it."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=UserModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = UserModel(name="test", value=42)
    cache.put("key1", obj)

    retrieved = cache.get("key1")
    assert retrieved is not None
    retrieved_typed = cast(UserModel, retrieved)
    assert retrieved_typed.name == "test"
    assert retrieved_typed.value == 42

    cache.close()


def test_get_nonexistent_returns_none(db_path: str) -> None:
    """Getting a key that doesn't exist should return None."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=UserModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    retrieved = cache.get("nonexistent")
    assert retrieved is None

    cache.close()


def test_put_overwrites_existing(db_path: str) -> None:
    """Putting to the same key should overwrite the previous value."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=UserModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj1 = UserModel(name="first", value=1)
    cache.put("key1", obj1)

    obj2 = UserModel(name="second", value=2)
    cache.put("key1", obj2)

    retrieved = cache.get("key1")
    assert retrieved is not None
    retrieved_typed = cast(UserModel, retrieved)
    assert retrieved_typed.name == "second"
    assert retrieved_typed.value == 2

    cache.close()


def test_multiple_keys(db_path: str) -> None:
    """Should be able to store and retrieve multiple different keys."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=UserModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj1 = UserModel(name="first", value=1)
    obj2 = UserModel(name="second", value=2)
    obj3 = UserModel(name="third", value=3)

    cache.put("key1", obj1)
    cache.put("key2", obj2)
    cache.put("key3", obj3)

    retrieved1 = cache.get("key1")
    retrieved2 = cache.get("key2")
    retrieved3 = cache.get("key3")

    assert retrieved1 is not None
    assert retrieved2 is not None
    assert retrieved3 is not None

    r1 = cast(UserModel, retrieved1)
    r2 = cast(UserModel, retrieved2)
    r3 = cast(UserModel, retrieved3)

    assert r1.name == "first" and r1.value == 1
    assert r2.name == "second" and r2.value == 2
    assert r3.name == "third" and r3.value == 3

    cache.close()
