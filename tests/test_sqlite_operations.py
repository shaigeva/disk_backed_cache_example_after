"""Tests for SQLite put/get operations."""

from typing import cast

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class DataModel(CacheableModel):
    """Model for SQLite operation tests."""

    schema_version: str = "1.0.0"
    content: str
    number: int


def test_put_stores_to_disk(db_path: str) -> None:
    """Put operation should store item in SQLite database."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=DataModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = DataModel(content="test data", number=42)
    cache.put("key1", obj)

    # Query disk directly
    cursor = cache._conn.execute("SELECT key, value, schema_version FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    row = cursor.fetchone()

    assert row is not None
    key, value_json, schema_version = row
    assert key == "key1"
    assert schema_version == "1.0.0"
    assert "test data" in value_json
    assert "42" in value_json

    cache.close()


def test_get_retrieves_from_disk(db_path: str) -> None:
    """Get should retrieve item from disk when not in memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=DataModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = DataModel(content="disk test", number=99)
    cache.put("key1", obj)

    # Clear memory cache
    cache._memory_cache.clear()  # type: ignore[attr-defined]

    # Should still retrieve from disk
    retrieved = cache.get("key1")
    assert retrieved is not None
    retrieved_typed = cast(DataModel, retrieved)
    assert retrieved_typed.content == "disk test"
    assert retrieved_typed.number == 99

    cache.close()


def test_disk_stores_metadata(db_path: str) -> None:
    """Disk storage should include timestamp, schema_version, and size."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=DataModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = DataModel(content="metadata test", number=123)
    cache.put("key1", obj, timestamp=1000.0)

    # Query all metadata
    cursor = cache._conn.execute(  # type: ignore[attr-defined]
        "SELECT timestamp, schema_version, size FROM cache WHERE key = ?",
        ("key1",),
    )
    row = cursor.fetchone()

    assert row is not None
    timestamp, schema_version, size = row
    assert timestamp == 1000.0
    assert schema_version == "1.0.0"
    assert size > 0  # Should have non-zero size

    cache.close()


def test_put_updates_disk_entry(db_path: str) -> None:
    """Putting the same key twice should update the disk entry."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=DataModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj1 = DataModel(content="first", number=1)
    cache.put("key1", obj1)

    obj2 = DataModel(content="second", number=2)
    cache.put("key1", obj2)

    # Should only have one entry in disk
    cursor = cache._conn.execute("SELECT COUNT(*) FROM cache")  # type: ignore[attr-defined]
    count = cursor.fetchone()[0]
    assert count == 1

    # Should have the latest value
    cursor = cache._conn.execute("SELECT value FROM cache WHERE key = ?", ("key1",))  # type: ignore[attr-defined]
    value_json = cursor.fetchone()[0]
    assert "second" in value_json
    assert "2" in value_json

    cache.close()


def test_get_from_disk_promotes_to_memory(db_path: str) -> None:
    """Getting from disk should load the item into memory."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=DataModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = DataModel(content="promote test", number=777)
    cache.put("key1", obj)

    # Clear memory cache
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    assert "key1" not in cache._memory_cache  # type: ignore[attr-defined]

    # Get from disk
    cache.get("key1")

    # Should now be in memory
    assert "key1" in cache._memory_cache  # type: ignore[attr-defined]
    mem_obj = cache._memory_cache["key1"]  # type: ignore[attr-defined]
    mem_obj_typed = cast(DataModel, mem_obj)
    assert mem_obj_typed.content == "promote test"
    assert mem_obj_typed.number == 777

    cache.close()
