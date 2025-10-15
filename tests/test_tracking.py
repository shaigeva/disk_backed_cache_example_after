"""Tests for count and size tracking."""

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class TrackModel(CacheableModel):
    """Model for tracking tests."""

    schema_version: str = "1.0.0"
    data: str


def test_get_count_starts_at_zero(db_path: str) -> None:
    """Initial count should be zero."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    assert cache.get_count() == 0

    cache.close()


def test_get_count_increases_after_put(db_path: str) -> None:
    """Count should increase when items are added."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", TrackModel(data="test1"))
    assert cache.get_count() == 1

    cache.put("key2", TrackModel(data="test2"))
    assert cache.get_count() == 2

    cache.put("key3", TrackModel(data="test3"))
    assert cache.get_count() == 3

    cache.close()


def test_get_count_does_not_increase_on_overwrite(db_path: str) -> None:
    """Count should not increase when overwriting existing key."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", TrackModel(data="test1"))
    assert cache.get_count() == 1

    cache.put("key1", TrackModel(data="test1_updated"))
    assert cache.get_count() == 1

    cache.close()


def test_get_count_decreases_after_delete(db_path: str) -> None:
    """Count should decrease when items are deleted."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", TrackModel(data="test1"))
    cache.put("key2", TrackModel(data="test2"))
    assert cache.get_count() == 2

    cache.delete("key1")
    assert cache.get_count() == 1

    cache.delete("key2")
    assert cache.get_count() == 0

    cache.close()


def test_get_count_includes_disk_items(db_path: str) -> None:
    """Count should include items stored on disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", TrackModel(data="test1"))
    cache.put("key2", TrackModel(data="test2"))

    # Clear memory but keep disk
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_item_count = 0  # type: ignore[attr-defined]

    # Count should still be 2 (from disk)
    assert cache.get_count() == 2

    cache.close()


def test_get_total_size_starts_at_zero(db_path: str) -> None:
    """Initial size should be zero."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    assert cache.get_total_size() == 0

    cache.close()


def test_get_total_size_increases_after_put(db_path: str) -> None:
    """Size should increase when items are added."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", TrackModel(data="test1"))
    size1 = cache.get_total_size()
    assert size1 > 0

    cache.put("key2", TrackModel(data="test2"))
    size2 = cache.get_total_size()
    assert size2 > size1

    cache.close()


def test_get_total_size_decreases_after_delete(db_path: str) -> None:
    """Size should decrease when items are deleted."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    cache.put("key1", TrackModel(data="test1"))
    cache.put("key2", TrackModel(data="test2"))
    size_before = cache.get_total_size()

    cache.delete("key1")
    size_after = cache.get_total_size()

    assert size_after < size_before
    assert size_after > 0

    cache.delete("key2")
    assert cache.get_total_size() == 0

    cache.close()


def test_get_total_size_includes_disk_items(db_path: str) -> None:
    """Size should include items stored on disk."""
    cache = DiskBackedCache(
        db_path=db_path,
        model=TrackModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    # Add items
    cache.put("key1", TrackModel(data="test1"))
    cache.put("key2", TrackModel(data="test2"))
    total_size = cache.get_total_size()

    # Clear memory but keep disk
    cache._memory_cache.clear()  # type: ignore[attr-defined]
    cache._memory_total_size = 0  # type: ignore[attr-defined]

    # Size should still be the same (from disk)
    assert cache.get_total_size() == total_size

    cache.close()
