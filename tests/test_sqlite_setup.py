from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


def test_sqlite_connection_created_successfully(db_path: str) -> None:
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

    assert cache._conn is not None
    cache.close()


def test_cache_table_exists_after_init(db_path: str) -> None:
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

    # Query sqlite_master to check if table exists
    cursor = cache._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == "cache"

    # Verify table schema
    cursor = cache._conn.execute("PRAGMA table_info(cache)")
    columns = {row[1]: row[2] for row in cursor.fetchall()}
    assert "key" in columns
    assert "value" in columns
    assert "timestamp" in columns
    assert "schema_version" in columns
    assert "size" in columns

    cache.close()


def test_wal_mode_enabled(db_path: str) -> None:
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

    # Check journal mode
    cursor = cache._conn.execute("PRAGMA journal_mode")
    result = cursor.fetchone()
    assert result is not None
    assert result[0].upper() == "WAL"

    cache.close()


def test_memory_database_works() -> None:
    # Test that :memory: database works
    cache = DiskBackedCache(
        db_path=":memory:",
        model=SampleModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    assert cache._conn is not None
    assert cache._db_path == ":memory:"

    # Verify table exists even in memory database
    cursor = cache._conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cache'")
    result = cursor.fetchone()
    assert result is not None

    cache.close()
