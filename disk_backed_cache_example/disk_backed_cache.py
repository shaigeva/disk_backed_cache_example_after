import logging
import sqlite3
import threading
import time
from typing import Optional, Type

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class CacheableModel(BaseModel):
    schema_version: str


class DiskBackedCache:
    """
    SQLite-backed storage for cache objects.

    Stores serialized objects with metadata (timestamp, schema_version, size)
    and provides LRU eviction support.
    """

    def __init__(
        self,
        db_path: str,
        model: Type[CacheableModel],
        max_memory_items: int,
        max_memory_size_bytes: int,
        max_disk_items: int,
        max_disk_size_bytes: int,
        memory_ttl_seconds: float,
        disk_ttl_seconds: float,
        max_item_size_bytes: int,  # items larger than this are disk-only
    ) -> None:
        # Validate constructor parameters
        if max_memory_items <= 0:
            raise ValueError("max_memory_items must be positive")
        if max_memory_size_bytes <= 0:
            raise ValueError("max_memory_size_bytes must be positive")
        if max_disk_items <= 0:
            raise ValueError("max_disk_items must be positive")
        if max_disk_size_bytes <= 0:
            raise ValueError("max_disk_size_bytes must be positive")
        if memory_ttl_seconds <= 0:
            raise ValueError("memory_ttl_seconds must be positive")
        if disk_ttl_seconds <= 0:
            raise ValueError("disk_ttl_seconds must be positive")
        if max_item_size_bytes <= 0:
            raise ValueError("max_item_size_bytes must be positive")

        self._model = model
        # Extract expected schema version from model class
        self._expected_schema_version = model.model_fields["schema_version"].default
        # Memory cache stores: (value, size, timestamp, schema_version)
        self._memory_cache: dict[str, tuple[CacheableModel, int, float, str]] = {}
        self._memory_count = 0
        self._memory_total_size = 0

        # Store configuration
        self._max_memory_items = max_memory_items
        self._max_memory_size_bytes = max_memory_size_bytes
        self._max_disk_items = max_disk_items
        self._max_disk_size_bytes = max_disk_size_bytes
        self._max_item_size_bytes = max_item_size_bytes
        self._memory_ttl_seconds = memory_ttl_seconds
        self._disk_ttl_seconds = disk_ttl_seconds

        # Thread safety
        self._lock = threading.RLock()

        # Setup SQLite connection
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)

        # Enable WAL mode for better concurrency
        self._conn.execute("PRAGMA journal_mode=WAL")

        # Create cache table
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                timestamp REAL NOT NULL,
                schema_version TEXT NOT NULL,
                size INTEGER NOT NULL
            )
        """)
        self._conn.commit()

    def get(self, key: str, timestamp: Optional[float] = None) -> Optional[CacheableModel]:
        with self._lock:
            logger.debug("get: key=%s", key)
            current_time = timestamp if timestamp is not None else time.time()

            # Check memory cache first
            cached = self._memory_cache.get(key)
            if cached:
                obj, _, item_timestamp, schema_version = cached
                # Validate schema version against expected
                if schema_version != self._expected_schema_version:
                    # Schema mismatch, remove from cache
                    logger.debug(
                        "get: schema version mismatch for key=%s (expected=%s, got=%s)",
                        key,
                        self._expected_schema_version,
                        schema_version,
                    )
                    self._delete_internal(key)
                    return None
                # Check TTL
                if current_time - item_timestamp > self._memory_ttl_seconds:
                    # Expired, remove from memory
                    logger.debug("get: memory TTL expired for key=%s", key)
                    _, size, _, _ = self._memory_cache[key]
                    del self._memory_cache[key]
                    self._memory_count -= 1
                    self._memory_total_size -= size
                    # Don't return, check disk
                else:
                    logger.debug("get: found in memory cache key=%s", key)
                    return obj

            # Check disk cache
            cursor = self._conn.execute(
                "SELECT value, schema_version, size, timestamp FROM cache WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()
            if row:
                stored_schema = row[1]
                size = row[2]
                item_timestamp = row[3]

                # Check TTL
                if current_time - item_timestamp > self._disk_ttl_seconds:
                    # Expired, remove from disk
                    logger.debug("get: disk TTL expired for key=%s", key)
                    self._delete_internal(key)
                    return None

                # Check schema version first (before expensive deserialization)
                if stored_schema != self._expected_schema_version:
                    # Schema mismatch, remove from disk
                    logger.debug("get: disk schema version mismatch for key=%s", key)
                    self._delete_internal(key)
                    return None
                try:
                    # Deserialize from JSON
                    obj = self._model.model_validate_json(row[0])

                    # Promote to memory if size permits and not too large
                    if size <= self._max_item_size_bytes:
                        # Add to memory (will trigger eviction if needed)
                        # Use disk timestamp to maintain LRU order
                        logger.debug("get: promoting from disk to memory key=%s", key)
                        if key not in self._memory_cache:
                            self._memory_count += 1
                        else:
                            # Replacing, adjust size
                            _, old_size, _, _ = self._memory_cache[key]
                            self._memory_total_size -= old_size

                        self._memory_cache[key] = (obj, size, item_timestamp, obj.schema_version)
                        self._memory_total_size += size
                        self._evict_memory_if_needed()
                    else:
                        logger.debug("get: found on disk (disk-only) key=%s", key)

                    return obj
                except Exception as e:
                    # If deserialization fails, delete and return None
                    logger.debug("get: deserialization error for key=%s: %s", key, e)
                    self._delete_internal(key)
                    return None

            logger.debug("get: key not found key=%s", key)
            return None

    def put(self, key: str, value: CacheableModel, timestamp: Optional[float] = None) -> None:
        with self._lock:
            logger.debug("put: key=%s", key)
            self._validate_key(key)
            self._validate_model(value)
            serialized = value.model_dump_json()
            size = len(serialized)
            current_time = timestamp if timestamp is not None else time.time()

            # Store in SQLite
            logger.debug("put: storing to disk key=%s size=%d", key, size)
            self._conn.execute(
                """
                INSERT OR REPLACE INTO cache (key, value, timestamp, schema_version, size)
                VALUES (?, ?, ?, ?, ?)
                """,
                (key, serialized, current_time, value.schema_version, size),
            )
            self._conn.commit()

            # Evict from disk if needed
            self._evict_disk_if_needed()

            # Store in memory only if size permits (disk-only for large items)
            if size <= self._max_item_size_bytes:
                # Store in memory and update tracking
                logger.debug("put: storing to memory key=%s", key)
                if key in self._memory_cache:
                    # Replacing existing item, adjust size
                    _, old_size, _, _ = self._memory_cache[key]
                    self._memory_total_size -= old_size
                else:
                    # New item
                    self._memory_count += 1

                self._memory_cache[key] = (value, size, current_time, value.schema_version)
                self._memory_total_size += size

                # Evict from memory if needed
                self._evict_memory_if_needed()
            else:
                # Item is too large for memory, disk-only
                logger.debug("put: item too large for memory, disk-only key=%s size=%d", key, size)
                # Remove from memory if it was there
                if key in self._memory_cache:
                    _, old_size, _, _ = self._memory_cache[key]
                    del self._memory_cache[key]
                    self._memory_count -= 1
                    self._memory_total_size -= old_size

    def _validate_key(self, key: object) -> None:
        if not isinstance(key, str):
            raise ValueError("Key must be a string")
        if len(key) > 256:
            raise ValueError("Key must be 256 characters or less")

    def _validate_model(self, value: object) -> None:
        if not isinstance(value, self._model):
            raise TypeError(f"Value must be an instance of {self._model.__name__}")

    def delete(self, key: str) -> None:
        with self._lock:
            self._delete_internal(key)

    def _delete_internal(self, key: str) -> None:
        """Internal delete without locking (assumes lock is held)."""
        logger.debug("delete: key=%s", key)
        # Delete from memory cache and update tracking
        if key in self._memory_cache:
            _, size, _, _ = self._memory_cache[key]
            del self._memory_cache[key]
            self._memory_count -= 1
            self._memory_total_size -= size
            logger.debug("delete: removed from memory key=%s", key)

        # Delete from SQLite
        self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
        self._conn.commit()
        logger.debug("delete: removed from disk key=%s", key)

    def get_total_size(self) -> int:
        with self._lock:
            # Get disk total size
            cursor = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM cache")
            disk_size = cursor.fetchone()[0]
            return self._memory_total_size + disk_size

    def get_count(self) -> int:
        with self._lock:
            # Get disk count
            cursor = self._conn.execute("SELECT COUNT(*) FROM cache")
            disk_count = cursor.fetchone()[0]
            return self._memory_count + disk_count

    def clear(self) -> None:
        with self._lock:
            logger.debug("clear: clearing all cache entries")
            # Clear memory cache
            self._memory_cache.clear()
            self._memory_count = 0
            self._memory_total_size = 0

            # Clear disk cache
            self._conn.execute("DELETE FROM cache")
            self._conn.commit()
            logger.debug("clear: cache cleared")

    def exists(self, key: str) -> bool:
        with self._lock:
            # Check memory first
            if key in self._memory_cache:
                return True

            # Check disk
            cursor = self._conn.execute("SELECT 1 FROM cache WHERE key = ? LIMIT 1", (key,))
            return cursor.fetchone() is not None

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _evict_memory_if_needed(self) -> None:
        """Evict items from memory cache to meet size and count limits."""
        # Evict based on count
        while self._memory_count > self._max_memory_items:
            self._evict_one_from_memory()

        # Evict based on size
        while self._memory_total_size > self._max_memory_size_bytes:
            self._evict_one_from_memory()

    def _evict_one_from_memory(self) -> None:
        """Evict the least recently used item from memory cache."""
        if not self._memory_cache:
            return

        # Find oldest item by timestamp (with alphabetical tie-breaking)
        oldest_key = min(
            self._memory_cache.keys(),
            key=lambda k: (self._memory_cache[k][2], k),  # (timestamp, key)
        )

        # Remove it
        logger.debug("evict: evicting from memory key=%s", oldest_key)
        _, size, _, _ = self._memory_cache[oldest_key]
        del self._memory_cache[oldest_key]
        self._memory_count -= 1
        self._memory_total_size -= size

    def _evict_disk_if_needed(self) -> None:
        """Evict items from disk cache to meet size and count limits."""
        # Evict based on count
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache")
        count = cursor.fetchone()[0]
        while count > self._max_disk_items:
            self._evict_one_from_disk()
            count -= 1

        # Evict based on size
        cursor = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM cache")
        total_size = cursor.fetchone()[0]
        while total_size > self._max_disk_size_bytes:
            # Find size of item to be evicted
            cursor = self._conn.execute("SELECT key, size FROM cache ORDER BY timestamp ASC, key ASC LIMIT 1")
            row = cursor.fetchone()
            if not row:
                break
            size_to_evict = row[1]
            self._evict_one_from_disk()
            total_size -= size_to_evict

    def _evict_one_from_disk(self) -> None:
        """Evict the least recently used item from disk cache."""
        # Find oldest item (with alphabetical tie-breaking)
        cursor = self._conn.execute("SELECT key FROM cache ORDER BY timestamp ASC, key ASC LIMIT 1")
        row = cursor.fetchone()
        if row:
            key_to_evict = row[0]
            logger.debug("evict: evicting from disk key=%s", key_to_evict)
            # Remove from both disk and memory (cascading eviction)
            # Use internal delete since we already hold the lock
            self._delete_internal(key_to_evict)
