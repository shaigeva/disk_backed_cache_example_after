import json
import logging
import os
import sqlite3
import threading
import time
from typing import Optional, Type

from pydantic import BaseModel, ConfigDict, ValidationError

# Define TRACE level (below DEBUG which is 10)
TRACE = 5
logging.addLevelName(TRACE, "TRACE")

# Set up logger for this module
logger = logging.getLogger(__name__)


class CacheableModel(BaseModel):
    model_config = ConfigDict(frozen=True)  # Makes objects immutable, otherwise cached objects can be modified...
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
        self._db_path = db_path
        self._model = model
        self._max_memory_items = max_memory_items
        self._max_memory_size_bytes = max_memory_size_bytes
        self._max_disk_items = max_disk_items
        self._max_disk_size_bytes = max_disk_size_bytes
        self._memory_ttl_seconds = memory_ttl_seconds
        self._disk_ttl_seconds = disk_ttl_seconds
        self._max_item_size_bytes = max_item_size_bytes

        # Extract schema version from model
        self._schema_version = self._extract_schema_version()

        # In-memory cache storage
        self._memory_cache: dict[str, CacheableModel] = {}
        self._memory_timestamps: dict[str, float] = {}

        # Setup SQLite connection
        self._setup_database()

        # Initialize counters
        self._memory_item_count = 0
        self._memory_total_size = 0

        # Initialize statistics counters
        self._stats_memory_hits = 0
        self._stats_disk_hits = 0
        self._stats_misses = 0
        self._stats_memory_evictions = 0
        self._stats_disk_evictions = 0
        self._stats_total_puts = 0
        self._stats_total_gets = 0
        self._stats_total_deletes = 0

        # Thread safety lock
        self._lock = threading.RLock()

    def _extract_schema_version(self) -> str:
        """Extract schema_version from the model class."""
        # Check if there's a default value on the model
        if hasattr(self._model, "model_fields"):
            schema_field = self._model.model_fields.get("schema_version")
            if schema_field and schema_field.default is not None:
                return str(schema_field.default)

        # If no default, require it to be set
        raise ValueError(f"Model {self._model.__name__} must define a default schema_version")

    def _setup_database(self) -> None:
        """Setup SQLite database connection and create tables."""
        # Create parent directory if needed (skip for :memory: database)
        if self._db_path != ":memory:":
            db_dir = os.path.dirname(self._db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

        # Open connection
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)

        # Enable WAL mode
        self._conn.execute("PRAGMA journal_mode=WAL")

        # Create table
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                timestamp REAL NOT NULL,
                schema_version TEXT NOT NULL,
                size INTEGER NOT NULL
            )
            """
        )
        self._conn.commit()

    def _validate_key(self, key: str) -> None:
        """Validate that key is a valid string within length limits."""
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
        if len(key) == 0:
            raise ValueError("Key cannot be empty")
        if len(key) > 256:
            raise ValueError(f"Key length {len(key)} exceeds maximum of 256 characters")

    def _serialize(self, value: CacheableModel) -> str:
        """Serialize a CacheableModel to JSON string."""
        return value.model_dump_json()

    def _deserialize(self, json_str: str) -> CacheableModel:
        """Deserialize JSON string to CacheableModel.

        Raises:
            ValueError: If JSON is invalid or doesn't match model schema
        """
        try:
            return self._model.model_validate_json(json_str)
        except (ValidationError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to deserialize JSON: {e}") from e

    def _calculate_size(self, value: CacheableModel) -> int:
        """Calculate the size of a serialized object in bytes."""
        return len(self._serialize(value))

    def _evict_from_memory_by_count(self) -> None:
        """Evict items from memory when count exceeds max_memory_items.

        Evicts least recently used items one at a time.
        Tie-breaking: alphabetically smallest key when timestamps equal.
        """
        while self._memory_item_count > self._max_memory_items:
            # Find least recently used item
            lru_key = min(
                self._memory_timestamps.keys(),
                key=lambda k: (self._memory_timestamps[k], k),  # Sort by (timestamp, key)
            )

            logger.log(TRACE, f"evicting from memory (count): key={lru_key!r}")
            # Remove from memory (keep on disk)
            obj_size = self._calculate_size(self._memory_cache[lru_key])
            del self._memory_cache[lru_key]
            del self._memory_timestamps[lru_key]
            self._memory_item_count -= 1
            self._memory_total_size -= obj_size

            # Update statistics
            self._stats_memory_evictions += 1

    def _evict_from_memory_by_size(self) -> None:
        """Evict items from memory when size exceeds max_memory_size_bytes.

        Evicts least recently used items one at a time.
        Tie-breaking: alphabetically smallest key when timestamps equal.
        """
        while self._memory_total_size > self._max_memory_size_bytes:
            # Find least recently used item
            lru_key = min(
                self._memory_timestamps.keys(),
                key=lambda k: (self._memory_timestamps[k], k),  # Sort by (timestamp, key)
            )

            logger.log(TRACE, f"evicting from memory (size): key={lru_key!r}")
            # Remove from memory (keep on disk)
            obj_size = self._calculate_size(self._memory_cache[lru_key])
            del self._memory_cache[lru_key]
            del self._memory_timestamps[lru_key]
            self._memory_item_count -= 1
            self._memory_total_size -= obj_size

            # Update statistics
            self._stats_memory_evictions += 1

    def _evict_from_disk_by_count(self) -> None:
        """Evict items from disk when count exceeds max_disk_items.

        Evicts least recently used items one at a time.
        Tie-breaking: alphabetically smallest key when timestamps equal.
        Cascades to memory: evicted items also removed from memory.
        """
        # Get current disk count
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache")
        disk_count = cursor.fetchone()[0]

        while disk_count > self._max_disk_items:
            # Find least recently used item from disk
            cursor = self._conn.execute("SELECT key, timestamp FROM cache ORDER BY timestamp ASC, key ASC LIMIT 1")
            row = cursor.fetchone()

            if row is None:
                break

            lru_key = row[0]

            logger.log(TRACE, f"evicting from disk (count): key={lru_key!r}")
            # Remove from disk
            self._conn.execute("DELETE FROM cache WHERE key = ?", (lru_key,))
            self._conn.commit()

            # Cascade: Remove from memory if present
            if lru_key in self._memory_cache:
                obj_size = self._calculate_size(self._memory_cache[lru_key])
                del self._memory_cache[lru_key]
                del self._memory_timestamps[lru_key]
                self._memory_item_count -= 1
                self._memory_total_size -= obj_size

            # Update statistics
            self._stats_disk_evictions += 1

            # Update count
            disk_count -= 1

    def _evict_from_disk_by_size(self) -> None:
        """Evict items from disk when size exceeds max_disk_size_bytes.

        Evicts least recently used items one at a time.
        Tie-breaking: alphabetically smallest key when timestamps equal.
        Cascades to memory: evicted items also removed from memory.
        """
        # Get current disk size
        cursor = self._conn.execute("SELECT SUM(size) FROM cache")
        result = cursor.fetchone()[0]
        disk_size = result if result is not None else 0

        while disk_size > self._max_disk_size_bytes:
            # Find least recently used item from disk
            cursor = self._conn.execute(
                "SELECT key, timestamp, size FROM cache ORDER BY timestamp ASC, key ASC LIMIT 1"
            )
            row = cursor.fetchone()

            if row is None:
                break

            lru_key, _, item_size = row

            logger.log(TRACE, f"evicting from disk (size): key={lru_key!r}")
            # Remove from disk
            self._conn.execute("DELETE FROM cache WHERE key = ?", (lru_key,))
            self._conn.commit()

            # Cascade: Remove from memory if present
            if lru_key in self._memory_cache:
                obj_size = self._calculate_size(self._memory_cache[lru_key])
                del self._memory_cache[lru_key]
                del self._memory_timestamps[lru_key]
                self._memory_item_count -= 1
                self._memory_total_size -= obj_size

            # Update statistics
            self._stats_disk_evictions += 1

            # Update size
            disk_size -= item_size

    def get(self, key: str, timestamp: Optional[float] = None) -> Optional[CacheableModel]:
        """Retrieve item from cache, checking memory first then disk."""
        self._validate_key(key)
        logger.log(TRACE, f"get(key={key!r})")

        with self._lock:
            self._stats_total_gets += 1

        if timestamp is None:
            timestamp = time.time()

        # Check memory first
        if key in self._memory_cache:
            # Check TTL
            memory_timestamp = self._memory_timestamps[key]
            if timestamp - memory_timestamp > self._memory_ttl_seconds:
                # Expired - remove from memory and continue to disk check
                logger.log(TRACE, f"get(key={key!r}): expired from memory (TTL exceeded)")
                obj_size = self._calculate_size(self._memory_cache[key])
                del self._memory_cache[key]
                del self._memory_timestamps[key]
                self._memory_item_count -= 1
                self._memory_total_size -= obj_size
            else:
                # Not expired - update timestamps for LRU
                logger.log(TRACE, f"get(key={key!r}): memory hit")
                self._memory_timestamps[key] = timestamp
                # Also update disk timestamp to keep them in sync
                self._conn.execute(
                    "UPDATE cache SET timestamp = ? WHERE key = ?",
                    (timestamp, key),
                )
                self._conn.commit()
                self._stats_memory_hits += 1
                return self._memory_cache[key]

        # Check disk
        cursor = self._conn.execute(
            "SELECT value, schema_version, timestamp FROM cache WHERE key = ?",
            (key,),
        )
        row = cursor.fetchone()

        if row is None:
            logger.log(TRACE, f"get(key={key!r}): miss (not found)")
            self._stats_misses += 1
            return None

        value_json, stored_schema_version, disk_timestamp = row

        # Check TTL
        if timestamp - disk_timestamp > self._disk_ttl_seconds:
            # Expired - delete and return None
            logger.log(TRACE, f"get(key={key!r}): expired from disk (TTL exceeded)")
            self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            self._conn.commit()
            self._stats_misses += 1
            return None

        # Validate schema version
        if stored_schema_version != self._schema_version:
            # Schema mismatch - delete and return None
            logger.log(
                TRACE,
                f"get(key={key!r}): schema version mismatch (stored={stored_schema_version!r}, expected={self._schema_version!r})",
            )
            self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            self._conn.commit()
            self._stats_misses += 1
            return None

        # Deserialize
        try:
            obj = self._deserialize(value_json)
        except ValueError as e:
            # Deserialization failed - delete and return None
            logger.log(TRACE, f"get(key={key!r}): deserialization failed: {e}")
            self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            self._conn.commit()
            self._stats_misses += 1
            return None

        # Update timestamp on disk
        self._conn.execute(
            "UPDATE cache SET timestamp = ? WHERE key = ?",
            (timestamp, key),
        )
        self._conn.commit()

        # Load into memory (only if size <= max_item_size_bytes)
        obj_size = len(value_json)
        if obj_size <= self._max_item_size_bytes:
            if key not in self._memory_cache:
                self._memory_item_count += 1
                self._memory_total_size += obj_size
            else:
                # Update size
                old_size = self._calculate_size(self._memory_cache[key])
                self._memory_total_size = self._memory_total_size - old_size + obj_size
            self._memory_cache[key] = obj
            self._memory_timestamps[key] = timestamp

            # Evict from memory if needed
            self._evict_from_memory_by_count()
            self._evict_from_memory_by_size()

        logger.log(TRACE, f"get(key={key!r}): disk hit")
        self._stats_disk_hits += 1
        return obj

    def put(self, key: str, value: CacheableModel, timestamp: Optional[float] = None) -> None:
        """Store item in both memory and disk cache."""
        self._validate_key(key)
        logger.log(TRACE, f"put(key={key!r})")
        self._stats_total_puts += 1

        # Validate model type
        if not isinstance(value, self._model):
            raise TypeError(f"Value must be an instance of {self._model.__name__}, got {type(value).__name__}")

        if timestamp is None:
            timestamp = time.time()

        # Serialize and calculate size
        value_json = self._serialize(value)
        size = len(value_json)

        # Validate size doesn't exceed disk limit
        if size > self._max_disk_size_bytes:
            raise ValueError(
                f"Item size ({size} bytes) exceeds max_disk_size_bytes ({self._max_disk_size_bytes} bytes)"
            )

        # Store to disk
        self._conn.execute(
            """
            INSERT OR REPLACE INTO cache (key, value, timestamp, schema_version, size)
            VALUES (?, ?, ?, ?, ?)
            """,
            (key, value_json, timestamp, self._schema_version, size),
        )
        self._conn.commit()

        # Store in memory (only if size <= max_item_size_bytes)
        if size <= self._max_item_size_bytes:
            if key not in self._memory_cache:
                self._memory_item_count += 1
                self._memory_total_size += size
            else:
                # Update size (subtract old, add new)
                old_size = self._calculate_size(self._memory_cache[key])
                self._memory_total_size = self._memory_total_size - old_size + size
            self._memory_cache[key] = value
            self._memory_timestamps[key] = timestamp

            # Evict from memory if needed
            self._evict_from_memory_by_count()
            self._evict_from_memory_by_size()
        else:
            # Item too large for memory - remove from memory if present
            if key in self._memory_cache:
                obj_size = self._calculate_size(self._memory_cache[key])
                del self._memory_cache[key]
                del self._memory_timestamps[key]
                self._memory_item_count -= 1
                self._memory_total_size -= obj_size

        # Evict from disk if needed
        self._evict_from_disk_by_count()
        self._evict_from_disk_by_size()

    def put_many(self, items: dict[str, CacheableModel], timestamp: Optional[float] = None) -> None:
        """Atomically store multiple items in the cache.

        All items succeed or all fail. Uses a single transaction for disk operations.
        """
        logger.log(TRACE, f"put_many(count={len(items)})")
        # Validate all keys first
        for key in items.keys():
            self._validate_key(key)

        # Validate all values first
        for value in items.values():
            if not isinstance(value, self._model):
                raise TypeError(f"Value must be an instance of {self._model.__name__}, got {type(value).__name__}")

        # If no items, return early
        if not items:
            return

        # Get timestamp
        if timestamp is None:
            timestamp = time.time()

        # Prepare serialized data for all items
        serialized_items: dict[str, tuple[str, int]] = {}
        for key, value in items.items():
            value_json = self._serialize(value)
            size = len(value_json)

            # Validate size doesn't exceed disk limit
            if size > self._max_disk_size_bytes:
                raise ValueError(
                    f"Item size for key {key!r} ({size} bytes) exceeds max_disk_size_bytes ({self._max_disk_size_bytes} bytes)"
                )

            serialized_items[key] = (value_json, size)

        # Store all items to disk in a single transaction
        try:
            # Begin transaction explicitly
            self._conn.execute("BEGIN")

            for key, (value_json, size) in serialized_items.items():
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO cache (key, value, timestamp, schema_version, size)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (key, value_json, timestamp, self._schema_version, size),
                )

            # Commit transaction
            self._conn.commit()
        except Exception:
            # Rollback on error
            self._conn.rollback()
            raise

        # Update memory cache after successful disk commit (only for items <= max_item_size_bytes)
        for key, value in items.items():
            size = serialized_items[key][1]

            if size <= self._max_item_size_bytes:
                if key not in self._memory_cache:
                    self._memory_item_count += 1
                    self._memory_total_size += size
                else:
                    # Update size (subtract old, add new)
                    old_size = self._calculate_size(self._memory_cache[key])
                    self._memory_total_size = self._memory_total_size - old_size + size

                self._memory_cache[key] = value
                self._memory_timestamps[key] = timestamp
            else:
                # Item too large for memory - remove from memory if present
                if key in self._memory_cache:
                    obj_size = self._calculate_size(self._memory_cache[key])
                    del self._memory_cache[key]
                    del self._memory_timestamps[key]
                    self._memory_item_count -= 1
                    self._memory_total_size -= obj_size

        # Evict from memory if needed
        self._evict_from_memory_by_count()
        self._evict_from_memory_by_size()

        # Evict from disk if needed
        self._evict_from_disk_by_count()
        self._evict_from_disk_by_size()

        # Update statistics - each item counts as a separate put
        self._stats_total_puts += len(items)

    def get_many(self, keys: list[str], timestamp: Optional[float] = None) -> dict[str, CacheableModel]:
        """Retrieve multiple items from cache.

        Returns dictionary of found items only (missing keys omitted).
        Does not update access timestamps.
        """
        logger.log(TRACE, f"get_many(count={len(keys)})")
        # Validate all keys first
        for key in keys:
            self._validate_key(key)

        result: dict[str, CacheableModel] = {}

        # Get current timestamp if not provided
        if timestamp is None:
            timestamp = time.time()

        # Process each key
        for key in keys:
            # Increment total_gets for each key
            self._stats_total_gets += 1

            # Check memory first
            if key in self._memory_cache:
                # Check memory TTL
                memory_timestamp = self._memory_timestamps[key]
                if timestamp - memory_timestamp > self._memory_ttl_seconds:
                    # Expired - remove from memory and continue to disk check
                    obj_size = self._calculate_size(self._memory_cache[key])
                    del self._memory_cache[key]
                    del self._memory_timestamps[key]
                    self._memory_item_count -= 1
                    self._memory_total_size -= obj_size
                else:
                    # Not expired
                    result[key] = self._memory_cache[key]
                    self._stats_memory_hits += 1
                    continue

            # Check disk
            cursor = self._conn.execute(
                "SELECT value, schema_version, timestamp FROM cache WHERE key = ?",
                (key,),
            )
            row = cursor.fetchone()

            if row is None:
                self._stats_misses += 1
                continue

            value_json, stored_schema_version, disk_timestamp = row

            # Check disk TTL
            if timestamp - disk_timestamp > self._disk_ttl_seconds:
                # Expired - delete and count as miss
                self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                self._conn.commit()
                self._stats_misses += 1
                continue

            # Validate schema version
            if stored_schema_version != self._schema_version:
                # Schema mismatch - delete and count as miss
                self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                self._conn.commit()
                self._stats_misses += 1
                continue

            # Deserialize
            try:
                obj = self._deserialize(value_json)
            except ValueError:
                # Deserialization failed - delete and count as miss
                self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                self._conn.commit()
                self._stats_misses += 1
                continue

            # Add to result (but don't update timestamp or promote to memory)
            result[key] = obj
            self._stats_disk_hits += 1

        return result

    def delete_many(self, keys: list[str]) -> None:
        """Remove multiple items from cache.

        Non-existent keys are silently ignored. All deletes are atomic.
        """
        logger.log(TRACE, f"delete_many(count={len(keys)})")
        # Validate all keys first
        for key in keys:
            self._validate_key(key)

        # Remove from memory first
        for key in keys:
            if key in self._memory_cache:
                obj_size = self._calculate_size(self._memory_cache[key])
                del self._memory_cache[key]
                del self._memory_timestamps[key]
                self._memory_item_count -= 1
                self._memory_total_size -= obj_size

        # Remove from disk in a single transaction
        try:
            # Begin transaction explicitly
            self._conn.execute("BEGIN")

            for key in keys:
                self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))

            # Commit transaction
            self._conn.commit()
        except Exception:
            # Rollback on error (memory changes can't be rolled back)
            self._conn.rollback()
            raise

        # Update statistics - each key counts as a separate delete
        self._stats_total_deletes += len(keys)

    def delete(self, key: str) -> None:
        """Remove item from both memory and disk cache."""
        self._validate_key(key)
        logger.log(TRACE, f"delete(key={key!r})")
        self._stats_total_deletes += 1

        # Remove from memory if present
        if key in self._memory_cache:
            obj_size = self._calculate_size(self._memory_cache[key])
            del self._memory_cache[key]
            del self._memory_timestamps[key]
            self._memory_item_count -= 1
            self._memory_total_size -= obj_size

        # Remove from disk
        self._conn.execute("DELETE FROM cache WHERE key = ?", (key,))
        self._conn.commit()

    def get_total_size(self) -> int:
        """Get total size of items in cache (from disk)."""
        # Get disk total size
        cursor = self._conn.execute("SELECT SUM(size) FROM cache")
        result = cursor.fetchone()[0]

        # Return 0 if no items (SUM returns None for empty table)
        return result if result is not None else 0

    def get_count(self) -> int:
        """Get total count of unique items in cache (memory + disk)."""
        # Get disk count
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache")
        disk_count = cursor.fetchone()[0]

        # Items in memory are also on disk, so we just return disk count
        # (which represents the total unique items)
        return disk_count

    def clear(self) -> None:
        """Remove all items from cache (both memory and disk)."""
        logger.log(TRACE, "clear()")
        # Clear memory
        self._memory_cache.clear()
        self._memory_timestamps.clear()
        self._memory_item_count = 0
        self._memory_total_size = 0

        # Clear disk
        self._conn.execute("DELETE FROM cache")
        self._conn.commit()

    def exists(self, key: str, timestamp: Optional[float] = None) -> bool:
        """Check if key exists in cache (memory or disk)."""
        self._validate_key(key)
        logger.log(TRACE, f"exists(key={key!r})")

        # Check memory first
        if key in self._memory_cache:
            return True

        # Check disk
        cursor = self._conn.execute(
            "SELECT COUNT(*) FROM cache WHERE key = ?",
            (key,),
        )
        count = cursor.fetchone()[0]
        return count > 0

    def get_stats(self) -> dict[str, int]:
        """Get cache statistics."""
        # Get current disk count
        cursor = self._conn.execute("SELECT COUNT(*) FROM cache")
        disk_count = cursor.fetchone()[0]

        return {
            "memory_hits": self._stats_memory_hits,
            "disk_hits": self._stats_disk_hits,
            "misses": self._stats_misses,
            "memory_evictions": self._stats_memory_evictions,
            "disk_evictions": self._stats_disk_evictions,
            "total_puts": self._stats_total_puts,
            "total_gets": self._stats_total_gets,
            "total_deletes": self._stats_total_deletes,
            "current_memory_items": self._memory_item_count,
            "current_disk_items": disk_count,
        }

    def close(self) -> None:
        """Close the database connection."""
        if hasattr(self, "_conn"):
            self._conn.close()
