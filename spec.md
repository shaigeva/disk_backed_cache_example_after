# Project Specification: Pydantic Object Cache

## Overview
Build a thread-safe, two-tier LRU cache for Pydantic objects with in-memory and SQLite-backed persistent storage. The cache supports configurable size limits, TTLs, and automatic eviction policies.

## Project Structure
```
disk_backed_cache_example/
├── __init__.py
├── disk_backed_cache.py    # DiskBackedCache class (two-tier cache with SQLite)
└── tests/
    ├── __init__.py
    ├── test_basic_put_and_get.py
    ├── test_key_validation.py
    ├── test_model_type_validation.py
    ├── test_serialization.py
    ├── test_sqlite_connection.py
    ├── test_sqlite_operations.py
    ├── test_delete_operations.py
    ├── test_tracking_and_schema.py
    ├── test_lru_eviction.py
    └── ...
```

## Dependencies
- Python 3.10+
- pydantic >= 2.0
- sqlite3 (built-in)
- threading (built-in)
- logging (built-in)

## Architecture

### Two-Tier Design
- **In-Memory Cache**: Fast access, references existing objects (no serialization), limited size, shorter TTL
- **SQLite Disk Cache**: Persistent storage (objects are serialized), larger capacity, longer TTL
- Disk backs memory - evicting from disk also evicts from memory
- Items exceeding max_item_size are stored on disk only

### Eviction Strategy
- **LRU (Least Recently Used)**: Items accessed least recently are evicted first
- **Count-based**: Maximum number of items in cache
- **Size-based**: Maximum total size of all cached items
- **Tie-breaking**: When timestamps are equal, evict alphabetically smallest key

### Concurrency Model
- Thread-safe with read-write locks
- Concurrent reads allowed
- Single writer at a time
- Each operation runs in its own transaction

## Core Components

### 1. CacheableModel

Base class that all cached Pydantic models must inherit from.

```python
from pydantic import BaseModel

class CacheableModel(BaseModel):
    schema_version: str  # Semantic version string (e.g., "1.0.0")
```

**Purpose**: Ensures all cached objects have a schema version for data evolution tracking.

### 2. DiskBackedCache

Main two-tier cache class that handles both in-memory caching and SQLite persistence with concurrent access.

#### Database Schema
```sql
CREATE TABLE cache (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    timestamp REAL NOT NULL,
    schema_version TEXT NOT NULL,
    size INTEGER NOT NULL
)
```

#### Constructor
```python
def __init__(self, db_path: str, expected_schema_version: str)
```
- Opens SQLite connection with WAL mode enabled
- Creates table if not exists
- Cleans up invalid entries (wrong schema version)

#### Public Methods

```python
class DiskBackedCache:
    """
    Two-tier LRU cache with in-memory and SQLite-backed persistent storage.

    Memory tier stores object references (no serialization) for fast access.
    Disk tier stores serialized objects with metadata (timestamp, schema_version, size).
    Provides automatic LRU eviction, TTL expiration, and thread-safe operations.
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
        raise NotImplementedError()

    def get(self, key: str, timestamp: Optional[float] = None) -> Optional[CacheableModel]:
        raise NotImplementedError()

    def put(self, key: str, value: CacheableModel, timestamp: Optional[float] = None) -> None:
        raise NotImplementedError()

    def delete(self, key: str) -> None:
        raise NotImplementedError()

    def get_total_size(self) -> int:
        raise NotImplementedError()

    def get_count(self) -> int:
        raise NotImplementedError()

    def clear(self) -> None:
        raise NotImplementedError()

    def exists(self, key: str, timestamp: Optional[float] = None) -> bool:
        raise NotImplementedError()

    def close(self) -> None:
        raise NotImplementedError()

    def get_stats(self) -> dict[str, int]:
        raise NotImplementedError()

    def put_many(self, items: dict[str, CacheableModel], timestamp: Optional[float] = None) -> None:
        raise NotImplementedError()

    def get_many(self, keys: list[str], timestamp: Optional[float] = None) -> dict[str, CacheableModel]:
        raise NotImplementedError()

    def delete_many(self, keys: list[str]) -> None:
        raise NotImplementedError()
```

#### Implementation Notes
- Use single shared connection
- Enable WAL mode: `PRAGMA journal_mode=WAL`
- Each operation runs in its own transaction
- Use parameterized queries to prevent SQL injection
- Log all operations at TRACE level

#### Initialization Behavior

When `__init__()` is called:
1. Validates directory exists, creates if needed
2. Extracts `schema_version` from the model class
3. Creates SQLite database connection
4. Enables WAL mode
5. Creates cache table if not exists
6. Initializes in-memory data structures
7. Sets up thread locks for concurrency
8. Cleans up invalid items from disk (wrong schema version, exceeding limits)

#### Method Specifications

```python
def get(self, key: str, timestamp: Optional[float] = None) -> Optional[CacheableModel]
```
Retrieves item from cache.

**Behavior:**
1. Checks memory cache first
2. If found in memory, validates TTL
3. If not in memory or expired, checks disk
4. If found on disk, validates TTL and schema version
5. Deserializes and loads into memory (if size permits)
6. Updates timestamp on disk
7. Returns object or None

**Error Handling:**
- Wrong schema_version: logs at TRACE, deletes from disk, returns None
- Deserialization error: logs at TRACE, deletes from disk, returns None
- Invalid key: raises ValueError

```python
def put(self, key: str, value: CacheableModel, timestamp: Optional[float] = None)
```
Stores item in cache.

**Behavior:**
1. Validates key and value
2. Serializes value to JSON
3. Stores to disk
4. Evicts from disk if needed to maintain limits
5. Stores in memory if size <= max_item_size
6. Evicts from memory if needed to maintain limits

**Error Handling:**
- Invalid key: raises ValueError (must be string <= 256 chars)
- Wrong model type: raises TypeError

```python
def delete(self, key: str)
```
Removes item from both memory and disk cache.

```python
def contains(self, key: str) -> bool
```
Checks if key exists in cache (checks both memory and disk).

```python
def clear()
```
Removes all items from cache (both memory and disk). Blocks until complete.

```python
def get_stats() -> dict[str, int]
```
Returns cache statistics including hit/miss rates and eviction counts.

**Returns:**
- Dictionary with keys:
  - `memory_hits`: Number of successful gets from memory
  - `disk_hits`: Number of successful gets from disk (not in memory)
  - `misses`: Number of gets that returned None
  - `memory_evictions`: Number of items evicted from memory
  - `disk_evictions`: Number of items evicted from disk
  - `total_puts`: Number of put operations
  - `total_gets`: Number of get operations
  - `total_deletes`: Number of delete operations
  - `current_memory_items`: Current count in memory
  - `current_disk_items`: Current count on disk

```python
def put_many(items: dict[str, CacheableModel], timestamp: Optional[float] = None)
```
Atomically store multiple items in the cache.

**Behavior:**
1. Validates all keys and values first
2. Uses a single transaction for disk operations
3. All items succeed or all fail (atomic)
4. Updates memory cache after successful disk commit
5. Performs eviction after all items are added
6. All items use the same timestamp if provided
7. Each item counts as a separate put operation in statistics (increments `total_puts` by number of items)

**Error Handling:**
- Invalid key in any item: raises ValueError, no items stored
- Wrong model type in any item: raises TypeError, no items stored
- Disk error: rolls back transaction, no items stored

```python
def get_many(keys: list[str], timestamp: Optional[float] = None) -> dict[str, CacheableModel]
```
Retrieve multiple items from cache.

**Behavior:**
1. Returns dictionary mapping keys to values
2. Keys not found are omitted from result (not included with None)
3. Updates hit/miss statistics for each key (each key counts as a separate get operation)
4. Schema validation applies to each item
5. Does not update access timestamps
6. Each key increments `total_gets` by 1, and increments either `memory_hits`, `disk_hits`, or `misses`

**Returns:**
- Dictionary of found items only (missing keys not included)

```python
def delete_many(keys: list[str])
```
Remove multiple items from cache.

**Behavior:**
1. Deletes from both memory and disk
2. Uses single transaction for disk operations
3. Non-existent keys are silently ignored
4. Partial success is not possible (atomic)
5. Each key counts as a separate delete operation in statistics (increments `total_deletes` by number of keys)


## Statistics and Metrics

The cache tracks the following metrics:
- **Hit/Miss Tracking**: Counts for memory hits, disk hits, and misses
- **Eviction Tracking**: Counts for memory and disk evictions
- **Operation Counts**: Total puts, gets, and deletes
- **Current State**: Current item counts in memory and disk

All statistics are thread-safe and updated atomically with their operations.

## Batch Operations

Batch operations provide atomic multi-item operations:
- **Atomicity**: All items in a batch succeed or all fail
- **Transaction Safety**: Disk operations use a single transaction
- **Performance**: Reduced overhead compared to individual operations
- **Consistency**: All items in a batch use the same timestamp
- **Statistics**: Each item in a batch is counted individually in operation counters
  - `put_many(items)` increments `total_puts` by `len(items)`
  - `get_many(keys)` increments `total_gets` by `len(keys)` and updates hit/miss counters for each key
  - `delete_many(keys)` increments `total_deletes` by `len(keys)`

## Key Behaviors

### Adding and deleting items
- When an item is added, it's added to both memory and disk
- When an item is fetched - if it was only on disk, it's re-added to the in-memory cache.

### LRU Eviction
- Items are evicted based on least recent access time
- `get()` updates the access timestamp
- When timestamps are equal, alphabetically smallest key is evicted first
- Eviction happens one item at a time until under limits

### TTL Expiration
- Memory and disk have separate TTLs
- Expired items are removed when accessed via `get()`
- No background cleanup process - expiration is checked on access

### Schema Version Validation
- Each cached object includes schema version from the model class
- On retrieval, schema version is checked against current model version
- Mismatched versions cause the cached object to be discarded
- Allows for safe data model evolution

### Size Calculation
- Size is calculated from serialized JSON string length
- Used for both size-based eviction and max_item_size check
- Stored in database for efficient total size queries

### Cascading Eviction
- When item is evicted from disk, it's also removed from memory
- This maintains consistency between tiers
- Disk is the source of truth

## Implementation Order

Follow the step-by-step plan in `IMPLEMENTATION_PLAN.md`:
1. **CacheableModel** - Base class definition
2. **Basic Operations** - In-memory put/get, validation, serialization
3. **SQLite Integration** - Database connection, disk operations
4. **Tracking & Metadata** - Counts, sizes, timestamps, schema versions
5. **Statistics** - Hit/miss tracking, eviction counters, operation counters
6. **Batch Operations** - Atomic multi-item operations
7. **LRU Eviction** - Memory and disk eviction policies
8. **Two-Tier Coordination** - Memory-disk integration, promotion, cascading
9. **TTL & Advanced Features** - Expiration, custom timestamps, logging
10. **Thread Safety & Polish** - Concurrency, edge cases, documentation

## Test Requirements

### Basic Operations (test_cache.py)
- Basic get/put/delete operations
- Key validation (length, type)
- Statistics tracking
- Clear operation

### TTL Functionality (test_ttl.py)
- Memory TTL expiration
- Disk TTL expiration
- Timestamp updates on get
- Custom timestamp parameter

### Eviction Logic (test_eviction.py)
- Count-based eviction (memory and disk)
- Size-based eviction (memory and disk)
- LRU order with tie-breaking
- Cascading eviction (disk → memory)
- Max item size (disk-only storage)

### Schema Validation (test_cache.py)
- Schema version matching
- Schema version mismatch handling
- Deserialization error handling

### Concurrency (test_concurrency.py)
- Concurrent reads
- Concurrent read/write
- Thread safety of statistics
- No race conditions during eviction

## Example Usage

```python
from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache

# Define your model
class User(CacheableModel):
    schema_version: str = "1.0.0"
    name: str
    email: str
    age: int

# Create cache
cache = DiskBackedCache(
    db_path="./cache_data/user_cache.db",
    model=User,
    max_memory_items=100,
    max_memory_size_bytes=1024 * 1024,  # 1MB
    max_disk_items=1000,
    max_disk_size_bytes=10 * 1024 * 1024,  # 10MB
    memory_ttl_seconds=60.0,  # 1 minute
    disk_ttl_seconds=3600.0,  # 1 hour
    max_item_size_bytes=10 * 1024,  # 10KB
)

# Store user
user = User(name="Alice", email="alice@example.com", age=30)
cache.put("user:123", user)

# Retrieve user
cached_user = cache.get("user:123")
if cached_user:
    print(f"Found user: {cached_user.name}")

# Check existence
if cache.exists("user:123"):
    print("User exists in cache")

# Clear cache
cache.clear()

# Clean up
cache.close()
```

## Edge Cases to Handle

1. Empty cache operations (get/delete on empty cache)
2. Exactly at limit (putting item when at count/size limit)
3. Oversized items (items larger than max_disk_size)
4. Directory permissions (non-writable directory)
5. Database corruption (SQLite errors)
6. Missing schema_version on model class
7. Concurrent access to same database file

## Success Criteria

- All tests pass with 100% coverage of critical paths
- No race conditions in concurrent operations
- Cache correctly enforces all size and count limits
- TTL expiration works correctly for both tiers
- LRU eviction maintains correct order
- Schema version validation prevents stale data
- Clean, intuitive API
- Comprehensive logging for debugging