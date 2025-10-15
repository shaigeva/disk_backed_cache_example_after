# DiskBackedCache

A high-performance, two-tier LRU cache for Pydantic objects with in-memory and SQLite-backed persistent storage.

## Features

- **Two-tier caching**: Fast in-memory cache backed by persistent SQLite storage
- **LRU eviction**: Automatic eviction based on least recently used with alphabetical tie-breaking
- **TTL support**: Separate time-to-live for memory and disk tiers
- **Size and count limits**: Control cache size by both item count and total bytes
- **Schema versioning**: Automatic validation and cleanup of outdated cached objects
- **Batch operations**: Atomic multi-item put/get/delete operations
- **Thread-safe**: Basic concurrency support with locking infrastructure
- **Disk-only storage**: Configurable size threshold for large items

## Installation

```bash
pip install -e .
```

## Quick Start

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
    db_path="cache.db",
    model=User,
    max_memory_items=100,
    max_memory_size_bytes=1024 * 1024,  # 1 MB
    max_disk_items=1000,
    max_disk_size_bytes=10 * 1024 * 1024,  # 10 MB
    memory_ttl_seconds=60.0,  # 1 minute
    disk_ttl_seconds=3600.0,  # 1 hour
    max_item_size_bytes=10 * 1024,  # 10 KB
)

# Store an item
user = User(name="Alice", email="alice@example.com", age=30)
cache.put("user:1", user)

# Retrieve an item
cached_user = cache.get("user:1")
if cached_user:
    print(f"Found: {cached_user.name}")

# Clean up
cache.close()
```

## API Reference

### CacheableModel

Base class for all cached models.

```python
from pydantic import BaseModel

class CacheableModel(BaseModel):
    schema_version: str  # Required for all models
```

**Requirements:**
- All cached objects must inherit from `CacheableModel`
- Must define a `schema_version` field (semantic version string)
- Objects are immutable (frozen=True in model config)

### DiskBackedCache

Main cache class providing two-tier storage.

#### Constructor

```python
DiskBackedCache(
    db_path: str,  # Path to SQLite database file or ":memory:"
    model: Type[CacheableModel],  # Pydantic model class
    max_memory_items: int,  # Maximum items in memory
    max_memory_size_bytes: int,  # Maximum memory size in bytes
    max_disk_items: int,  # Maximum items on disk
    max_disk_size_bytes: int,  # Maximum disk size in bytes
    memory_ttl_seconds: float,  # TTL for memory tier
    disk_ttl_seconds: float,  # TTL for disk tier
    max_item_size_bytes: int,  # Items larger than this are disk-only
)
```

#### Methods

**get(key, timestamp=None) -> Optional[CacheableModel]**

Retrieve an item from cache.
- Checks memory first, then disk
- Updates LRU timestamp on access
- Returns None if not found or expired

**put(key, value, timestamp=None) -> None**

Store an item in cache.
- Stores to both memory and disk
- Large items (> max_item_size_bytes) stored on disk only
- Triggers eviction if limits exceeded

**delete(key) -> None**

Remove an item from cache (both memory and disk).

**get_many(keys, timestamp=None) -> dict[str, CacheableModel]**

Retrieve multiple items.
- Returns dict of found items only
- Does not update timestamps (read-only batch operation)

**put_many(items, timestamp=None) -> None**

Store multiple items atomically.
- All items stored in single transaction
- All succeed or all fail

**delete_many(keys) -> None**

Delete multiple items atomically.

**exists(key, timestamp=None) -> bool**

Check if key exists in cache.

**get_count() -> int**

Get total number of items in cache.

**get_total_size() -> int**

Get total size of cached items in bytes.

**clear() -> None**

Remove all items from cache.

**close() -> None**

Close the database connection.

**get_stats() -> dict[str, int]**

Get cache statistics.

Returns:
```python
{
    "memory_hits": int,  # Successful gets from memory
    "disk_hits": int,  # Successful gets from disk
    "misses": int,  # Gets that returned None
    "memory_evictions": int,  # Items evicted from memory
    "disk_evictions": int,  # Items evicted from disk
    "total_puts": int,  # Total put operations
    "total_gets": int,  # Total get operations
    "total_deletes": int,  # Total delete operations
    "current_memory_items": int,  # Items currently in memory
    "current_disk_items": int,  # Items currently on disk
}
```

## How It Works

### Two-Tier Architecture

The cache maintains two storage tiers:

1. **Memory Tier**: Fast access using Python dicts, stores object references
2. **Disk Tier**: Persistent SQLite storage, stores serialized JSON

Items are stored in both tiers (unless too large), with automatic promotion and eviction.

### LRU Eviction

Items are evicted based on least recent access:
- Each access updates the item's timestamp
- When limits are exceeded, least recently used items are evicted
- Ties broken alphabetically by key
- Disk eviction cascades to memory

### TTL Expiration

Separate TTLs for memory and disk:
- Memory TTL should be shorter (e.g., 1 minute)
- Disk TTL should be longer (e.g., 1 hour)
- Items expire based on time since last access (sliding window)
- Expired items automatically removed on access

### Large Item Handling

Items exceeding `max_item_size_bytes`:
- Stored on disk only (not in memory)
- Not promoted to memory on retrieval
- Don't count toward memory limits

### Schema Versioning

Each cached object includes its schema version:
- On retrieval, version checked against current model
- Mismatched versions automatically discarded
- Enables safe data model evolution

## Examples

See [example.py](example.py) for comprehensive usage examples.

### Basic Operations

```python
# Put
cache.put("key1", MyModel(value=1))

# Get
item = cache.get("key1")

# Delete
cache.delete("key1")

# Check existence
if cache.exists("key1"):
    print("Item exists")
```

### Batch Operations

```python
# Batch put
items = {
    "key1": MyModel(value=1),
    "key2": MyModel(value=2),
    "key3": MyModel(value=3),
}
cache.put_many(items)

# Batch get
result = cache.get_many(["key1", "key2", "key3"])

# Batch delete
cache.delete_many(["key1", "key2"])
```

### Statistics

```python
stats = cache.get_stats()
print(f"Hit rate: {stats['memory_hits'] + stats['disk_hits']} / {stats['total_gets']}")
print(f"Current items: {stats['current_memory_items']} in memory, {stats['current_disk_items']} on disk")
```

## Testing

Run the test suite:

```bash
# All tests
pytest

# Specific test file
pytest tests/test_basic_put_and_get.py

# With coverage
pytest --cov=disk_backed_cache_example

# In-memory mode (faster)
pytest --db-mode=memory
```

## Performance Considerations

- **Memory access**: O(1) for hits, very fast
- **Disk access**: Slower but persistent, benefits from SQLite WAL mode
- **Eviction**: O(n) where n = number of items (finds LRU item)
- **Batch operations**: More efficient than individual operations

## Limitations

- **Thread safety**: Basic infrastructure in place, full concurrent read/write locking future work
- **Eviction overhead**: Finding LRU item requires scanning all timestamps
- **Large values**: Very large items may impact performance

## Development

```bash
# Setup
uv sync

# Run tests
uv run pytest

# Run validations
./devtools/run_all_agent_validations.sh

# Format code
uv run ruff format .

# Lint
uv run ruff check .

# Type check
uv run ty check disk_backed_cache_example
```

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions welcome! Please ensure all tests pass and follow the existing code style.
