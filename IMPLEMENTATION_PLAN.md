# Implementation Plan - DiskBackedCache

## Overview
Implementing a two-tier LRU cache for Pydantic objects with in-memory and SQLite storage.
Each step is a complete, testable capability with its own tests.

## Progress Tracking
- [x] Step 1: In-Memory Cache - Basic Put/Get
- [x] Step 2: Key Validation
- [x] Step 3: Model Type Validation
- [x] Step 4: Serialization/Deserialization
- [x] Step 5: SQLite Connection Setup
- [x] Step 6: SQLite Put/Get Operations
- [x] Step 7: Delete Operation - Memory
- [x] Step 8: Delete Operation - SQLite
- [x] Step 9: Contains Check (not exists)
- [x] Step 10: Memory Count Tracking
- [x] Step 11: SQLite Count Tracking
- [x] Step 12: Memory Size Tracking
- [x] Step 13: SQLite Size Tracking
- [x] Step 14: Timestamp Storage
- [x] Step 15: Schema Version Storage
- [x] Step 16: Schema Version Validation
- [x] Step 17: Memory LRU - Count-Based Eviction
- [x] Step 18: Memory LRU - Size-Based Eviction
- [x] Step 19: SQLite LRU - Count-Based Eviction
- [x] Step 20: SQLite LRU - Size-Based Eviction
- [x] Step 21: Two-Tier Coordination - Put
- [x] Step 22: Two-Tier Coordination - Get with Promotion
- [x] Step 23: Max Item Size for Disk-Only Storage
- [x] Step 24: Cascading Eviction (Disk→Memory)
- [x] Step 25: Memory TTL Check
- [x] Step 26: Disk TTL Check
- [x] Step 27: Custom Timestamp Parameter
- [x] Step 28: Clear Operation
- [x] Step 29: Close Operation
- [x] Step 30: Basic Thread Safety (Read-Write Locks)
- [x] Step 31: LRU Tie-Breaking (Alphabetical)
- [x] Step 32: Logging at TRACE Level
- [x] Step 33: Edge Cases & Error Handling

## Detailed Steps

### Step 1: In-Memory Cache - Basic Put/Get
**Implementation:**
- Add simple dict `self._memory_cache` to store objects
- Implement `put()` to store in memory dict
- Implement `get()` to retrieve from memory dict
- Ignore all other parameters for now

**Tests:**
- `test_after_put_get_returns_the_object` - Put an object, get it back
- `test_get_nonexistent_key_returns_none` - Getting missing key returns None

### Step 2: Key Validation
**Implementation:**
- Validate key is string type
- Validate key length <= 256 characters
- Raise ValueError for invalid keys

**Tests:**
- `test_put_with_invalid_key_type_raises_valueerror` - Non-string key raises error
- `test_put_with_too_long_key_raises_valueerror` - Key > 256 chars raises error
- `test_valid_key_accepted` - Valid keys work

### Step 3: Model Type Validation
**Implementation:**
- Store expected model type from constructor
- Validate put() value is instance of expected model
- Raise TypeError for wrong model type

**Tests:**
- `test_put_with_wrong_model_type_raises_typeerror` - Wrong type raises error
- `test_put_with_correct_model_type_succeeds` - Correct type works
- `test_subclass_of_model_accepted` - Subclasses are accepted

### Step 4: Serialization/Deserialization
**Implementation:**
- Add JSON serialization using pydantic's `model_dump_json()`
- Store serialized JSON string (even in memory for now)
- Deserialize using `model_validate_json()`
- Calculate size from JSON string length

**Tests:**
- `test_serialized_object_can_be_deserialized` - Round-trip works
- `test_size_calculation_from_serialized_json` - Size matches JSON length
- `test_complex_nested_object_serialization` - Nested objects work

### Step 5: SQLite Connection Setup
**Implementation:**
- Create SQLite connection in constructor
- Enable WAL mode: `PRAGMA journal_mode=WAL`
- Create cache table with schema (key, value, timestamp, schema_version, size)
- Handle both file path and `:memory:` for testing

**Tests:**
- `test_sqlite_connection_created_successfully` - Connection established
- `test_cache_table_exists_after_init` - Table created
- `test_wal_mode_enabled` - WAL pragma set correctly
- `test_memory_database_works` - `:memory:` database works

### Step 6: SQLite Put/Get Operations
**Implementation:**
- Implement `put()` for SQLite (store serialized JSON)
- Implement `get()` for SQLite (retrieve and deserialize)
- Use parameterized queries
- Each operation in its own transaction

**Tests:**
- `test_sqlite_put_and_get_returns_object` - Basic storage works
- `test_sqlite_get_nonexistent_returns_none` - Missing key returns None
- `test_sqlite_transaction_isolation` - Each operation is isolated

### Step 7: Delete Operation - Memory
**Implementation:**
- Implement `delete()` to remove from memory cache
- Handle non-existent keys gracefully (no-op)

**Tests:**
- `test_delete_removes_from_memory` - Delete removes item
- `test_delete_nonexistent_key_is_noop` - No error on missing key

### Step 8: Delete Operation - SQLite
**Implementation:**
- Implement `delete()` for SQLite
- Use DELETE SQL statement with parameterized query

**Tests:**
- `test_delete_removes_from_sqlite` - Delete removes from DB
- `test_delete_from_both_memory_and_disk` - Removes from both

### Step 9: Contains Check
**Implementation:**
- Implement `contains()` method (not exists per spec)
- Check memory first, then disk
- Don't update timestamps (just checking existence)

**Tests:**
- `test_contains_returns_true_for_memory_item` - Found in memory
- `test_contains_returns_true_for_disk_item` - Found on disk
- `test_contains_returns_false_for_nonexistent` - Not found

### Step 10: Memory Count Tracking
**Implementation:**
- Track count of items in memory
- Update on put/delete

**Tests:**
- `test_memory_count_increases_on_put` - Count goes up
- `test_memory_count_decreases_on_delete` - Count goes down
- `test_memory_count_accurate` - Count matches actual items

### Step 11: SQLite Count Tracking
**Implementation:**
- Implement `get_count()` using COUNT(*) query
- Consider both memory and disk items

**Tests:**
- `test_sqlite_count_query_returns_correct_value` - COUNT works
- `test_total_count_includes_both_tiers` - Combined count

### Step 12: Memory Size Tracking
**Implementation:**
- Track total size of items in memory
- Update on put/delete
- Store size with each item

**Tests:**
- `test_memory_size_tracking_on_put` - Size increases
- `test_memory_size_tracking_on_delete` - Size decreases
- `test_memory_total_size_accurate` - Total is correct

### Step 13: SQLite Size Tracking
**Implementation:**
- Store size column in SQLite
- Implement `get_total_size()` using SUM(size) query

**Tests:**
- `test_sqlite_stores_size_metadata` - Size stored in DB
- `test_sqlite_total_size_query` - SUM query works
- `test_combined_size_both_tiers` - Total across tiers

### Step 14: Timestamp Storage
**Implementation:**
- Add timestamp tracking using `time.time()`
- Store timestamp on put()
- Update timestamp on get() (for LRU)
- Support custom timestamp parameter

**Tests:**
- `test_timestamp_stored_on_put` - Initial timestamp set
- `test_timestamp_updated_on_get` - Get updates timestamp
- `test_custom_timestamp_parameter` - Can override timestamp

### Step 15: Schema Version Storage
**Implementation:**
- Extract schema_version from model class
- Store in SQLite schema_version column
- Store with memory items too

**Tests:**
- `test_schema_version_extracted_from_model` - Extraction works
- `test_schema_version_stored_in_cache` - Stored correctly
- `test_model_without_schema_version_fails` - Error if missing

### Step 16: Schema Version Validation
**Implementation:**
- On get(), check schema version matches
- Delete item if version mismatch
- Log at TRACE level (will add logging later)

**Tests:**
- `test_get_with_wrong_schema_returns_none` - Mismatch returns None
- `test_wrong_schema_item_deleted_from_cache` - Item removed
- `test_correct_schema_version_returns_object` - Match works

### Step 17: Memory LRU - Count-Based Eviction
**Implementation:**
- When putting would exceed max_memory_items
- Find oldest item by timestamp
- Remove oldest item
- Repeat until under limit

**Tests:**
- `test_memory_evicts_oldest_on_count_limit` - Oldest removed
- `test_memory_lru_order_maintained` - Order correct
- `test_memory_eviction_makes_room_for_new` - New item fits

### Step 18: Memory LRU - Size-Based Eviction
**Implementation:**
- When putting would exceed max_memory_size_bytes
- Evict oldest items until enough space

**Tests:**
- `test_memory_evicts_on_size_limit` - Size limit enforced
- `test_memory_evicts_multiple_for_large_item` - Multiple evicted if needed

### Step 19: SQLite LRU - Count-Based Eviction
**Implementation:**
- When putting would exceed max_disk_items
- DELETE oldest items using ORDER BY timestamp LIMIT

**Tests:**
- `test_disk_evicts_oldest_on_count_limit` - SQL eviction works
- `test_disk_eviction_count_accurate` - Count maintained

### Step 20: SQLite LRU - Size-Based Eviction
**Implementation:**
- When putting would exceed max_disk_size_bytes
- Query oldest items, evict until enough space

**Tests:**
- `test_disk_evicts_on_size_limit` - Size limit enforced
- `test_disk_size_tracking_after_eviction` - Size accurate

### Step 21: Two-Tier Coordination - Put
**Implementation:**
- put() stores to disk first
- Then stores to memory (if fits)
- Both tiers updated together

**Tests:**
- `test_put_stores_in_both_tiers` - Both updated
- `test_put_disk_succeeds_memory_fails` - Handle partial success

### Step 22: Two-Tier Coordination - Get with Promotion
**Implementation:**
- get() checks memory first
- If not in memory, check disk
- If found on disk, promote to memory (if fits)

**Tests:**
- `test_get_checks_memory_first` - Memory preferred
- `test_get_promotes_from_disk_to_memory` - Promotion works
- `test_get_promotion_respects_memory_limits` - Won't exceed limits

### Step 23: Max Item Size for Disk-Only Storage
**Implementation:**
- Items > max_item_size_bytes go to disk only
- Never stored in memory, even on get()

**Tests:**
- `test_large_item_stored_disk_only` - Large items disk-only
- `test_small_item_stored_both_tiers` - Small items in both
- `test_large_item_not_promoted_to_memory` - No promotion

### Step 24: Cascading Eviction (Disk→Memory)
**Implementation:**
- When item evicted from disk, also remove from memory
- Maintains consistency between tiers

**Tests:**
- `test_disk_eviction_removes_from_memory` - Cascading works
- `test_cascading_maintains_consistency` - Tiers stay in sync

### Step 25: Memory TTL Check
**Implementation:**
- Check memory_ttl_seconds on get()
- Remove expired items
- Return None for expired

**Tests:**
- `test_expired_memory_item_returns_none` - Expired returns None
- `test_valid_memory_item_returns_object` - Non-expired works
- `test_expired_item_removed_from_memory` - Cleanup happens

### Step 26: Disk TTL Check
**Implementation:**
- Check disk_ttl_seconds on get()
- Remove expired items from SQLite

**Tests:**
- `test_expired_disk_item_returns_none` - Expired returns None
- `test_expired_item_removed_from_disk` - DB cleanup

### Step 27: Custom Timestamp Parameter
**Implementation:**
- Support timestamp parameter in put() and get()
- Use for TTL calculations

**Tests:**
- `test_custom_timestamp_on_put` - Put with custom time
- `test_custom_timestamp_on_get_for_ttl` - Get uses custom time
- `test_timestamp_none_uses_current_time` - Default behavior

### Step 28: Clear Operation
**Implementation:**
- Implement clear() to remove all items
- Clear both memory and disk
- Use DELETE FROM cache for SQLite

**Tests:**
- `test_clear_removes_all_items` - Everything removed
- `test_clear_resets_counts_and_sizes` - Stats reset
- `test_clear_on_empty_cache` - Works when empty

### Step 29: Close Operation
**Implementation:**
- Implement close() for cleanup
- Close SQLite connection
- Clear memory cache

**Tests:**
- `test_close_releases_resources` - Connection closed
- `test_operations_after_close_fail` - Can't use after close

### Step 30: Basic Thread Safety (Read-Write Locks)
**Implementation:**
- Add threading.RLock for thread safety
- Protect all public methods
- Allow concurrent reads where possible

**Tests:**
- `test_concurrent_reads_allowed` - Multiple readers OK
- `test_writes_are_serialized` - Writers exclusive
- `test_no_race_conditions` - Thread-safe operations

### Step 31: LRU Tie-Breaking (Alphabetical)
**Implementation:**
- When timestamps equal, evict alphabetically smallest key
- Modify eviction queries to ORDER BY timestamp, key

**Tests:**
- `test_lru_tiebreak_uses_alphabetical_order` - Ties broken correctly
- `test_tiebreak_in_memory_eviction` - Memory tiebreak works
- `test_tiebreak_in_disk_eviction` - Disk tiebreak works

### Step 32: Logging at TRACE Level
**Implementation:**
- Add logging for all operations
- Use TRACE level (or DEBUG as Python doesn't have TRACE)
- Log errors, schema mismatches, evictions

**Tests:**
- `test_operations_are_logged` - Logging happens
- `test_errors_logged_at_appropriate_level` - Error logging

### Step 33: Edge Cases & Error Handling
**Implementation:**
- Handle database corruption
- Handle file permissions
- Handle oversized items (> max_disk_size)
- Validate constructor parameters

**Tests:**
- `test_empty_cache_operations` - Edge: empty cache
- `test_exactly_at_limit` - Edge: at limits
- `test_oversized_item_rejected` - Edge: too large
- `test_invalid_db_path_handled` - Error handling

## Notes
- Each step builds on previous steps
- Run `./devtools/run_all_agent_validations.sh` after each step
- Update this file's checkboxes as steps are completed
- If a step fails validation, fix before proceeding