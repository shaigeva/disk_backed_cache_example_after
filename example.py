"""Example usage of DiskBackedCache."""

from typing import cast

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class User(CacheableModel):
    """Example user model."""

    schema_version: str = "1.0.0"
    name: str
    email: str
    age: int


def main() -> None:
    """Demonstrate cache usage."""
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

    print("=== DiskBackedCache Example ===\n")

    # Store users
    print("1. Storing users...")
    users = {
        "user:1": User(name="Alice", email="alice@example.com", age=30),
        "user:2": User(name="Bob", email="bob@example.com", age=25),
        "user:3": User(name="Charlie", email="charlie@example.com", age=35),
    }
    cache.put_many(users)
    print(f"   Stored {len(users)} users\n")

    # Retrieve users
    print("2. Retrieving users...")
    alice = cache.get("user:1")
    if alice:
        alice_typed = cast(User, alice)
        print(f"   Found: {alice_typed.name} ({alice_typed.email}), age {alice_typed.age}")
    bob = cache.get("user:2")
    if bob:
        bob_typed = cast(User, bob)
        print(f"   Found: {bob_typed.name} ({bob_typed.email}), age {bob_typed.age}\n")

    # Get multiple users
    print("3. Batch retrieval...")
    result = cache.get_many(["user:1", "user:2", "user:3"])
    print(f"   Retrieved {len(result)} users\n")

    # Update a user
    print("4. Updating user...")
    cache.put("user:1", User(name="Alice Smith", email="alice.smith@example.com", age=31))
    updated = cache.get("user:1")
    if updated:
        updated_typed = cast(User, updated)
        print(f"   Updated: {updated_typed.name} ({updated_typed.email}), age {updated_typed.age}\n")

    # Check existence
    print("5. Checking existence...")
    print(f"   user:1 exists: {cache.exists('user:1')}")
    print(f"   user:999 exists: {cache.exists('user:999')}\n")

    # Get statistics
    print("6. Cache statistics...")
    stats = cache.get_stats()
    print(f"   Memory hits: {stats['memory_hits']}")
    print(f"   Disk hits: {stats['disk_hits']}")
    print(f"   Misses: {stats['misses']}")
    print(f"   Total operations: {stats['total_puts']} puts, {stats['total_gets']} gets")
    print(f"   Current items: {stats['current_memory_items']} in memory, {stats['current_disk_items']} on disk\n")

    # Delete a user
    print("7. Deleting user...")
    cache.delete("user:2")
    print("   Deleted user:2")
    print(f"   user:2 exists: {cache.exists('user:2')}\n")

    # Get count and size
    print("8. Cache metrics...")
    print(f"   Total items: {cache.get_count()}")
    print(f"   Total size: {cache.get_total_size()} bytes\n")

    # Clear cache
    print("9. Clearing cache...")
    cache.clear()
    print("   Cache cleared")
    print(f"   Total items: {cache.get_count()}\n")

    # Close cache
    print("10. Closing cache...")
    cache.close()
    print("    Cache closed\n")

    print("=== Example complete ===")


if __name__ == "__main__":
    main()
