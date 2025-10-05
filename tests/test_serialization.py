from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str
    value: int


class NestedModel(CacheableModel):
    schema_version: str = "1.0.0"
    title: str
    data: dict[str, int]
    items: list[str]


def test_serialized_object_can_be_deserialized(db_path: str) -> None:
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

    obj = SampleModel(name="test", value=42)
    cache.put("key1", obj)
    result = cache.get("key1")

    assert result is not None
    assert isinstance(result, SampleModel)
    assert result.name == "test"
    assert result.value == 42


def test_size_calculation_from_serialized_json(db_path: str) -> None:
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

    obj = SampleModel(name="test", value=42)
    serialized = obj.model_dump_json()
    expected_size = len(serialized)

    cache.put("key1", obj)

    # Access internal state to verify size is stored correctly
    assert "key1" in cache._memory_cache
    stored_obj, stored_size, _, _ = cache._memory_cache["key1"]
    assert stored_size == expected_size
    assert isinstance(stored_obj, SampleModel)
    assert stored_obj.name == "test"


def test_complex_nested_object_serialization(db_path: str) -> None:
    cache = DiskBackedCache(
        db_path=db_path,
        model=NestedModel,
        max_memory_items=10,
        max_memory_size_bytes=1024 * 1024,
        max_disk_items=100,
        max_disk_size_bytes=10 * 1024 * 1024,
        memory_ttl_seconds=60.0,
        disk_ttl_seconds=3600.0,
        max_item_size_bytes=10 * 1024,
    )

    obj = NestedModel(
        title="complex",
        data={"a": 1, "b": 2, "c": 3},
        items=["item1", "item2", "item3"],
    )

    cache.put("key1", obj)
    result = cache.get("key1")

    assert result is not None
    assert isinstance(result, NestedModel)
    assert result.title == "complex"
    assert result.data == {"a": 1, "b": 2, "c": 3}
    assert result.items == ["item1", "item2", "item3"]
