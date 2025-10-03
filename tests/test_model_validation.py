import pytest

from disk_backed_cache_example.disk_backed_cache import CacheableModel, DiskBackedCache


class SampleModel(CacheableModel):
    schema_version: str = "1.0.0"
    name: str


class AnotherModel(CacheableModel):
    schema_version: str = "1.0.0"
    title: str


class SubclassModel(SampleModel):
    extra_field: str = "default"


def test_put_with_wrong_model_type_raises_typeerror(db_path: str) -> None:
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

    wrong_obj = AnotherModel(title="test")
    with pytest.raises(TypeError, match="Value must be an instance of SampleModel"):
        cache.put("key1", wrong_obj)  # type: ignore


def test_put_with_correct_model_type_succeeds(db_path: str) -> None:
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

    obj = SampleModel(name="test")
    cache.put("key1", obj)
    result = cache.get("key1")
    assert result is not None


def test_subclass_of_model_accepted(db_path: str) -> None:
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

    subclass_obj = SubclassModel(name="test", extra_field="extra")
    cache.put("key1", subclass_obj)
    result = cache.get("key1")
    assert result is not None
    assert isinstance(result, SubclassModel)
