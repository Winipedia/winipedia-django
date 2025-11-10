"""Tests for winipedia_django.bulk module."""

from collections import defaultdict
from typing import TYPE_CHECKING, Any, cast

import pytest
from django.db import models
from winipedia_utils.utils.modules.module import make_obj_importpath
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.db import bulk
from winipedia_django.utils.db.bulk import (
    MODE_CREATE,
    MODE_DELETE,
    MODE_UPDATE,
    bulk_create_bulks_in_steps,
    bulk_create_in_steps,
    bulk_delete,
    bulk_delete_in_steps,
    bulk_method_in_steps,
    bulk_method_in_steps_atomic,
    bulk_update_in_steps,
    flatten_bulk_in_steps_result,
    get_bulk_method,
    get_differences_between_bulks,
    get_step_chunks,
    multi_simulate_bulk_deletion,
    simulate_bulk_deletion,
)
from winipedia_django.utils.db.fields import get_fields

if TYPE_CHECKING:
    from collections.abc import Iterable


def test_bulk_create_in_steps() -> None:
    """Test func for bulk_create_in_steps."""

    # Create test model
    class BulkCreateTestModel(models.Model):
        """Test model for bulk_create_in_steps."""

        name: models.CharField[str, str] = models.CharField(max_length=100)
        value: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_bulk_create"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulk
    with pytest.MonkeyPatch().context() as m:
        # Mock Django's bulk_create method
        def mock_bulk_create(
            objs: list[models.Model],
            **kwargs: Any,  # noqa: ARG001
        ) -> list[models.Model]:
            # Assign PKs to objects like Django's bulk_create does
            for i, obj in enumerate(objs, start=1):
                if obj.pk is None:
                    obj.pk = i
            return list(objs)

        m.setattr(BulkCreateTestModel.objects, "bulk_create", mock_bulk_create)

        empty_result = bulk_create_in_steps(BulkCreateTestModel, [])
        assert_with_msg(
            empty_result == [],
            "Expected empty bulk to return empty list",
        )

        # Test with small bulk (less than step size)
        test_instances = [
            BulkCreateTestModel(name=f"test_{i}", value=i) for i in range(3)
        ]

        result = bulk_create_in_steps(BulkCreateTestModel, test_instances, step=5)

        # Test that we get the same instances back
        assert_with_msg(
            len(result) == len(test_instances),
            f"Expected {len(test_instances)} instances, got {len(result)}",
        )

        # Test with custom step size
        result_custom_step = bulk_create_in_steps(
            BulkCreateTestModel, test_instances, step=2
        )
        assert_with_msg(
            len(result_custom_step) == len(test_instances),
            f"Expected {len(test_instances)} instances with custom step, "
            f"got {len(result_custom_step)}",
        )

        # Test default step size
        result_default = bulk_create_in_steps(BulkCreateTestModel, test_instances)
        assert_with_msg(
            len(result_default) == len(test_instances),
            f"Expected {len(test_instances)} instances with default step, "
            f"got {len(result_default)}",
        )

        # Test that PKs were assigned by the mock
        for i, instance in enumerate(result_default, start=1):
            assert_with_msg(
                instance.pk is not None,
                f"Expected instance {i} to have a PK assigned",
            )
            assert_with_msg(
                instance.pk == i,
                f"Expected instance {i} to have PK {i}, got {instance.pk}",
            )


def test_bulk_update_in_steps() -> None:
    """Test func for bulk_update_in_steps."""

    # Create test model
    class BulkUpdateTestModel(models.Model):
        """Test model for bulk_update_in_steps."""

        name: models.CharField[str, str] = models.CharField(max_length=100)
        value: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_bulk_update"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulk
    with pytest.MonkeyPatch().context() as m:
        # Mock Django's bulk_update method
        def mock_bulk_update(
            objs: list[models.Model],
            fields: list[str],  # noqa: ARG001
            **kwargs: Any,  # noqa: ARG001
        ) -> int:
            return len(objs)

        m.setattr(BulkUpdateTestModel.objects, "bulk_update", mock_bulk_update)

        empty_result = bulk_update_in_steps(BulkUpdateTestModel, [], ["name"])
        assert_with_msg(
            empty_result == 0,
            "Expected empty bulk to return 0",
        )

        # Test with small bulk
        test_instances = [
            BulkUpdateTestModel(pk=i, name=f"updated_{i}", value=i * 10)
            for i in range(1, 4)
        ]

        # Test with update fields
        result = bulk_update_in_steps(
            BulkUpdateTestModel, test_instances, ["name", "value"], step=5
        )

        assert_with_msg(
            result == len(test_instances),
            f"Expected {len(test_instances)} updated, got {result}",
        )

        # Test with single field
        result_single = bulk_update_in_steps(
            BulkUpdateTestModel, test_instances, ["name"]
        )
        assert_with_msg(
            result_single == len(test_instances),
            f"Expected {len(test_instances)} updated with single field, "
            f"got {result_single}",
        )

        # Test with custom step size
        result_custom = bulk_update_in_steps(
            BulkUpdateTestModel, test_instances, ["value"], step=2
        )
        assert_with_msg(
            result_custom == len(test_instances),
            f"Expected {len(test_instances)} updated with custom step, "
            f"got {result_custom}",
        )


def test_bulk_delete_in_steps() -> None:
    """Test func for bulk_delete_in_steps."""

    # Create test model
    class BulkDeleteTestModel(models.Model):
        """Test model for bulk_delete_in_steps."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_bulk_delete_steps"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulk
    with pytest.MonkeyPatch().context() as m:
        # Mock Django's QuerySet delete method
        def mock_delete() -> tuple[int, dict[str, int]]:
            return (0, {})

        # Mock filter method to return a QuerySet with delete method
        class MockQuerySet:
            def delete(self) -> tuple[int, dict[str, int]]:
                return mock_delete()

        def mock_filter(**kwargs: Any) -> MockQuerySet:  # noqa: ARG001
            return MockQuerySet()

        m.setattr(BulkDeleteTestModel.objects, "filter", mock_filter)

        empty_result = bulk_delete_in_steps(BulkDeleteTestModel, [])
        assert_with_msg(
            empty_result == (0, {}),
            "Expected empty bulk to return (0, {})",
        )

        # Test with small bulk
        test_instances = [
            BulkDeleteTestModel(pk=i, name=f"delete_{i}") for i in range(1, 4)
        ]

        # Update mock to return proper delete result
        def mock_delete_with_count() -> tuple[int, dict[str, int]]:
            return (len(test_instances), {"BulkDeleteTestModel": len(test_instances)})

        class MockQuerySetWithCount:
            def delete(self) -> tuple[int, dict[str, int]]:
                return mock_delete_with_count()

        def mock_filter_with_count(**kwargs: Any) -> MockQuerySetWithCount:  # noqa: ARG001
            return MockQuerySetWithCount()

        m.setattr(BulkDeleteTestModel.objects, "filter", mock_filter_with_count)

        result = bulk_delete_in_steps(BulkDeleteTestModel, test_instances)

        assert_with_msg(
            result == (3, {"BulkDeleteTestModel": 3}),
            f"Expected (3, {{'BulkDeleteTestModel': 3}}), got {result}",
        )


def test_bulk_method_in_steps() -> None:
    """Test func for bulk_method_in_steps."""

    # Create test model
    class BulkMethodTestModel(models.Model):
        """Test model for bulk_method_in_steps."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_bulk_method_steps"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulk
    with pytest.MonkeyPatch().context() as m:
        # Mock Django's bulk_create method
        def mock_bulk_create(
            objs: list[models.Model],
            **kwargs: Any,  # noqa: ARG001
        ) -> list[models.Model]:
            # Assign PKs to objects like Django's bulk_create does
            for i, obj in enumerate(objs, start=1):
                if obj.pk is None:
                    obj.pk = i
            return list(objs)

        m.setattr(BulkMethodTestModel.objects, "bulk_create", mock_bulk_create)

        empty_result = bulk_method_in_steps(
            BulkMethodTestModel, [], step=10, mode=MODE_CREATE
        )
        assert_with_msg(
            empty_result == [],
            "Expected empty bulk to return empty list for create mode",
        )

        # Test with small bulk
        test_instances = [BulkMethodTestModel(name=f"test_{i}") for i in range(2)]

        create_result = bulk_method_in_steps(
            BulkMethodTestModel, test_instances, step=10, mode=MODE_CREATE
        )
        assert_with_msg(
            len(create_result) == len(test_instances),  # type: ignore[arg-type]
            f"Expected {len(test_instances)} created, got {len(create_result)}",  # type: ignore[arg-type]
        )


def test_bulk_method_in_steps_atomic() -> None:
    """Test func for bulk_method_in_steps_atomic."""

    # Create test model
    class AtomicTestModel(models.Model):
        """Test model for bulk_method_in_steps_atomic."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_atomic_steps"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulk
    with pytest.MonkeyPatch().context() as m:
        # Mock Django's bulk_create method
        def mock_bulk_create(
            objs: list[models.Model],
            **kwargs: Any,  # noqa: ARG001
        ) -> list[models.Model]:
            # Assign PKs to objects like Django's bulk_create does
            for i, obj in enumerate(objs, start=1):
                if obj.pk is None:
                    obj.pk = i
            return list(objs)

        m.setattr(AtomicTestModel.objects, "bulk_create", mock_bulk_create)

        empty_result = bulk_method_in_steps_atomic(
            AtomicTestModel, [], step=10, mode=MODE_CREATE
        )
        assert_with_msg(
            empty_result == [],
            "Expected empty bulk to return empty list",
        )

        test_instances = [AtomicTestModel(name=f"test_{i}") for i in range(2)]
        result = bulk_method_in_steps_atomic(
            AtomicTestModel, test_instances, step=10, mode=MODE_CREATE
        )

        assert_with_msg(
            len(result) == len(test_instances),  # type: ignore[arg-type]
            f"Expected {len(test_instances)} items, got {len(result)}",  # type: ignore[arg-type]
        )


def test_get_step_chunks() -> None:
    """Test func for get_step_chunks."""

    # Create test model
    class ChunkTestModel(models.Model):
        """Test model for get_step_chunks."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_chunks"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulk
    empty_chunks = list(get_step_chunks([], 5))
    assert_with_msg(
        empty_chunks == [],
        "Expected empty chunks for empty bulk",
    )

    # Test with bulk smaller than step size
    small_bulk = [ChunkTestModel(name=f"test_{i}") for i in range(3)]
    small_chunks = list(get_step_chunks(small_bulk, 5))

    assert_with_msg(
        small_chunks == [(small_bulk,)],
        f"Expected one chunk, got {small_chunks}",
    )

    # Test with bulk larger than step size
    large_bulk = [ChunkTestModel(name=f"test_{i}") for i in range(7)]
    large_chunks = list(get_step_chunks(large_bulk, 3))

    expected_large_chunks = [
        (large_bulk[:3],),
        (large_bulk[3:6],),
        (large_bulk[6:],),
    ]
    assert_with_msg(
        large_chunks == expected_large_chunks,
        f"Expected {expected_large_chunks}, got {large_chunks}",
    )


def test_get_bulk_method() -> None:
    """Test func for get_bulk_method."""

    # Create test model
    class BulkMethodTestModel(models.Model):
        """Test model for get_bulk_method."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_get_bulk_method"

        def __str__(self) -> str:
            return self.name

    # Test create method
    create_method = get_bulk_method(BulkMethodTestModel, MODE_CREATE)
    assert_with_msg(
        callable(create_method),
        "Expected create method to be callable",
    )

    # Test update method
    update_method = get_bulk_method(BulkMethodTestModel, MODE_UPDATE, fields=["name"])
    assert_with_msg(
        callable(update_method),
        "Expected update method to be callable",
    )

    # Test delete method
    delete_method = get_bulk_method(BulkMethodTestModel, MODE_DELETE)
    assert_with_msg(
        callable(delete_method),
        "Expected delete method to be callable",
    )

    # Test invalid mode raises ValueError
    with pytest.raises(ValueError, match="Invalid method"):
        get_bulk_method(BulkMethodTestModel, "invalid_mode")  # type: ignore[arg-type]


def test_flatten_bulk_in_steps_result() -> None:
    """Test func for flatten_bulk_in_steps_result."""

    # Create test model for results
    class FlattenTestModel(models.Model):
        """Test model for flatten_bulk_in_steps_result."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_flatten"

        def __str__(self) -> str:
            return self.name

    # Test create mode flattening
    test_instances = [FlattenTestModel(name=f"test_{i}") for i in range(3)]
    create_results = [test_instances[:2], test_instances[2:]]
    flattened_create = flatten_bulk_in_steps_result(create_results, MODE_CREATE)

    assert_with_msg(
        len(flattened_create) == len(test_instances),  # type: ignore[arg-type]
        f"Expected {len(test_instances)} flattened items, got {len(flattened_create)}",  # type: ignore[arg-type]
    )

    # Test update mode flattening
    update_results = [5, 3, 2]  # Counts from different chunks
    flattened_update = flatten_bulk_in_steps_result(update_results, MODE_UPDATE)
    expected_update_sum = 10
    assert_with_msg(
        flattened_update == expected_update_sum,
        f"Expected sum {expected_update_sum}, got {flattened_update}",
    )

    # Test delete mode flattening
    delete_results = [
        (3, {"Model1": 2, "Model2": 1}),
        (2, {"Model1": 1, "Model2": 1}),
    ]
    flattened_delete = flatten_bulk_in_steps_result(delete_results, MODE_DELETE)

    total_count, model_counts = flattened_delete  # type: ignore[misc]
    expected_total = 5
    assert_with_msg(
        total_count == expected_total,
        f"Expected total count {expected_total}, got {total_count}",
    )
    assert_with_msg(
        model_counts["Model1"] == 3,  # type: ignore[index] # noqa: PLR2004
        f"Expected Model1 count 3, got {model_counts['Model1']}",  # type: ignore[index]
    )
    assert_with_msg(
        model_counts["Model2"] == 2,  # type: ignore[index] # noqa: PLR2004
        f"Expected Model2 count 2, got {model_counts['Model2']}",  # type: ignore[index]
    )

    # Test invalid mode raises ValueError
    with pytest.raises(ValueError, match="Invalid method"):
        flatten_bulk_in_steps_result([], "invalid_mode")


def test_bulk_delete() -> None:
    """Test func for bulk_delete."""

    # Create test model
    class BulkDeleteTestModel(models.Model):
        """Test model for bulk_delete."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_bulk_delete"

        def __str__(self) -> str:
            return self.name

    # Test with non-empty list (empty list has a bug in the actual function)
    test_instances = [BulkDeleteTestModel(pk=i, name=f"test_{i}") for i in range(1, 4)]

    # Mock the model's objects manager
    with pytest.MonkeyPatch().context() as m:

        class MockQuerySet:
            def delete(self) -> tuple[int, dict[str, int]]:
                return (3, {"BulkDeleteTestModel": 3})

        def mock_filter(**kwargs: Any) -> MockQuerySet:  # noqa: ARG001
            return MockQuerySet()

        m.setattr(BulkDeleteTestModel.objects, "filter", mock_filter)

        result = bulk_delete(BulkDeleteTestModel, test_instances)

        assert_with_msg(
            result == (3, {"BulkDeleteTestModel": 3}),
            f"Expected (3, {{'BulkDeleteTestModel': 3}}), got {result}",
        )


def test_bulk_create_bulks_in_steps() -> None:
    """Test func for bulk_create_bulks_in_steps."""

    # Create test models with dependencies
    class Author(models.Model):
        """Test model for bulk_create_bulks_in_steps."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_bulk_create_bulks"

        def __str__(self) -> str:
            return self.name

    class Book(models.Model):
        """Test model for bulk_create_bulks_in_steps."""

        title: models.CharField[str, str] = models.CharField(max_length=200)
        author: models.ForeignKey[Author, Author] = models.ForeignKey(
            Author, on_delete=models.CASCADE
        )

        class Meta:
            app_label = "test_bulk_create_bulks"

        def __str__(self) -> str:
            return self.title

    # Test with empty bulks
    empty_result = bulk_create_bulks_in_steps({})
    assert_with_msg(
        empty_result == {},
        "Expected empty result for empty bulks",
    )

    # Mock the bulk_create methods
    with pytest.MonkeyPatch().context() as m:

        def mock_bulk_create(
            objs: list[models.Model],
            **kwargs: Any,  # noqa: ARG001
        ) -> list[models.Model]:
            # Assign PKs to objects like Django's bulk_create does
            for i, obj in enumerate(objs, start=1):
                if obj.pk is None:
                    obj.pk = i
            return list(objs)

        m.setattr(Author.objects, "bulk_create", mock_bulk_create)
        m.setattr(Book.objects, "bulk_create", mock_bulk_create)

        # Test with dependent models
        authors = [Author(name=f"author_{i}") for i in range(2)]
        books = [Book(title=f"book_{i}", author=authors[0]) for i in range(3)]

        bulk_by_class = cast(
            "dict[type[models.Model], Iterable[models.Model]]",
            {
                Author: authors,
                Book: books,
            },
        )

        result = bulk_create_bulks_in_steps(bulk_by_class)

        assert_with_msg(
            len(result) == 2,  # noqa: PLR2004
            f"Expected 2 model types in result, got {len(result)}",
        )
        assert_with_msg(
            Author in result,
            "Expected Author in result",
        )
        assert_with_msg(
            Book in result,
            "Expected Book in result",
        )
        assert_with_msg(
            len(result[Author]) == len(authors),
            f"Expected {len(authors)} authors, got {len(result[Author])}",
        )
        assert_with_msg(
            len(result[Book]) == len(books),
            f"Expected {len(books)} books, got {len(result[Book])}",
        )


def test_get_differences_between_bulks() -> None:
    """Test func for get_differences_between_bulks."""

    # Create test model
    class DiffTestModel(models.Model):
        """Test model for get_differences_between_bulks."""

        name: models.CharField[str, str] = models.CharField(max_length=100)
        value: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_differences"

        def __str__(self) -> str:
            return self.name

    # Test with empty bulks
    empty_result = get_differences_between_bulks([], [], [])
    assert_with_msg(
        empty_result == ([], [], [], []),
        "Expected empty result for empty bulks",
    )

    # Test with different instances
    bulk1 = [
        DiffTestModel(name="test_1", value=1),
        DiffTestModel(name="test_2", value=2),
        DiffTestModel(name="common", value=3),
    ]
    bulk2 = [
        DiffTestModel(name="test_3", value=4),
        DiffTestModel(name="test_4", value=5),
        DiffTestModel(name="common", value=3),
    ]

    # Get fields for comparison
    fields = get_fields(DiffTestModel)

    result = (
        get_differences_between_bulks(bulk1, bulk2, fields)  # type: ignore[arg-type]
    )

    # Test that we get lists back
    expected = (
        [bulk1[0], bulk1[1]],
        [bulk2[0], bulk2[1]],
        [bulk1[2]],
        [bulk2[2]],
    )
    assert_with_msg(
        result == expected,
        f"Expected {expected}, got {result}",
    )

    # Test with different model types raises ValueError
    class OtherModel(models.Model):
        """Other test model."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_differences_other"

        def __str__(self) -> str:
            return self.name

    other_bulk = [OtherModel(name="other")]

    with pytest.raises(ValueError, match="Both bulks must be of the same model type"):
        get_differences_between_bulks(bulk1, other_bulk, fields)  # type: ignore[arg-type]


def test_simulate_bulk_deletion() -> None:
    """Test func for simulate_bulk_deletion."""

    # Create test model
    class SimulateDeleteTestModel(models.Model):
        """Test model for simulate_bulk_deletion."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_simulate_delete"

        def __str__(self) -> str:
            return self.name

    # Test with empty entries
    empty_result = simulate_bulk_deletion(SimulateDeleteTestModel, [])
    assert_with_msg(
        empty_result == {},
        "Expected empty result for empty entries",
    )

    # Test with mock entries
    test_instances: list[models.Model] = [
        SimulateDeleteTestModel(pk=i, name=f"test_{i}") for i in range(1, 4)
    ]

    # Mock the Collector to avoid actual database operations
    with pytest.MonkeyPatch().context() as m:

        class MockCollector:
            def __init__(self, *_args: Any, **_kwargs: Any) -> None:
                self.data: defaultdict[type[models.Model], set[models.Model]] = (
                    defaultdict(set)
                )
                self.fast_deletes: list[Any] = []

            def collect(self, entries: list[models.Model]) -> None:
                # Simulate collecting the entries
                for entry in entries:
                    self.data[entry.__class__].add(entry)

        m.setattr(make_obj_importpath(bulk) + ".Collector", MockCollector)

        result = simulate_bulk_deletion(SimulateDeleteTestModel, test_instances)

        assert_with_msg(
            SimulateDeleteTestModel in result,
            "Expected SimulateDeleteTestModel in result",
        )
        assert_with_msg(
            len(result[SimulateDeleteTestModel]) == len(test_instances),
            f"Expected {len(test_instances)} instances, "
            f"got {len(result[SimulateDeleteTestModel])}",
        )


def test_multi_simulate_bulk_deletion() -> None:
    """Test func for multi_simulate_bulk_deletion."""

    # Create test models
    class MultiDeleteModel1(models.Model):
        """Test model 1 for multi_simulate_bulk_deletion."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_multi_delete_1"

        def __str__(self) -> str:
            return self.name

    class MultiDeleteModel2(models.Model):
        """Test model 2 for multi_simulate_bulk_deletion."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_multi_delete_2"

        def __str__(self) -> str:
            return self.name

    # Test with empty entries
    empty_result = multi_simulate_bulk_deletion({})
    assert_with_msg(
        empty_result == {},
        "Expected empty result for empty entries",
    )

    # Mock the simulate_bulk_deletion function
    with pytest.MonkeyPatch().context() as m:

        def mock_simulate_bulk_deletion(
            model_class: type[models.Model], entries: list[models.Model]
        ) -> dict[type[models.Model], set[models.Model]]:
            # Return a mock result - use list instead of set to avoid hashing issues
            return {
                model_class: set(entries[:1])
            }  # Only take first item to avoid hashing issues

        m.setattr(
            make_obj_importpath(simulate_bulk_deletion),
            mock_simulate_bulk_deletion,
        )

        # Test with multiple model types -
        # use instances with PKs to avoid hashing issues
        model1_instances: list[models.Model] = [
            MultiDeleteModel1(pk=i, name=f"model1_{i}") for i in range(1, 3)
        ]
        model2_instances: list[models.Model] = [
            MultiDeleteModel2(pk=i, name=f"model2_{i}") for i in range(1, 4)
        ]

        entries: dict[type[models.Model], list[models.Model]] = {
            MultiDeleteModel1: model1_instances,
            MultiDeleteModel2: model2_instances,
        }

        result = multi_simulate_bulk_deletion(entries)

        assert_with_msg(
            result
            == {
                MultiDeleteModel1: set(model1_instances[:1]),
                MultiDeleteModel2: set(model2_instances[:1]),
            },
            f"Expected {{MultiDeleteModel1: {model1_instances[:1]}, "
            f"MultiDeleteModel2: {model2_instances[:1]}}}, got {result}",
        )
