"""Tests for winipedia_django.database module."""

from django.db import models
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.db.fields import get_fields
from winipedia_django.utils.db.models import (
    BaseModel,
    hash_model_instance,
    topological_sort_models,
)


def test_topological_sort_models() -> None:
    """Test func for topological_sort_models."""

    # Create test models with dependencies for testing
    class Author(models.Model):
        """Test model for topological sorting."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.name

    class Publisher(models.Model):
        """Test model for topological sorting."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.name

    class Book(models.Model):
        """Test model for topological sorting."""

        title: models.CharField[str, str] = models.CharField(max_length=200)
        author: models.ForeignKey[Author, Author] = models.ForeignKey(
            Author, on_delete=models.CASCADE
        )
        publisher: models.ForeignKey[Publisher, Publisher] = models.ForeignKey(
            Publisher, on_delete=models.CASCADE
        )

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.title

    class Review(models.Model):
        """Test model for topological sorting."""

        book: models.ForeignKey[Book, Book] = models.ForeignKey(
            Book, on_delete=models.CASCADE
        )
        rating: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return f"Review of {self.book}"

    # Test basic topological sorting
    models_to_sort = [Review, Book, Author, Publisher]
    sorted_models = topological_sort_models(models_to_sort)

    # Test that we get all models back
    assert_with_msg(
        len(sorted_models) == len(models_to_sort),
        f"Expected {len(models_to_sort)} models, got {len(sorted_models)}",
    )
    assert_with_msg(
        set(sorted_models) == set(models_to_sort),
        "Expected all input models to be in output",
    )

    # Test dependency order: Author and Publisher should come before Book
    author_index = sorted_models.index(Author)
    publisher_index = sorted_models.index(Publisher)
    book_index = sorted_models.index(Book)
    review_index = sorted_models.index(Review)

    assert_with_msg(
        author_index < book_index,
        f"Expected Author (index {author_index}) to come before Book "
        f"(index {book_index})",
    )
    assert_with_msg(
        publisher_index < book_index,
        f"Expected Publisher (index {publisher_index}) to come before Book "
        f"(index {book_index})",
    )
    assert_with_msg(
        book_index < review_index,
        f"Expected Book (index {book_index}) to come before Review "
        f"(index {review_index})",
    )

    # Test with models that have no dependencies
    independent_models = [Author, Publisher]
    sorted_independent = topological_sort_models(independent_models)
    expected_independent_count = len(independent_models)
    assert_with_msg(
        len(sorted_independent) == expected_independent_count,
        f"Expected {expected_independent_count} independent models",
    )
    assert_with_msg(
        set(sorted_independent) == set(independent_models),
        "Expected all independent models to be returned",
    )

    # Test with single model
    single_model = topological_sort_models([Author])
    assert_with_msg(
        single_model == [Author],
        "Expected single model to be returned as-is",
    )

    # Test with empty list
    empty_result: list[type[models.Model]] = topological_sort_models([])
    assert_with_msg(
        empty_result == [],
        "Expected empty list to return empty list",
    )


def test_hash_model_instance() -> None:
    """Test func for hash_model_instance."""

    # Create a test model for hashing
    class HashTestModel(models.Model):
        """Test model for hash_model_instance."""

        name: models.CharField[str, str] = models.CharField(max_length=100)
        value: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.name

    # Test hashing with saved instance (has pk)
    saved_instance = HashTestModel(pk=1, name="test", value=42)
    saved_fields = get_fields(HashTestModel)
    saved_hash = hash_model_instance(saved_instance, saved_fields)

    # Test that hash is based on pk for saved instances
    assert_with_msg(
        type(saved_hash) is int,
        f"Expected hash to be int, got {type(saved_hash)}",
    )

    # Test that same pk produces same hash
    another_saved_instance = HashTestModel(pk=1, name="different", value=99)
    another_saved_hash = hash_model_instance(another_saved_instance, saved_fields)
    assert_with_msg(
        saved_hash == another_saved_hash,
        "Expected instances with same pk to have same hash",
    )

    # Test hashing with unsaved instance (no pk)
    unsaved_instance = HashTestModel(name="test", value=42)
    unsaved_hash = hash_model_instance(unsaved_instance, saved_fields)

    assert_with_msg(
        type(unsaved_hash) is int,
        f"Expected hash to be int, got {type(unsaved_hash)}",
    )

    # Test that same field values produce same hash for unsaved instances
    same_unsaved_instance = HashTestModel(name="test", value=42)
    same_unsaved_hash = hash_model_instance(same_unsaved_instance, saved_fields)
    assert_with_msg(
        unsaved_hash == same_unsaved_hash,
        "Expected instances with same field values to have same hash",
    )

    # Test that different field values produce different hash
    different_unsaved_instance = HashTestModel(name="different", value=42)
    different_unsaved_hash = hash_model_instance(
        different_unsaved_instance, saved_fields
    )
    assert_with_msg(
        unsaved_hash != different_unsaved_hash,
        "Expected instances with different field values to have different hash",
    )

    # Test with subset of fields
    name_field = next(
        f for f in saved_fields if hasattr(f, "name") and f.name == "name"
    )
    subset_fields = [name_field]
    subset_hash1 = hash_model_instance(unsaved_instance, subset_fields)
    subset_hash2 = hash_model_instance(same_unsaved_instance, subset_fields)

    assert_with_msg(
        subset_hash1 == subset_hash2,
        "Expected instances with same subset field values to have same hash",
    )

    # Test that saved and unsaved instances with same pk have different hashes
    # (saved uses pk, unsaved uses field values)
    unsaved_with_pk = HashTestModel(pk=1, name="test", value=42)
    unsaved_with_pk.pk = None  # Force it to be treated as unsaved
    unsaved_pk_hash = hash_model_instance(unsaved_with_pk, saved_fields)

    # This should be different from saved_hash because one uses pk, other uses fields
    assert_with_msg(
        saved_hash != unsaved_pk_hash,
        "Expected saved and unsaved instances to have different hash methods",
    )


class TestBaseModel:
    """Test class for BaseModel."""

    def test___str__(self) -> None:
        """Test method for __str__."""

        class TestModel(BaseModel):
            """Test model for __str__."""

            name: models.CharField[str, str] = models.CharField(max_length=100)
            value: models.IntegerField[int, int] = models.IntegerField()

            class Meta(BaseModel.Meta):
                app_label = "test_app"

        test_instance = TestModel(name="test", value=42)
        expected = "TestModel(None)"
        assert_with_msg(
            str(test_instance) == expected,
            f"Expected '{expected}', got {test_instance}",
        )

    def test___repr__(self) -> None:
        """Test method for __repr__."""

        class TestModel2(BaseModel):
            """Test model for __repr__."""

            name: models.CharField[str, str] = models.CharField(max_length=100)
            value: models.IntegerField[int, int] = models.IntegerField()

            class Meta(BaseModel.Meta):
                app_label = "test_app"

        test_instance = TestModel2(name="test", value=42)
        expected = "TestModel2(None)"
        assert_with_msg(
            repr(test_instance) == expected,
            f"Expected '{expected}', got {test_instance}",
        )

    def test_meta(self) -> None:
        """Test method for meta."""

        class TestModel3(BaseModel):
            """Test model for meta."""

            name: models.CharField[str, str] = models.CharField(max_length=100)
            value: models.IntegerField[int, int] = models.IntegerField()

            class Meta(BaseModel.Meta):
                app_label = "test_app"

        test_instance = TestModel3(name="test", value=42)
        assert_with_msg(
            test_instance.meta == test_instance._meta,  # noqa: SLF001
            "Expected meta to return _meta",
        )
