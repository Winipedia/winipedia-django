"""Tests for winipedia_django.database module."""

from django.db import models
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.database import (
    BaseModel,
    execute_sql,
    get_field_names,
    get_fields,
    get_model_meta,
    hash_model_instance,
    topological_sort_models,
)


class CustomContentType(models.Model):
    """Custom model to replace ContentType for testing."""

    app_label: models.CharField[str, str] = models.CharField(max_length=100)
    model: models.CharField[str, str] = models.CharField(max_length=100)

    class Meta:
        """Meta class for CustomContentType."""

        app_label = "test_app"
        db_table = "custom_content_type"

    def __str__(self) -> str:
        """String representation of CustomContentType."""
        return f"{self.app_label}.{self.model}"


def test_get_model_meta() -> None:
    """Test func for get_model_meta."""

    # Create a test model since Django auth models aren't available
    class MetaTestModel(models.Model):
        """Test model for get_model_meta."""

        name: models.CharField[str, str] = models.CharField(max_length=100)

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.name

    # Test that get_model_meta returns the _meta attribute
    meta = get_model_meta(MetaTestModel)
    assert_with_msg(
        meta is MetaTestModel._meta,  # noqa: SLF001
        "Expected get_model_meta to return the model's _meta attribute",
    )

    # Test that the meta object has expected attributes
    assert_with_msg(
        hasattr(meta, "db_table"),
        "Expected meta object to have db_table attribute",
    )
    assert_with_msg(
        hasattr(meta, "get_fields"),
        "Expected meta object to have get_fields method",
    )
    assert_with_msg(
        hasattr(meta, "app_label"),
        "Expected meta object to have app_label attribute",
    )

    # Test with CustomContentType model
    meta_ct = get_model_meta(CustomContentType)
    assert_with_msg(
        meta_ct is CustomContentType._meta,  # noqa: SLF001
        "Expected get_model_meta to work with CustomContentType model",
    )
    assert_with_msg(
        meta_ct.db_table == "custom_content_type",
        f"Expected CustomContentType db_table to be 'custom_content_type', "
        f"got {meta_ct.db_table}",
    )

    # Test that different models return different meta objects
    assert_with_msg(
        meta is not meta_ct,
        "Expected different models to have different meta objects",
    )


def test_get_fields() -> None:
    """Test func for get_fields."""

    # Create test models for testing
    class FieldsTestModel(models.Model):
        name: models.CharField[str, str] = models.CharField(max_length=100)
        value: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.name

    # Test with our test model
    fields = get_fields(FieldsTestModel)

    # Test that we get a list of field objects
    assert_with_msg(
        len(fields) > 0,
        "Expected TestModel to have fields",
    )

    # Test that we can find expected fields
    field_names = [f.name for f in fields if hasattr(f, "name")]
    expected_fields = {"id", "name", "value"}

    for expected_field in expected_fields:
        assert_with_msg(
            expected_field in field_names,
            f"Expected field '{expected_field}' to be in TestModel fields, "
            f"got {field_names}",
        )

    # Test with CustomContentType model to check for relationships
    ct_fields = get_fields(CustomContentType)
    ct_field_names = [f.name for f in ct_fields if hasattr(f, "name")]

    # CustomContentType model should have basic fields
    expected_ct_fields = {"id", "app_label", "model"}
    for expected_field in expected_ct_fields:
        assert_with_msg(
            expected_field in ct_field_names,
            f"Expected field '{expected_field}' to be in CustomContentType fields, "
            f"got {ct_field_names}",
        )

    # Test that different models return different field lists
    assert_with_msg(
        set(field_names) != set(ct_field_names),
        "Expected different models to have different field names",
    )


def test_get_field_names() -> None:
    """Test func for get_field_names."""

    # Create test model for testing
    class FieldNamesTestModel(models.Model):
        """Test model for get_field_names."""

        name: models.CharField[str, str] = models.CharField(max_length=100)
        value: models.IntegerField[int, int] = models.IntegerField()

        class Meta:
            app_label = "test_app"

        def __str__(self) -> str:
            return self.name

    # Test with test model fields
    fields = get_fields(FieldNamesTestModel)
    field_names = get_field_names(fields)

    # Test that we get a list of strings
    assert_with_msg(
        len(field_names) > 0,
        "Expected get_field_names to return non-empty list",
    )

    # Test that expected field names are present
    expected_names = {"id", "name", "value"}
    for expected_name in expected_names:
        assert_with_msg(
            expected_name in field_names,
            f"Expected field name '{expected_name}' to be in field names, "
            f"got {field_names}",
        )

    # Test with CustomContentType model
    ct_fields = get_fields(CustomContentType)
    ct_field_names = get_field_names(ct_fields)

    expected_ct_names = {"id", "app_label", "model"}
    for expected_name in expected_ct_names:
        assert_with_msg(
            expected_name in ct_field_names,
            f"Expected field name '{expected_name}' to be in CustomContentType "
            f"field names, got {ct_field_names}",
        )

    # Test that different models have different field names
    assert_with_msg(
        set(field_names) != set(ct_field_names),
        "Expected different models to have different field names",
    )

    # Test with empty list
    empty_field_names = get_field_names([])
    assert_with_msg(
        empty_field_names == [],
        "Expected get_field_names with empty list to return empty list",
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
    empty_result = topological_sort_models([])
    assert_with_msg(
        empty_result == [],
        "Expected empty list to return empty list",
    )


def test_execute_sql() -> None:
    """Test func for execute_sql."""
    # Test basic SQL execution without parameters
    sql = "SELECT 1 as test_column"
    columns, rows = execute_sql(sql)

    # Test that we get expected column names and results
    assert_with_msg(
        columns == ["test_column"],
        f"Expected columns ['test_column'], got {columns}",
    )
    assert_with_msg(
        len(rows) == 1,
        f"Expected 1 row, got {len(rows)}",
    )
    assert_with_msg(
        rows[0] == (1,),
        f"Expected row (1,), got {rows[0]}",
    )

    # Test SQL with multiple columns and rows
    multi_sql = "SELECT 1 as col1, 'test' as col2 UNION SELECT 2, 'test2'"
    multi_columns, multi_rows = execute_sql(multi_sql)

    assert_with_msg(
        multi_columns == ["col1", "col2"],
        f"Expected columns ['col1', 'col2'], got {multi_columns}",
    )
    assert_with_msg(
        len(multi_rows) == 2,  # noqa: PLR2004
        f"Expected 2 rows, got {len(multi_rows)}",
    )

    # Test SQL with parameters (using SQLite parameter style)
    param_sql = "SELECT %(test_value)s as param_value"
    param_columns, param_rows = execute_sql(param_sql, {"test_value": "test_param"})

    assert_with_msg(
        param_columns == ["param_value"],
        f"Expected columns ['param_value'], got {param_columns}",
    )
    assert_with_msg(
        len(param_rows) == 1,
        f"Expected 1 row, got {len(param_rows)}",
    )
    assert_with_msg(
        param_rows[0] == ("test_param",),
        f"Expected row ('test_param',), got {param_rows[0]}",
    )

    # Test with empty result set
    empty_sql = "SELECT 1 as empty_col WHERE 1=0"
    empty_columns, empty_rows = execute_sql(empty_sql)

    assert_with_msg(
        empty_columns == ["empty_col"],
        f"Expected columns ['empty_col'], got {empty_columns}",
    )
    assert_with_msg(
        len(empty_rows) == 0,
        f"Expected 0 rows, got {len(empty_rows)}",
    )

    # Test that function returns tuple
    result = execute_sql("SELECT 1")
    assert_with_msg(
        type(result) is tuple,
        f"Expected result to be tuple, got {type(result)}",
    )
    assert_with_msg(
        len(result) == 2,  # noqa: PLR2004
        f"Expected tuple of length 2, got {len(result)}",
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
        expected = (
            "TestModel(id=None, created_at=None, updated_at=None, name=test, value=42)"
        )
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
        expected = (
            "TestModel2(id=None, created_at=None, updated_at=None, name=test, value=42)"
        )
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
