"""Tests module."""

from django.db import models
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.db.fields import get_field_names, get_fields, get_model_meta


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
