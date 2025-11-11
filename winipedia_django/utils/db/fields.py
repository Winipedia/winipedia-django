"""Fields module.

Utils for working with Django model fields.
"""

from typing import TYPE_CHECKING, Any

from django.db.models import Field, Model

if TYPE_CHECKING:
    from django.contrib.contenttypes.fields import GenericForeignKey
    from django.db.models.fields.related import ForeignObjectRel
    from django.db.models.options import Options


def get_field_names(
    fields: "list[Field[Any, Any] | ForeignObjectRel | GenericForeignKey]",
) -> list[str]:
    """Get the names of all fields from a Django model including relationships.

    Retrieves the names of all field objects from a Django model, including
    regular fields, foreign key relationships, reverse foreign key relationships,
    and generic foreign keys. This provides a comprehensive view of all model
    attributes that can be used for introspection, validation, or bulk operations.

    Args:
        fields (list[Field | ForeignObjectRel | GenericForeignKey]):
            The list of field objects to get names from.

    Returns:
        list[str]: A list containing the names of all fields.

    Example:
        >>> from django.contrib.auth.models import User
        >>> fields = get_fields(User)
        >>> field_names = get_field_names(fields)
        >>> 'username' in field_names
        True
        >>> 'email' in field_names
        True
    """
    return [field.name for field in fields]


def get_model_meta(model: type[Model]) -> "Options[Model]":
    """Get the Django model metadata options object.

    Retrieves the _meta attribute from a Django model class, which contains
    metadata about the model including field definitions, table name, and
    other model configuration options. This is a convenience wrapper around
    accessing the private _meta attribute directly.

    Args:
        model (type[Model]): The Django model class to get metadata from.

    Returns:
        Options[Model]: The model's metadata options object containing
            field definitions, table information, and other model configuration.

    Example:
        >>> from django.contrib.auth.models import User
        >>> meta = get_model_meta(User)
        >>> meta.db_table
        'auth_user'
        >>> len(meta.get_fields())
        11
    """
    return model._meta  # noqa: SLF001


def get_fields[TModel: Model](
    model: type[TModel],
) -> "list[Field[Any, Any] | ForeignObjectRel | GenericForeignKey]":
    """Get all fields from a Django model including relationships.

    Retrieves all field objects from a Django model, including regular fields,
    foreign key relationships, reverse foreign key relationships, and generic
    foreign keys. This provides a comprehensive view of all model attributes
    that can be used for introspection, validation, or bulk operations.

    Args:
        model (type[Model]): The Django model class to get fields from.

    Returns:
        list[Field | ForeignObjectRel | GenericForeignKey]: A list
            containing all field objects associated with the model, including:
            - Regular model fields (CharField, IntegerField, etc.)
            - Foreign key fields (ForeignKey, OneToOneField, etc.)
            - Reverse relationship fields (ForeignObjectRel)
            - Generic foreign key fields (GenericForeignKey)

    Example:
        >>> from django.contrib.auth.models import User
        >>> fields = get_fields(User)
        >>> field_names = [f.name for f in fields if hasattr(f, 'name')]
        >>> 'username' in field_names
        True
        >>> 'email' in field_names
        True
    """
    return get_model_meta(model).get_fields()
