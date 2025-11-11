"""Bulk utilities for Django models.

This module provides utility functions for working with Django models,
including bulk operations and validation. These utilities help with
efficiently managing large amounts of data in Django applications.
"""

from collections import defaultdict
from collections.abc import Callable, Generator, Iterable
from functools import partial
from itertools import islice
from typing import TYPE_CHECKING, Any, Literal, cast, get_args, overload

from django.db import router, transaction
from django.db.models import (
    Field,
    Model,
    QuerySet,
)
from django.db.models.deletion import Collector
from winipedia_utils.utils.iterating.concurrent.multithreading import multithread_loop
from winipedia_utils.utils.logging.logger import get_logger

from winipedia_django.utils.db.models import (
    hash_model_instance,
    topological_sort_models,
)

if TYPE_CHECKING:
    from django.contrib.contenttypes.fields import GenericForeignKey
    from django.db.models.fields.related import ForeignObjectRel

logger = get_logger(__name__)

MODE_TYPES = Literal["create", "update", "delete"]
MODES = get_args(MODE_TYPES)

MODE_CREATE = MODES[0]
MODE_UPDATE = MODES[1]
MODE_DELETE = MODES[2]

STANDARD_BULK_SIZE = 1000


def bulk_create_in_steps[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int = STANDARD_BULK_SIZE,
) -> list[TModel]:
    """Create model instances from bulk and saves them to the database in steps.

    Takes a list of model instances and creates them in the database in steps.
    This is useful when you want to create a large number of objects
    in the database. It also uses multithreading to speed up the process.

    Args:
        model (type[Model]): The Django model class to create.
        bulk (Iterable[Model]): a list of model instances to create.
        step (int, optional): The step size of the bulk creation.
                              Defaults to STANDARD_BULK_SIZE.

    Returns:
        list[Model]: a list of created objects.
    """
    return cast(
        "list[TModel]",
        bulk_method_in_steps(model=model, bulk=bulk, step=step, mode=MODE_CREATE),
    )


def bulk_update_in_steps[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    update_fields: list[str],
    step: int = STANDARD_BULK_SIZE,
) -> int:
    """Update model instances in the database in steps using multithreading.

    Takes a list of model instances and updates them in the database in chunks.
    This is useful when you want to update a large number of objects efficiently.
    Uses multithreading to speed up the process by processing chunks in parallel.

    Args:
        model (type[Model]): The Django model class to update.
        bulk (Iterable[Model]): A list of model instances to update.
        update_fields (list[str]): List of field names to update on the models.
        step (int, optional): The step size for bulk updates.
                              Defaults to STANDARD_BULK_SIZE.

    Returns:
        int: Total number of objects updated across all chunks.
    """
    return cast(
        "int",
        bulk_method_in_steps(
            model=model, bulk=bulk, step=step, mode=MODE_UPDATE, fields=update_fields
        ),
    )


def bulk_delete_in_steps[TModel: Model](
    model: type[TModel], bulk: Iterable[TModel], step: int = STANDARD_BULK_SIZE
) -> tuple[int, dict[str, int]]:
    """Delete model instances from the database in steps using multithreading.

    Takes a list of model instances and deletes them from the database in chunks.
    This is useful when you want to delete a large number of objects efficiently.
    Uses multithreading to speed up the process by processing chunks in parallel.
    Also handles cascade deletions according to model relationships.

    Args:
        model (type[Model]): The Django model class to update.
        bulk (Iterable[Model]): A list of model instances to delete.
        step (int, optional): The step size for bulk deletions.
                              Defaults to STANDARD_BULK_SIZE.

    Returns:
        tuple[int, dict[str, int]]: A tuple containing the
                                    total count of deleted objects
            and a dictionary mapping model names to their deletion counts.
    """
    return cast(
        "tuple[int, dict[str, int]]",
        bulk_method_in_steps(
            model=model,
            bulk=bulk,
            step=step,
            mode=MODE_DELETE,
        ),
    )


@overload
def bulk_method_in_steps[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: Literal["create"],
    **kwargs: Any,
) -> list[TModel]: ...


@overload
def bulk_method_in_steps[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: Literal["update"],
    **kwargs: Any,
) -> int: ...


@overload
def bulk_method_in_steps[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: Literal["delete"],
    **kwargs: Any,
) -> tuple[int, dict[str, int]]: ...


def bulk_method_in_steps[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: MODE_TYPES,
    **kwargs: Any,
) -> int | tuple[int, dict[str, int]] | list[TModel]:
    """Execute bulk operations on model instances in steps with transaction handling.

    This is the core function that handles bulk create, update, or delete operations
    by dividing the work into manageable chunks and processing them with multithreading.
    It includes transaction safety checks and delegates to the atomic version.

    Args:
        model (type[Model]): The Django model class to perform operations on.
        bulk (Iterable[Model]): A list of model instances to process.
        step (int): The step size for chunking the bulk operations.
        mode (MODE_TYPES): The operation mode - 'create', 'update', or 'delete'.
        **kwargs: Additional keyword arguments passed to the bulk operation methods.

    Returns:
        None | int | tuple[int, dict[str, int]] | list[Model]:
        The result depends on mode:
            - create: list of created model instances
            - update: integer count of updated objects
            - delete: tuple of (total_count, count_by_model_dict)
            - None if bulk is empty
    """
    # check if we are inside a transaction.atomic block
    _in_atomic_block = transaction.get_connection().in_atomic_block
    if _in_atomic_block:
        logger.info(
            "BE CAREFUL USING BULK OPERATIONS INSIDE A BROADER TRANSACTION BLOCK. "
            "BULKING WITH BULKS THAT DEPEND ON EACH OTHER CAN CAUSE "
            "INTEGRITY ERRORS OR POTENTIAL OTHER ISSUES."
        )
    return bulk_method_in_steps_atomic(
        model=model, bulk=bulk, step=step, mode=mode, **kwargs
    )


# Overloads for bulk_method_in_steps_atomic
@overload
@transaction.atomic
def bulk_method_in_steps_atomic[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: Literal["create"],
    **kwargs: Any,
) -> list[TModel]: ...


@overload
@transaction.atomic
def bulk_method_in_steps_atomic[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: Literal["update"],
    **kwargs: Any,
) -> int: ...


@overload
@transaction.atomic
def bulk_method_in_steps_atomic[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: Literal["delete"],
    **kwargs: Any,
) -> tuple[int, dict[str, int]]: ...


@transaction.atomic
def bulk_method_in_steps_atomic[TModel: Model](
    model: type[TModel],
    bulk: Iterable[TModel],
    step: int,
    mode: MODE_TYPES,
    **kwargs: Any,
) -> int | tuple[int, dict[str, int]] | list[TModel]:
    """Bulk create, update or delete the given list of objects in steps.

    WHEN BULK CREATING OR UPDATING A BULK
    AND THEN A SECOND BULK THAT DEPENDS ON THE FIRST BULK,
    YOU WILL RUN INTO A INTEGRITY ERROR IF YOU DO THE
    ENTIRE THING IN AN @transaction.atomic DECORATOR.
    REMOVE THE DECORATORS THAT ARE HIGHER UP THAN THE ONE OF THIS FUNCTION
    TO AVOID THIS ERROR.

    Args:
        model (type[Model]): The Django model class to perform operations on.
        bulk (Iterable[Model]): A list of model instances to process.
        step (int): number of objects to process in one chunk
        mode (MODE_TYPES): The operation mode - 'create', 'update', or 'delete'.
        **kwargs: Additional keyword arguments passed to the bulk operation methods.

    Returns:
        None | int | tuple[int, dict[str, int]] | list[Model]:
        The result depends on mode:
            - create: list of created model instances
            - update: integer count of updated objects
            - delete: tuple of (total_count, count_by_model_dict)
            - None if bulk is empty
    """
    bulk_method = get_bulk_method(model=model, mode=mode, **kwargs)

    chunks = get_step_chunks(bulk=bulk, step=step)

    # multithreading significantly increases speed
    result = multithread_loop(
        process_function=bulk_method,
        process_args=chunks,
    )

    return flatten_bulk_in_steps_result(result=result, mode=mode)


def get_step_chunks(
    bulk: Iterable[Model], step: int
) -> Generator[tuple[list[Model]], None, None]:
    """Yield chunks of the given size from the bulk.

    Args:
        bulk (Iterable[Model]): The bulk to chunk.
        step (int): The size of each chunk.

    Yields:
        Generator[list[Model], None, None]: Chunks of the bulk.
    """
    bulk = iter(bulk)
    while True:
        chunk = list(islice(bulk, step))
        if not chunk:
            break
        yield (chunk,)  # bc concurrent_loop expects a tuple of args


# Overloads for get_bulk_method
@overload
def get_bulk_method(
    model: type[Model], mode: Literal["create"], **kwargs: Any
) -> Callable[[list[Model]], list[Model]]: ...


@overload
def get_bulk_method(
    model: type[Model], mode: Literal["update"], **kwargs: Any
) -> Callable[[list[Model]], int]: ...


@overload
def get_bulk_method(
    model: type[Model], mode: Literal["delete"], **kwargs: Any
) -> Callable[[list[Model]], tuple[int, dict[str, int]]]: ...


def get_bulk_method(
    model: type[Model], mode: MODE_TYPES, **kwargs: Any
) -> Callable[[list[Model]], list[Model] | int | tuple[int, dict[str, int]]]:
    """Get the appropriate bulk method function based on the operation mode.

    Creates and returns a function that performs the specified bulk operation
    (create, update, or delete) on a chunk of model instances. The returned
    function is configured with the provided kwargs.

    Args:
        model (type[Model]): The Django model class to perform operations on.
        mode (MODE_TYPES): The operation mode - 'create', 'update', or 'delete'.
        **kwargs: Additional keyword arguments to pass to the bulk operation method.

    Raises:
        ValueError: If the mode is not one of the valid MODE_TYPES.

    Returns:
        Callable[[list[Model]], Any]: A function that performs the bulk operation
            on a chunk of model instances.
    """
    bulk_method: Callable[[list[Model]], list[Model] | int | tuple[int, dict[str, int]]]
    if mode == MODE_CREATE:

        def bulk_create_chunk(chunk: list[Model]) -> list[Model]:
            return model.objects.bulk_create(objs=chunk, **kwargs)

        bulk_method = bulk_create_chunk
    elif mode == MODE_UPDATE:

        def bulk_update_chunk(chunk: list[Model]) -> int:
            return model.objects.bulk_update(objs=chunk, **kwargs)

        bulk_method = bulk_update_chunk
    elif mode == MODE_DELETE:

        def bulk_delete_chunk(chunk: list[Model]) -> tuple[int, dict[str, int]]:
            return bulk_delete(model=model, objs=chunk, **kwargs)

        bulk_method = bulk_delete_chunk
    else:
        msg = f"Invalid method. Must be one of {MODES}"
        raise ValueError(msg)

    return bulk_method


# Overloads for flatten_bulk_in_steps_result
@overload
def flatten_bulk_in_steps_result[TModel: Model](
    result: list[list[TModel]], mode: Literal["create"]
) -> list[TModel]: ...


@overload
def flatten_bulk_in_steps_result[TModel: Model](
    result: list[int], mode: Literal["update"]
) -> int: ...


@overload
def flatten_bulk_in_steps_result[TModel: Model](
    result: list[tuple[int, dict[str, int]]], mode: Literal["delete"]
) -> tuple[int, dict[str, int]]: ...


def flatten_bulk_in_steps_result[TModel: Model](
    result: list[int] | list[tuple[int, dict[str, int]]] | list[list[TModel]], mode: str
) -> int | tuple[int, dict[str, int]] | list[TModel]:
    """Flatten and aggregate results from multithreaded bulk operations.

    Processes the results returned from parallel bulk operations and aggregates
    them into the appropriate format based on the operation mode. Handles
    different return types for create, update, and delete operations.

    Args:
        result (list[Any]): List of results from each chunk operation.
        mode (str): The operation mode - 'create', 'update', or 'delete'.

    Raises:
        ValueError: If the mode is not one of the valid operation modes.

    Returns:
        None | int | tuple[int, dict[str, int]] | list[Model]: Aggregated result:
            - update: sum of updated object counts
            - delete: tuple of (total_count, count_by_model_dict)
            - create: flattened list of all created objects
    """
    if mode == MODE_UPDATE:
        # formated as [1000, 1000, ...]
        # since django 4.2 bulk_update returns the count of updated objects
        result = cast("list[int]", result)
        return int(sum(result))
    if mode == MODE_DELETE:
        # formated as [(count, {model_name: count, model_cascade_name: count}), ...]
        # join the results to get the total count of deleted objects
        result = cast("list[tuple[int, dict[str, int]]]", result)
        total_count = 0
        count_sum_by_model: defaultdict[str, int] = defaultdict(int)
        for count_sum, count_by_model in result:
            total_count += count_sum
            for model_name, count in count_by_model.items():
                count_sum_by_model[model_name] += count
        return (total_count, dict(count_sum_by_model))
    if mode == MODE_CREATE:
        # formated as [[obj1, obj2, ...], [obj1, obj2, ...], ...]
        result = cast("list[list[TModel]]", result)
        return [item for sublist in result for item in sublist]

    msg = f"Invalid method. Must be one of {MODES}"
    raise ValueError(msg)


def bulk_delete(
    model: type[Model], objs: Iterable[Model], **_: Any
) -> tuple[int, dict[str, int]]:
    """Delete model instances using Django's QuerySet delete method.

    Deletes the provided model instances from the database using Django's
    built-in delete functionality. Handles both individual model instances
    and QuerySets, and returns deletion statistics including cascade counts.

    Args:
        model (type[Model]): The Django model class to delete from.
        objs (list[Model]): A list of model instances to delete.

    Returns:
        tuple[int, dict[str, int]]: A tuple containing the total count of deleted
            objects and a dictionary mapping model names to their deletion counts.
    """
    if not isinstance(objs, QuerySet):
        objs = list(objs)
        pks = [obj.pk for obj in objs]
        query_set = model.objects.filter(pk__in=pks)
    else:
        query_set = objs

    return query_set.delete()


def bulk_create_bulks_in_steps[TModel: Model](
    bulk_by_class: dict[type[TModel], Iterable[TModel]],
    step: int = STANDARD_BULK_SIZE,
) -> dict[type[TModel], list[TModel]]:
    """Create multiple bulks of different model types in dependency order.

    Takes a dictionary mapping model classes to lists of instances and creates
    them in the database in the correct order based on model dependencies.
    Uses topological sorting to ensure foreign key constraints are satisfied.

    Args:
        bulk_by_class (dict[type[Model], list[Model]]): Dictionary mapping model classes
            to lists of instances to create.
        step (int, optional): The step size for bulk creation. Defaults to 1000.
        validate (bool, optional): Whether to validate instances before creation.
        Defaults to True.

    Returns:
        dict[type[Model], list[Model]]: Dictionary mapping model classes to lists
            of created instances.
    """
    # order the bulks in order of creation depending how they depend on each other
    models_ = list(bulk_by_class.keys())
    ordered_models = topological_sort_models(models=models_)

    results: dict[type[TModel], list[TModel]] = {}
    for model_ in ordered_models:
        bulk = bulk_by_class[model_]
        result = bulk_create_in_steps(model=model_, bulk=bulk, step=step)
        results[model_] = result

    return results


def get_differences_between_bulks(
    bulk1: list[Model],
    bulk2: list[Model],
    fields: "list[Field[Any, Any] | ForeignObjectRel | GenericForeignKey]",
) -> tuple[list[Model], list[Model], list[Model], list[Model]]:
    """Compare two bulks and return their differences and intersections.

    Compares two lists of model instances by computing hashes of their field values
    and returns the differences and intersections between them. Optionally allows
    specifying which fields to compare and the depth of comparison for related objects.

    Args:
        bulk1 (list[Model]): First list of model instances to compare.
        bulk2 (list[Model]): Second list of model instances to compare.
        fields (list[Field] | None, optional): List of fields to compare.
            Defaults to None, which compares all fields.
        max_depth (int | None, optional): Maximum depth for comparing related objects.
            Defaults to None.

    Raises:
        ValueError: If the two bulks contain different model types.

    Returns:
        tuple[list[Model], list[Model], list[Model], list[Model]]: A tuple containing:
            - Objects in bulk1 but not in bulk2
            - Objects in bulk2 but not in bulk1
            - Objects in both bulk1 and bulk2 (from bulk1)
            - Objects in both bulk1 and bulk2 (from bulk2)
    """
    if not bulk1 or not bulk2:
        return bulk1, bulk2, [], []

    if type(bulk1[0]) is not type(bulk2[0]):
        msg = "Both bulks must be of the same model type."
        raise ValueError(msg)

    hash_model_instance_with_fields = partial(
        hash_model_instance,
        fields=fields,
    )
    # Precompute hashes and map them directly to models in a single pass for both bulks
    hashes1 = list(map(hash_model_instance_with_fields, bulk1))
    hashes2 = list(map(hash_model_instance_with_fields, bulk2))

    # Convert keys to sets for difference operations
    set1, set2 = set(hashes1), set(hashes2)

    # Calculate differences between sets
    # Find differences and intersection with original order preserved
    # Important, we need to return the original objects that are the same in memory,
    # so in_1_not_2 and in_2_not_1
    in_1_not_2 = set1 - set2
    in_1_not_2_list = [
        model
        for model, hash_ in zip(bulk1, hashes1, strict=False)
        if hash_ in in_1_not_2
    ]

    in_2_not_1 = set2 - set1
    in_2_not_1_list = [
        model
        for model, hash_ in zip(bulk2, hashes2, strict=False)
        if hash_ in in_2_not_1
    ]

    in_1_and_2 = set1 & set2
    in_1_and_2_from_1 = [
        model
        for model, hash_ in zip(bulk1, hashes1, strict=False)
        if hash_ in in_1_and_2
    ]
    in_1_and_2_from_2 = [
        model
        for model, hash_ in zip(bulk2, hashes2, strict=False)
        if hash_ in in_1_and_2
    ]

    return in_1_not_2_list, in_2_not_1_list, in_1_and_2_from_1, in_1_and_2_from_2


def simulate_bulk_deletion(
    model_class: type[Model], entries: list[Model]
) -> dict[type[Model], set[Model]]:
    """Simulate bulk deletion to preview what objects would be deleted.

    Uses Django's Collector to simulate the deletion process and determine
    which objects would be deleted due to cascade relationships, without
    actually performing the deletion. Useful for previewing deletion effects.

    Args:
        model_class (type[Model]): The Django model class of the entries to delete.
        entries (list[Model]): List of model instances to simulate deletion for.

    Returns:
        dict[type[Model], set[Model]]: Dictionary mapping model classes to sets
            of objects that would be deleted, including cascade deletions.
    """
    if not entries:
        return {}

    # Initialize the Collector
    using = router.db_for_write(model_class)
    collector = Collector(using)

    # Collect deletion cascade for all entries
    collector.collect(entries)

    # Prepare the result dictionary
    deletion_summary: defaultdict[type[Model], set[Model]] = defaultdict(set)

    # Add normal deletes
    for model, objects in collector.data.items():
        deletion_summary[model].update(objects)  # objects is already iterable

    # Add fast deletes (explicitly expand querysets)
    for queryset in collector.fast_deletes:
        deletion_summary[queryset.model].update(list(queryset))

    return deletion_summary


def multi_simulate_bulk_deletion(
    entries: dict[type[Model], list[Model]],
) -> dict[type[Model], set[Model]]:
    """Simulate bulk deletion for multiple model types and aggregate results.

    Performs deletion simulation for multiple model types and combines the results
    into a single summary. This is useful when you want to preview the deletion
    effects across multiple related model types.

    Args:
        entries (dict[type[Model], list[Model]]): Dictionary mapping model classes
            to lists of instances to simulate deletion for.

    Returns:
        dict[type[Model], set[Model]]: Dictionary mapping model classes to sets
            of all objects that would be deleted across all simulations.
    """
    deletion_summaries = [
        simulate_bulk_deletion(model, entry) for model, entry in entries.items()
    ]
    # join the dicts to get the total count of deleted objects
    joined_deletion_summary = defaultdict(set)
    for deletion_summary in deletion_summaries:
        for model, objects in deletion_summary.items():
            joined_deletion_summary[model].update(objects)

    return dict(joined_deletion_summary)
