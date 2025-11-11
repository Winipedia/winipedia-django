"""module."""

from argparse import ArgumentParser
from collections.abc import Callable, Iterable
from typing import Any

import polars as pl
import pytest
from django.db.models import Model
from pytest_mock import MockerFixture
from winipedia_utils.utils.data.dataframe.cleaning import CleaningDF
from winipedia_utils.utils.testing.assertions import assert_with_msg

from tests.models import ModelA, ModelB
from winipedia_django.utils.commands.import_data import ImportDataBaseCommand


class MyCleaningDF(CleaningDF):
    """Concrete implementation of CleaningDF for testing."""

    STR_COL = "str_col"
    INT_COL = "int_col"

    @classmethod
    def get_rename_map(cls) -> dict[str, str]:
        """Test implementation of rename_map."""
        return {
            cls.STR_COL: "str_col_old",
            cls.INT_COL: "int_col_old",
        }

    @classmethod
    def get_col_dtype_map(cls) -> dict[str, type[pl.DataType]]:
        """Test implementation of col_cls_map."""
        return {
            cls.STR_COL: pl.Utf8,
            cls.INT_COL: pl.Int64,
        }

    @classmethod
    def get_drop_null_subsets(cls) -> tuple[tuple[str, ...], ...]:
        """Test implementation of drop_null_subsets."""
        return ((cls.STR_COL, cls.INT_COL),)

    @classmethod
    def get_fill_null_map(cls) -> dict[str, Any]:
        """Test implementation of fill_null_map."""
        return {
            cls.STR_COL: "",
            cls.INT_COL: 0,
        }

    @classmethod
    def get_sort_cols(cls) -> tuple[tuple[str, bool], ...]:
        """Test implementation of sort_cols."""
        return ((cls.INT_COL, False), (cls.STR_COL, True))

    @classmethod
    def get_unique_subsets(cls) -> tuple[tuple[str, ...], ...]:
        """Test implementation of unique_subsets."""
        return ((cls.STR_COL, cls.INT_COL),)

    @classmethod
    def get_no_null_cols(cls) -> tuple[str, ...]:
        """Test implementation of not_null_cols."""
        return (cls.STR_COL, cls.INT_COL)

    @classmethod
    def get_col_converter_map(
        cls,
    ) -> dict[str, Callable[[pl.Series], pl.Series]]:
        """Test implementation of col_converter_map."""
        # lets add 1 to the int_col
        return {
            cls.INT_COL: cls.skip_col_converter,
            cls.STR_COL: cls.skip_col_converter,
        }

    @classmethod
    def get_add_on_duplicate_cols(cls) -> tuple[str, ...]:
        """Test implementation of add_on_duplicate_cols."""
        return ()

    @classmethod
    def get_col_precision_map(cls) -> dict[str, int]:
        """Test implementation of col_precision_map."""
        return {}


@pytest.fixture
def import_data_command() -> type[ImportDataBaseCommand]:
    """Fixture for ImportDataBaseCommand."""

    class TestImportDataCommand(ImportDataBaseCommand):
        """Test class for ImportDataBaseCommand."""

        def add_command_arguments(self, parser: ArgumentParser) -> None:
            """Required implementation."""

        def handle_import(self) -> pl.DataFrame:
            """Required implementation."""
            return pl.DataFrame(
                {
                    "str_col_old": ["a", "b", "c"],
                    "int_col_old": [1, 2, 3],
                }
            )

        def get_cleaning_df_cls(self) -> type[MyCleaningDF]:
            """Required implementation."""
            return MyCleaningDF

        def get_bulks_by_model(
            self, df: pl.DataFrame
        ) -> dict[type[Model], Iterable[Model]]:
            """Required implementation."""
            bulk_a = [
                ModelA(str_field=r["str_col"], int_field=r["int_col"])
                for r in df.iter_rows(named=True)
            ]
            bulk_b = [ModelB(model_a=model_a) for model_a in bulk_a]
            return {
                ModelA: bulk_a,
                ModelB: bulk_b,
            }

    return TestImportDataCommand


class TestImportDataBaseCommand:
    """Test class for ImportDataBaseCommand."""

    @pytest.mark.django_db
    def test_handle_import(
        self, import_data_command: type[ImportDataBaseCommand]
    ) -> None:
        """Test method for handle_import."""
        cmd = import_data_command()
        df = cmd.handle_import()

        # just assert that it returns a dataframe
        assert_with_msg(
            isinstance(df, pl.DataFrame),
            f"Expected dataframe, got {type(df)}",
        )

    def test_get_cleaning_df_cls(
        self, import_data_command: type[ImportDataBaseCommand]
    ) -> None:
        """Test method for get_cleaning_df_cls."""
        cmd = import_data_command()
        df_cls = cmd.get_cleaning_df_cls()

        assert_with_msg(
            df_cls is MyCleaningDF,
            f"Expected MyCleaningDF, got {df_cls}",
        )

    def test_get_bulks_by_model(
        self, import_data_command: type[ImportDataBaseCommand]
    ) -> None:
        """Test method for get_bulks_by_model."""
        cmd = import_data_command()
        bulk_by_model = cmd.get_bulks_by_model(
            pl.DataFrame(
                {
                    "str_col": ["a", "b", "c"],
                    "int_col": [1, 2, 3],
                }
            )
        )

        assert_with_msg(
            set(bulk_by_model.keys()) == {ModelA, ModelB},
            f"Expected {{ModelA, ModelB}}, got {bulk_by_model.keys()}",
        )
        bulk_a = list(bulk_by_model[ModelA])
        bulk_b = list(bulk_by_model[ModelB])
        expected_len = 3
        assert_with_msg(
            len(bulk_a) == expected_len,
            f"Expected 3 items in bulk_a, got {len(bulk_a)}",
        )
        assert_with_msg(
            len(bulk_b) == expected_len,
            f"Expected 3 items in bulk_b, got {len(bulk_b)}",
        )

    @pytest.mark.django_db
    def test_handle_command(
        self,
        import_data_command: type[ImportDataBaseCommand],
        mocker: MockerFixture,
    ) -> None:
        """Test method for handle_command."""
        cmd = import_data_command()
        cmd.handle_command()

        # mock get data to return empty dataframe
        mocker.patch.object(
            cmd,
            "handle_import",
            return_value=pl.DataFrame(
                {
                    "str_col_old": [],
                    "int_col_old": [],
                }
            ),
        )
        cmd.handle_command()

    @pytest.mark.django_db
    def test_import_to_db(
        self, import_data_command: type[ImportDataBaseCommand], mocker: MockerFixture
    ) -> None:
        """Test method for import_to_db."""
        cmd = import_data_command()
        spy = mocker.spy(cmd, cmd.import_to_db.__name__)
        cmd.handle_command()
        spy.assert_called_once()

        # test the data
        expected_num = 3
        assert_with_msg(
            ModelA.objects.count() == expected_num,
            f"Expected {expected_num} ModelA objects, got {ModelA.objects.count()}",
        )
        assert_with_msg(
            ModelB.objects.count() == expected_num,
            f"Expected {expected_num} ModelB objects, got {ModelB.objects.count()}",
        )
