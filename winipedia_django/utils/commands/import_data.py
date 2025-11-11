"""Import command for Django.

A base class for importing data from a source to the database.
"""

from abc import abstractmethod
from collections.abc import Iterable

import polars as pl
from django.db.models import Model
from winipedia_utils.utils.data.dataframe.cleaning import CleaningDF
from winipedia_utils.utils.logging.logger import get_logger

from winipedia_django.utils.commands.base.base import ABCBaseCommand
from winipedia_django.utils.db.bulk import bulk_create_bulks_in_steps

logger = get_logger(__name__)


class ImportDataBaseCommand(ABCBaseCommand):
    """Base class for importing data from a source to the database.

    This class provides a standardized way to import data from a source to the database.
    It uses the cleaning df cls to clean the data and then imports it to the database.
    """

    @abstractmethod
    def handle_import(self) -> pl.DataFrame:
        """Handle importing the data from the source.

        The data is possibly dirty and the job of this class to standardize the process
        of importing the data.
        """

    @abstractmethod
    def get_cleaning_df_cls(self) -> type[CleaningDF]:
        """You will define a child of a cleaning df cls and return it.

        The cleaning df cls is responsible for cleaning the data.
        See: winipedia_utils.utils.data.dataframe.cleaning.CleaningDF
        """

    @abstractmethod
    def get_bulks_by_model(
        self, df: pl.DataFrame
    ) -> dict[type[Model], Iterable[Model]]:
        """Get the bulks of data to import by model.

        The data is cleaned and ready to be imported to the database.
        You need to return a dictionary mapping model classes to lists of instances
        to create.

        Args:
            df (pl.DataFrame): The cleaned data to import.
                Is passed from handle_command() automatically.
        """

    def handle_command(self) -> None:
        """Execute the import command.

        This method handles the import process from start to finish.
        It imports the data with handle_import(), cleans it with the cleaning df cls,
        and then imports it to the database.
        """
        data_df = self.handle_import()

        cleaning_df_cls = self.get_cleaning_df_cls()
        self.cleaning_df = cleaning_df_cls(data_df)

        self.import_to_db()

    def import_to_db(self) -> None:
        """Import the cleaned data to the database."""
        bulks_by_model = self.get_bulks_by_model(df=self.cleaning_df.df)

        bulk_create_bulks_in_steps(bulks_by_model)
