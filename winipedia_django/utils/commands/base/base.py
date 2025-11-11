"""Command utilities for Django.

This module provides utility functions for working with Django commands,
including command execution and output handling. These utilities help with
managing and automating Django command-line tasks.
"""

import logging
from abc import abstractmethod
from argparse import ArgumentParser
from typing import Any

from django.core.management import BaseCommand
from winipedia_utils.utils.oop.mixins.mixin import ABCLoggingMixin

logger = logging.getLogger(__name__)


class ABCBaseCommand(ABCLoggingMixin, BaseCommand):
    """Abstract base class for Django management commands with logging and validation.

    This class serves as a foundation for creating Django management commands that
    require abstract method implementation enforcement and automatic logging.
    It combines Django's BaseCommand with ABCImplementationLoggingMixin to provide
    both command functionality and development-time validation.

    The class implements a template method pattern where common argument handling
    and execution flow are managed by final methods, while specific implementations
    are defined through abstract methods that subclasses must implement.

    Key Features:
        - Automatic logging of method calls with performance tracking
        - Compile-time validation that all abstract methods are implemented
        - Structured argument handling with base and custom arguments
        - Template method pattern for consistent command execution flow

    Inheritance Order:
        The order of inheritance is critical: ABCImplementationLoggingMixin must
        come before BaseCommand because Django's BaseCommand doesn't call
        super().__init__(), so the mixin's metaclass initialization must happen
        first to ensure proper class construction.

    Example:
        >>> class MyCommand(ABCBaseCommand):
        ...     def add_command_arguments(self, parser):
        ...         parser.add_argument('--my-option', help='Custom option')
        ...
        ...     def handle_command(self, *args, **options):
        ...         self.stdout.write('Executing my command')

    Note:
        - All methods are automatically logged with performance tracking
        - Subclasses must implement add_command_arguments and handle_command
        - The @final decorator prevents overriding of template methods
    """

    class Options:
        """Just a container class for hard coding the option keys."""

        DRY_RUN = "dry_run"
        FORCE = "force"
        DELETE = "delete"
        YES = "yes"
        TIMEOUT = "timeout"
        BATCH_SIZE = "batch_size"
        THREADS = "threads"
        PROCESSES = "processes"

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Configure command-line arguments for the Django management command.

        This method implements the template method pattern by first adding common
        base arguments that are used across multiple commands, then delegating
        to the abstract add_command_arguments method for command-specific arguments.

        The @final decorator prevents subclasses from overriding this method,
        ensuring consistent argument handling across all commands while still
        allowing customization through the abstract method.

        Args:
            parser (ArgumentParser): Django's argument parser instance used to
                define command-line options and arguments for the command.

        Note:
            - This method is final and cannot be overridden by subclasses
            - Common arguments are added first via _add_arguments()
            - Custom arguments are added via the abstract add_command_arguments()
            - Subclasses must implement add_command_arguments() for specific needs
        """
        # add base args that are used in most commands
        self.base_add_arguments(parser)

        # add additional args that are specific to the command
        self.add_command_arguments(parser)

    def base_add_arguments(self, parser: ArgumentParser) -> None:
        """Add common command-line arguments used across multiple commands.

        This method defines base arguments that are commonly used across different
        Django management commands. These arguments provide standard functionality
        like dry-run mode, verbosity control, and batch processing options.

        The method is final to ensure consistent base argument handling, while
        command-specific arguments are handled through the abstract
        add_command_arguments method.

        Args:
            parser (ArgumentParser): Django's argument parser instance to which
                common arguments should be added.

        Note:
            - Provides standard arguments for dry-run, verbosity, and batch processing
            - The @final decorator prevents subclasses from overriding this method
            - Command-specific arguments should be added via add_command_arguments()
        """
        parser.add_argument(
            f"--{self.Options.DRY_RUN}",
            action="store_true",
            help="Show what would be done without actually executing the changes",
        )

        parser.add_argument(
            f"--{self.Options.FORCE}",
            action="store_true",
            help="Force an action in a command",
        )

        parser.add_argument(
            f"--{self.Options.DELETE}",
            action="store_true",
            help="Deleting smth in a command",
        )

        parser.add_argument(
            f"--{self.Options.YES}",
            action="store_true",
            help="Answer yes to all prompts",
            default=False,
        )

        parser.add_argument(
            f"--{self.Options.TIMEOUT}",
            type=int,
            help="Timeout for a command",
            default=None,
        )

        parser.add_argument(
            f"--{self.Options.BATCH_SIZE}",
            type=int,
            default=None,
            help="Number of items to process in each batch",
        )

        parser.add_argument(
            f"--{self.Options.THREADS}",
            type=int,
            default=None,
            help="Number of threads to use for processing",
        )

        parser.add_argument(
            f"--{self.Options.PROCESSES}",
            type=int,
            default=None,
            help="Number of processes to use for processing",
        )

    @abstractmethod
    def add_command_arguments(self, parser: ArgumentParser) -> None:
        """Add command-specific arguments to the argument parser.

        This abstract method must be implemented by subclasses to define
        command-specific command-line arguments. It is called after common
        base arguments are added, allowing each command to customize its
        argument interface while maintaining consistent base functionality.

        Subclasses should use this method to add arguments specific to their
        command's functionality, such as file paths, configuration options,
        or operational flags.

        Args:
            parser (ArgumentParser): Django's argument parser instance to which
                command-specific arguments should be added.

        Example:
            >>> def add_command_arguments(self, parser):
            ...     parser.add_argument(
            ...         '--input-file',
            ...         type=str,
            ...         required=True,
            ...         help='Path to input file'
            ...     )
            ...     parser.add_argument(
            ...         '--output-format',
            ...         choices=['json', 'csv', 'xml'],
            ...         default='json',
            ...         help='Output format for results'
            ...     )

        Note:
            - This method is abstract and must be implemented by subclasses
            - Called after _add_arguments() adds common base arguments
            - Should focus on command-specific functionality only
        """

    def handle(self, *args: Any, **options: Any) -> None:
        """Execute the Django management command using template method pattern.

        This method implements the main execution flow for the command by first
        calling common handling logic through _handle(), then delegating to
        the command-specific implementation via handle_command().

        The @final decorator ensures this execution pattern cannot be overridden,
        maintaining consistent command execution flow while allowing customization
        through the abstract handle_command method.

        Args:
            *args: Positional arguments passed from Django's command execution.
            **options: Keyword arguments containing parsed command-line options
                and their values as defined by add_arguments().

        Note:
            - This method is final and cannot be overridden by subclasses
            - Common handling logic is executed first via _handle()
            - Command-specific logic is executed via abstract handle_command()
            - All method calls are automatically logged with performance tracking
        """
        self.base_handle(*args, **options)
        self.handle_command()

    def base_handle(self, *args: Any, **options: Any) -> None:
        """Execute common handling logic shared across all commands.

        This method is intended to contain common processing logic that should
        be executed before command-specific handling. Currently, it serves as
        a placeholder for future common functionality such as logging setup,
        validation, or shared initialization.

        The method is final to ensure consistent common handling across all
        commands, while command-specific logic is handled through the abstract
        handle_command method.

        Args:
            *args: Positional arguments passed from Django's command execution.
                Currently unused but reserved for future common processing.
            **options: Keyword arguments containing parsed command-line options.
                Currently unused but reserved for future common processing.

        Note:
            - Examples might include logging setup, database connection validation, etc.
            - The @final decorator prevents subclasses from overriding this method
            - Called before handle_command() in the template method pattern
        """
        self.args = args
        self.options = options

    @abstractmethod
    def handle_command(self) -> None:
        """Execute command-specific logic and functionality.

        This abstract method must be implemented by subclasses to define the
        core functionality of the Django management command. It is called after
        common handling logic is executed, allowing each command to implement
        its specific business logic while benefiting from shared infrastructure.

        This method should contain the main logic that the command is designed
        to perform, such as data processing, database operations, file manipulation,
        or any other command-specific tasks.

        Args:
            None, args and options are stored in self.args and self.options

        Example:
            >>> def handle_command(self):
            ...     args, options = self.args, self.options
            ...     input_file = options['input_file']
            ...     dry_run = options['dry_run']  # Base argument
            ...     batch_size = options['batch_size']  # Base argument
            ...     quiet = options['quiet']  # Base argument
            ...
            ...     if dry_run:
            ...         self.stdout.write('Dry run mode - no changes will be made')
            ...
            ...     if not quiet:
            ...         msg = f'Processing {input_file} in batches of {batch_size}'
            ...         self.stdout.write(msg)
            ...
            ...     # Perform command-specific operations
            ...     self.process_file(input_file, batch_size, dry_run)
            ...
            ...     if not quiet:
            ...         self.stdout.write('Command completed successfully')

        Note:
            - This method is abstract and must be implemented by subclasses
            - Called after _handle() executes common logic
            - Should contain the main functionality of the command
            - All method calls are automatically logged with performance tracking
            - Use self.stdout.write() for output instead of print()
        """

    def get_option(self, option: str) -> Any:
        """Get an option from the command options."""
        return self.options[option]
