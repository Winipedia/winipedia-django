"""Tests for winipedia_django.command module."""

from argparse import ArgumentParser
from typing import Any, final

import pytest
from winipedia_utils.utils.oop.mixins.mixin import ABCLoggingMixin
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.commands.base.command import ABCBaseCommand


class TestABCBaseCommand:
    """Test class for ABCBaseCommand."""

    def test_add_arguments(self) -> None:
        """Test method for add_arguments."""

        # Test that add_arguments follows template method pattern correctly
        class TestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Track that this method was called."""
                self.add_command_arguments_called = True
                parser.add_argument("--custom", help="Custom argument")

            @final
            def handle_command(self, *_args: Any, **_options: Any) -> None:
                """Required implementation."""

        # Test the template method pattern by checking arguments are added
        command = TestCommand()
        parser = ArgumentParser()
        command.add_arguments(parser)

        # Verify that both base and custom arguments were added
        added_arguments: set[str] = set()
        for action in parser._actions:  # noqa: SLF001
            if action.option_strings:
                added_arguments.update(action.option_strings)

        # Check that base arguments were added (from _add_arguments)
        assert_with_msg(
            "--dry-run" in added_arguments,
            "Expected base argument --dry-run to be added",
        )
        assert_with_msg(
            "--custom" in added_arguments,
            "Expected custom argument --custom to be added",
        )
        assert_with_msg(
            command.add_command_arguments_called,
            "Expected add_command_arguments to be called",
        )

        # Test that the method is final
        assert_with_msg(
            hasattr(ABCBaseCommand.add_arguments, "__final__"),
            "Expected add_arguments to be marked as final",
        )

        # Test class inheritance and structure
        base_classes = ABCBaseCommand.__bases__
        base_class_names = [cls.__name__ for cls in base_classes]

        assert_with_msg(
            f"{ABCLoggingMixin.__name__}" in base_class_names,
            f"Expected {ABCLoggingMixin.__name__} in base classes, "
            f"got {base_class_names}",
        )
        assert_with_msg(
            "BaseCommand" in base_class_names,
            f"Expected BaseCommand in base classes, got {base_class_names}",
        )

        # Test that class has expected abstract methods
        abstract_methods: set[str] = getattr(
            ABCBaseCommand, "__abstractmethods__", set()
        )
        expected_abstract_methods = {"add_command_arguments", "handle_command"}
        assert_with_msg(
            expected_abstract_methods.issubset(abstract_methods),
            f"Expected abstract methods {expected_abstract_methods}, "
            f"got {abstract_methods}",
        )

    def test__add_arguments(self) -> None:
        """Test method for _add_arguments."""

        # Test that _add_arguments adds all expected common arguments
        class TestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""

            @final
            def handle_command(self, *args: Any, **options: Any) -> None:
                """Required implementation."""

        command = TestCommand()
        parser = ArgumentParser()

        # Call _add_arguments directly
        command._add_arguments(parser)  # noqa: SLF001

        # Test that all expected arguments were added
        expected_arguments = {
            "--dry-run",
            "--size",
            "--force",
            "--delete",
            "--quiet",
            "--debug",
            "--yes",
            "--config",
            "--timeout",
            "--batch-size",
            "--no-input",
            "--threads",
            "--processes",
        }

        # Get all argument names from parser
        added_arguments: set[str] = set()
        for action in parser._actions:  # noqa: SLF001
            if action.option_strings:
                added_arguments.update(action.option_strings)

        for expected_arg in expected_arguments:
            assert_with_msg(
                expected_arg in added_arguments,
                f"Expected argument {expected_arg} to be added, got {added_arguments}",
            )

        # Test specific argument properties
        action_map = {
            action.dest: action
            for action in parser._actions  # noqa: SLF001
            if action.dest != "help"
        }

        # Constants for expected values
        expected_batch_size_default = None
        expected_timeout_default = None
        expected_yes_default = False

        # Test boolean arguments by checking their action type
        assert_with_msg(
            action_map["force"].__class__.__name__ == "_StoreTrueAction",
            "Expected force to be store_true action",
        )
        assert_with_msg(
            action_map["yes"].default == expected_yes_default,
            f"Expected yes default to be {expected_yes_default}",
        )

        # Test integer arguments
        assert_with_msg(
            action_map["size"].type is int,
            "Expected size to be int type",
        )
        assert_with_msg(
            action_map["batch_size"].default == expected_batch_size_default,
            f"Expected batch_size default to be {expected_batch_size_default}",
        )
        assert_with_msg(
            action_map["timeout"].default == expected_timeout_default,
            f"Expected timeout default to be {expected_timeout_default}",
        )

        # Test string arguments
        assert_with_msg(
            action_map["config"].type is str,
            "Expected config to be str type",
        )

        # Test that the method is final
        assert_with_msg(
            hasattr(ABCBaseCommand._add_arguments, "__final__"),  # noqa: SLF001
            "Expected _add_arguments to be marked as final",
        )

    def test_add_command_arguments(self) -> None:
        """Test method for add_command_arguments."""
        # Test that add_command_arguments is abstract and must be implemented
        abstract_methods: set[str] = getattr(
            ABCBaseCommand, "__abstractmethods__", set()
        )
        assert_with_msg(
            "add_command_arguments" in abstract_methods,
            "Expected add_command_arguments to be abstract",
        )

        # Test that concrete implementation works correctly
        class ConcreteTestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Add test-specific arguments."""
                parser.add_argument("--test-arg", help="Test argument")

            @final
            def handle_command(self, *args: Any, **options: Any) -> None:
                """Handle test command execution."""

        # Test that concrete implementation can be instantiated
        command = ConcreteTestCommand()
        assert_with_msg(
            command.__class__.__bases__[0] is ABCBaseCommand,
            "Expected concrete command to inherit from ABCBaseCommand",
        )

        # Test that the method can add custom arguments
        parser = ArgumentParser()
        command.add_command_arguments(parser)

        # Check that custom argument was added
        added_arguments: set[str] = set()
        for action in parser._actions:  # noqa: SLF001
            if action.option_strings:
                added_arguments.update(action.option_strings)

        assert_with_msg(
            "--test-arg" in added_arguments,
            "Expected custom argument --test-arg to be added",
        )

    def test_handle(self) -> None:
        """Test method for handle."""

        # Test that handle follows template method pattern correctly
        class TestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""

            @final
            def handle_command(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
                """Track that this method was called."""
                self.handle_command_called = True

        command = TestCommand()

        # Test the template method pattern
        command.handle()

        assert_with_msg(
            command.handle_command_called,
            "Expected handle_command to be called by handle",
        )

        # Test that the method is final
        assert_with_msg(
            hasattr(ABCBaseCommand.handle, "__final__"),
            "Expected handle to be marked as final",
        )

        # Test integration with Django's BaseCommand functionality
        assert_with_msg(
            hasattr(command, "stdout"),
            "Expected command to have stdout from BaseCommand",
        )
        assert_with_msg(
            hasattr(command, "stderr"),
            "Expected command to have stderr from BaseCommand",
        )
        assert_with_msg(
            hasattr(command, "style"),
            "Expected command to have style from BaseCommand",
        )

    def test__handle(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test method for _handle."""

        # Test that _handle logs all command options correctly
        class TestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""

            @final
            def handle_command(self, *args: Any, **options: Any) -> None:
                """Required implementation."""

        command = TestCommand()

        # Test options to log
        test_options = {
            "dry_run": True,
            "size": 100,
            "config": "test.json",
            "quiet": False,
        }

        # Clear any previous log records
        caplog.clear()

        # Call _handle directly
        command._handle(**test_options)  # noqa: SLF001

        # Verify that logger.info was called for each option
        expected_calls = len(test_options)
        assert_with_msg(
            len(caplog.records) == expected_calls,
            f"Expected {expected_calls} log records, got {len(caplog.records)}",
        )

        # Verify the log message format
        for record in caplog.records:
            assert_with_msg(
                "Command" in record.message and "runs with option" in record.message,
                f"Expected log message format, got {record.message}",
            )

        # Test that the method is final
        assert_with_msg(
            hasattr(ABCBaseCommand._handle, "__final__"),  # noqa: SLF001
            "Expected _handle to be marked as final",
        )

    def test_handle_command(self) -> None:
        """Test method for handle_command."""
        # Test that handle_command is abstract and must be implemented
        abstract_methods: set[str] = getattr(
            ABCBaseCommand, "__abstractmethods__", set()
        )
        assert_with_msg(
            "handle_command" in abstract_methods,
            "Expected handle_command to be abstract",
        )

        # Test that concrete implementation works correctly
        class ConcreteTestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""

            @final
            def handle_command(self, *_args: Any, **_options: Any) -> None:
                """Required implementation that will be logged."""
                self.handle_command_called = True

        command = ConcreteTestCommand()

        # Verify that the method exists and can be called
        assert_with_msg(
            hasattr(command, "handle_command"),
            "Expected command to have handle_command method",
        )

        # Test that the method can be called without errors
        command.handle()
        assert_with_msg(
            command.handle_command_called,
            "Expected handle_command to be called by handle",
        )
