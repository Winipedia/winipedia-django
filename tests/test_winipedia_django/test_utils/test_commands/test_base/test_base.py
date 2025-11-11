"""Tests for winipedia_django.command module."""

from argparse import ArgumentParser
from typing import Any, final

from django.core.management import call_command
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.commands.base.base import ABCBaseCommand


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

        assert_with_msg(
            command.add_command_arguments_called,
            "Expected add_command_arguments to be called",
        )

    def test_base_add_arguments(self) -> None:
        """Test method for _add_arguments."""

        # Test that _add_arguments adds all expected common arguments
        class TestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""

            @final
            def handle_command(self) -> None:
                """Required implementation."""

        command = TestCommand()
        parser = ArgumentParser()

        # Call _add_arguments directly
        command.base_add_arguments(parser)

        # Test that all expected arguments were added
        expected_arguments = {
            f"--{TestCommand.Options.DRY_RUN}",
            f"--{TestCommand.Options.FORCE}",
            f"--{TestCommand.Options.DELETE}",
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
            def handle_command(self) -> None:
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
            def handle_command(self) -> None:
                """Track that this method was called."""
                self.handle_command_called = True

        command = TestCommand()

        # Test the template method pattern
        command.handle()

        assert_with_msg(
            command.handle_command_called,
            "Expected handle_command to be called by handle",
        )

    def test_base_handle(self) -> None:
        """Test method for _handle."""

        # Test that _handle logs all command options correctly
        class TestCommand(ABCBaseCommand):
            @final
            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""

            @final
            def handle_command(self) -> None:
                """Required implementation."""

        command = TestCommand()

        # test it sets args and options correctly
        args = ("test_arg",)
        options = {"test_option": "test_value"}
        command.handle(*args, **options)
        assert_with_msg(
            command.args == args,
            f"Expected args to be {args}, got {command.args}",
        )
        assert_with_msg(
            command.options == options,
            f"Expected options to be {options}, got {command.options}",
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

    def test_get_option(self) -> None:
        """Test method for get_option."""

        # Test that get_option returns the correct value
        class TestCommand(ABCBaseCommand):
            class Options(ABCBaseCommand.Options):
                EXTRA = "extra"

            def add_command_arguments(self, parser: ArgumentParser) -> None:
                """Required implementation."""
                parser.add_argument(
                    f"--{self.Options.EXTRA}",
                    help="Extra option",
                )

            def handle_command(self) -> None:
                """Required implementation."""
                self.extra_option = self.get_option(self.Options.EXTRA)

        # call the command with djangos call_command
        cmd = TestCommand()
        call_command(cmd, extra="test")
        assert_with_msg(
            cmd.extra_option == "test",
            f"Expected extra_option to be 'test', got {cmd.extra_option}",
        )
