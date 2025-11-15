"""Contains an simple test for cli."""

from winipedia_utils.utils.os.os import run_subprocess
from winipedia_utils.utils.testing.assertions import assert_with_msg


def test_main() -> None:
    """Test for the main cli entrypoint."""
    result = run_subprocess(["poetry", "run", "winipedia-utils", "--help"])
    assert_with_msg(
        result.returncode == 0,
        "Expected returncode 0",
    )
