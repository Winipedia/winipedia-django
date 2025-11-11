"""Test module for sql.py."""

import pytest
from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.db.sql import execute_sql


@pytest.mark.django_db
def test_execute_sql() -> None:
    """Test func for execute_sql."""
    # create an empty table to test on
    execute_sql("CREATE TABLE test_execute_sql (id INTEGER PRIMARY KEY, name TEXT)")
    # Test basic SQL execution without parameters
    sql = "SELECT * FROM test_execute_sql"
    columns, rows = execute_sql(sql)

    assert_with_msg(
        columns == ["id", "name"],
        f"Expected columns ['id', 'name'], got {columns}",
    )
    assert_with_msg(
        len(rows) == 0,
        f"Expected 0 rows, got {len(rows)}",
    )
