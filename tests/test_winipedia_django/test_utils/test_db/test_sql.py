"""Test module for sql.py."""

from winipedia_utils.utils.testing.assertions import assert_with_msg

from winipedia_django.utils.db.sql import execute_sql


def test_execute_sql() -> None:
    """Test func for execute_sql."""
    # Test basic SQL execution without parameters
    sql = "SELECT 1 as test_column"
    columns, rows = execute_sql(sql)

    # Test that we get expected column names and results
    assert_with_msg(
        columns == ["test_column"],
        f"Expected columns ['test_column'], got {columns}",
    )
    assert_with_msg(
        len(rows) == 1,
        f"Expected 1 row, got {len(rows)}",
    )
    assert_with_msg(
        rows[0] == (1,),
        f"Expected row (1,), got {rows[0]}",
    )

    # Test SQL with multiple columns and rows
    multi_sql = "SELECT 1 as col1, 'test' as col2 UNION SELECT 2, 'test2'"
    multi_columns, multi_rows = execute_sql(multi_sql)

    assert_with_msg(
        multi_columns == ["col1", "col2"],
        f"Expected columns ['col1', 'col2'], got {multi_columns}",
    )
    assert_with_msg(
        len(multi_rows) == 2,  # noqa: PLR2004
        f"Expected 2 rows, got {len(multi_rows)}",
    )

    # Test SQL with parameters (using SQLite parameter style)
    param_sql = "SELECT %(test_value)s as param_value"
    param_columns, param_rows = execute_sql(param_sql, {"test_value": "test_param"})

    assert_with_msg(
        param_columns == ["param_value"],
        f"Expected columns ['param_value'], got {param_columns}",
    )
    assert_with_msg(
        len(param_rows) == 1,
        f"Expected 1 row, got {len(param_rows)}",
    )
    assert_with_msg(
        param_rows[0] == ("test_param",),
        f"Expected row ('test_param',), got {param_rows[0]}",
    )

    # Test with empty result set
    empty_sql = "SELECT 1 as empty_col WHERE 1=0"
    empty_columns, empty_rows = execute_sql(empty_sql)

    assert_with_msg(
        empty_columns == ["empty_col"],
        f"Expected columns ['empty_col'], got {empty_columns}",
    )
    assert_with_msg(
        len(empty_rows) == 0,
        f"Expected 0 rows, got {len(empty_rows)}",
    )

    # Test that function returns tuple
    result = execute_sql("SELECT 1")
    assert_with_msg(
        type(result) is tuple,
        f"Expected result to be tuple, got {type(result)}",
    )
    assert_with_msg(
        len(result) == 2,  # noqa: PLR2004
        f"Expected tuple of length 2, got {len(result)}",
    )
