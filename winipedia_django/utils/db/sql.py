"""Module for database operations with sql."""

from typing import Any

from django.db import connection


def execute_sql(
    sql: str, params: dict[str, Any] | None = None
) -> tuple[list[str], list[Any]]:
    """Execute raw SQL query and return column names with results.

    Executes a raw SQL query using Django's database connection and returns
    both the column names and the result rows. This provides a convenient
    way to run custom SQL queries while maintaining Django's database
    connection management and parameter binding for security.

    The function automatically handles cursor management and ensures proper
    cleanup of database resources. Parameters are safely bound to prevent
    SQL injection attacks.

    Args:
        sql (str): The SQL query string to execute. Can contain parameter
            placeholders that will be safely bound using the params argument.
        params (dict[str, Any] | None, optional): Dictionary of parameters
            to bind to the SQL query for safe parameter substitution.
            Defaults to None if no parameters are needed.

    Returns:
        tuple[list[str], list[Any]]: A tuple containing:
            - list[str]: Column names from the query result
            - list[Any]: List of result rows, where each row is a tuple
              of values corresponding to the column names
        Empty list if no results are returned

    Raises:
        django.db.Error: If there's a database error during query execution
        django.db.ProgrammingError: If the SQL syntax is invalid
        django.db.IntegrityError: If the query violates database constraints

    Example:
        >>> sql = "SELECT id, username FROM auth_user WHERE is_active = %(active)s"
        >>> params = {"active": True}
        >>> columns, rows = execute_sql(sql, params)
        >>> columns
        ['id', 'username']
        >>> rows[0]
        (1, 'admin')

    Note:
        - Uses Django's default database connection
        - Automatically manages cursor lifecycle
        - Parameters are safely bound to prevent SQL injection
        - Returns all results in memory - use with caution for large datasets
    """
    with connection.cursor() as cursor:
        cursor.execute(sql=sql, params=params)
        rows = cursor.fetchall()
        column_names = (
            [col[0] for col in cursor.description] if cursor.description else []
        )

    return column_names, rows
