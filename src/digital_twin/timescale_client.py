"""
ECDT - Digital Twin
TimescaleDB client.

This module provides the low-level PostgreSQL/TimescaleDB connection
used by the Digital Twin time-series layer.

Responsibilities
----------------
- Load TIMESCALE_URI from the environment.
- Open PostgreSQL connections.
- Execute SQL queries.
- Execute batch insert operations.
- Provide a simple connectivity check.

This module does NOT:
- define the time-series schema;
- ingest telemetry;
- implement business queries.

Those responsibilities belong to:
- timeseries_schema.py
- timeseries_ingestion.py
- timeseries_queries.py
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

from dotenv import load_dotenv

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError as exc:
    raise ImportError(
        "psycopg is required for TimescaleDB support. "
        'Install it with: pip install "psycopg[binary]>=3.2,<4"'
    ) from exc


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TimescaleClientError(RuntimeError):
    """Base exception for TimescaleDB client errors."""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class TimescaleClient:
    """
    Reusable client for ECDT TimescaleDB.

    The connection URI is read from:

        TIMESCALE_URI

    The Docker Compose configuration already provides this variable to the
    backend service.
    """

    def __init__(
        self,
        uri: str | None = None,
    ) -> None:
        self.uri = uri or os.getenv("TIMESCALE_URI")

        if not self.uri:
            raise TimescaleClientError(
                "TIMESCALE_URI is not configured."
            )

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def connect(self) -> psycopg.Connection:
        """
        Open a new PostgreSQL connection.

        Returns:
            Active psycopg connection.
        """

        try:
            return psycopg.connect(
                self.uri,
                row_factory=dict_row,
            )

        except psycopg.Error as exc:
            raise TimescaleClientError(
                "Unable to connect to TimescaleDB."
            ) from exc

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        """
        Provide a managed database connection.

        The connection is automatically closed when leaving the context.
        """

        connection = self.connect()

        try:
            yield connection

        finally:
            connection.close()

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def execute(
        self,
        query: str,
        parameters: Sequence[Any] | None = None,
        *,
        fetch: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Execute one SQL statement.

        Parameters:
            query:
                SQL statement.

            parameters:
                Optional positional parameters.

            fetch:
                If True, return query results.

        Returns:
            List of dictionaries when fetch=True.
        """

        try:
            with self.connection() as connection:

                with connection.cursor() as cursor:
                    cursor.execute(
                        query,
                        parameters,
                    )

                    rows = (
                        cursor.fetchall()
                        if fetch
                        else []
                    )

                connection.commit()

                return list(rows)

        except psycopg.Error as exc:
            raise TimescaleClientError(
                "TimescaleDB query execution failed."
            ) from exc

    def execute_many(
        self,
        query: str,
        parameters: Sequence[Sequence[Any]],
    ) -> int:
        """
        Execute a parameterized SQL statement for multiple rows.

        Returns:
            Number of affected rows reported by psycopg.
        """

        if not parameters:
            return 0

        try:
            with self.connection() as connection:

                with connection.cursor() as cursor:
                    cursor.executemany(
                        query,
                        parameters,
                    )

                    row_count = cursor.rowcount

                connection.commit()

                return row_count

        except psycopg.Error as exc:
            raise TimescaleClientError(
                "TimescaleDB batch execution failed."
            ) from exc

    # ------------------------------------------------------------------
    # Connectivity
    # ------------------------------------------------------------------

    def ping(self) -> bool:
        """
        Check TimescaleDB connectivity.

        Returns:
            True when the database accepts SELECT 1.
        """

        try:
            self.execute("SELECT 1")
            return True

        except TimescaleClientError:
            return False

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "TimescaleClient":
        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        # Connections are managed per operation.
        return None