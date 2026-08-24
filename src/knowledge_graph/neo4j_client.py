"""
ECDT - Neo4j Client

Provides a reusable Neo4j client for the ECDT Knowledge Graph.

Responsibilities:
- Load Neo4j connection settings from environment variables.
- Create and manage the Neo4j Bolt driver.
- Verify connectivity.
- Execute Cypher queries.
- Execute write transactions.
- Close the driver cleanly.

Environment variables:
    NEO4J_URI
    NEO4J_USER
    NEO4J_PASSWORD

Example:
    NEO4J_URI=bolt://localhost:7687
    NEO4J_USER=neo4j
    NEO4J_PASSWORD=your_password
"""

from __future__ import annotations

import os
from typing import Any, Iterable

from neo4j import Driver, GraphDatabase
from dotenv import load_dotenv


# Load .env from the project root.
load_dotenv()


class Neo4jClient:
    """Reusable client for interacting with the ECDT Neo4j database."""

    def __init__(
        self,
        uri: str | None = None,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        """
        Initialize the Neo4j client.

        Parameters:
            uri:
                Neo4j Bolt URI.
                Defaults to NEO4J_URI or bolt://localhost:7687.

            user:
                Neo4j username.
                Defaults to NEO4J_USER or neo4j.

            password:
                Neo4j password.
                Defaults to NEO4J_PASSWORD.
        """

        self.uri = uri or os.getenv(
            "NEO4J_URI",
            "bolt://localhost:7687",
        )

        self.user = user or os.getenv(
            "NEO4J_USER",
            "neo4j",
        )

        self.password = password or os.getenv(
            "NEO4J_PASSWORD",
        )

        if not self.password:
            raise ValueError(
                "NEO4J_PASSWORD is not configured. "
                "Define it in the .env file."
            )

        self.driver: Driver = GraphDatabase.driver(
            self.uri,
            auth=(self.user, self.password),
        )

    def verify_connectivity(self) -> bool:
        """
        Verify that the Neo4j server is reachable.

        Returns:
            True if the connection succeeds.

        Raises:
            Exception if Neo4j cannot be reached.
        """

        self.driver.verify_connectivity()
        return True

    def execute(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a Cypher query and return the result as dictionaries.

        Parameters:
            query:
                Cypher query to execute.

            parameters:
                Optional Cypher parameters.

        Returns:
            List of records represented as dictionaries.
        """

        with self.driver.session() as session:
            result = session.run(
                query,
                parameters or {},
            )

            return [record.data() for record in result]

    def execute_write(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a write query inside a transaction.

        Useful for CREATE / MERGE / SET / DELETE operations.

        Parameters:
            query:
                Cypher write query.

            parameters:
                Optional Cypher parameters.

        Returns:
            List of returned records.
        """

        def _write_transaction(tx):
            result = tx.run(
                query,
                parameters or {},
            )
            return [record.data() for record in result]

        with self.driver.session() as session:
            return session.execute_write(_write_transaction)

    def execute_many(
        self,
        query: str,
        parameters_list: Iterable[dict[str, Any]],
    ) -> int:
        """
        Execute the same Cypher query for multiple parameter sets.

        This will be useful later when importing:
        - services.csv
        - dependencies.csv
        - target_incidents.csv

        Parameters:
            query:
                Cypher query to execute.

            parameters_list:
                Iterable of parameter dictionaries.

        Returns:
            Number of executed operations.
        """

        count = 0

        with self.driver.session() as session:
            with session.begin_transaction() as tx:
                for parameters in parameters_list:
                    tx.run(
                        query,
                        parameters,
                    )
                    count += 1

                tx.commit()

        return count

    def close(self) -> None:
        """Close the Neo4j driver."""

        self.driver.close()

    def __enter__(self) -> "Neo4jClient":
        """Support usage with a context manager."""

        return self

    def __exit__(
        self,
        exc_type,
        exc_value,
        traceback,
    ) -> None:
        """Close the driver when leaving a context manager."""

        self.close()


if __name__ == "__main__":
    """
    Standalone connectivity test.

    Run from the ECDT project root:

        python -m src.knowledge_graph.neo4j_client
    """

    print("=" * 60)
    print("ECDT - Neo4j Connectivity Test")
    print("=" * 60)

    try:
        with Neo4jClient() as client:
            client.verify_connectivity()

            print(f"URI      : {client.uri}")
            print(f"USER     : {client.user}")
            print("STATUS   : Connected")

            result = client.execute(
                "RETURN 'ECDT Neo4j OK' AS status"
            )

            print(f"SERVER   : {result[0]['status']}")

            version_result = client.execute(
                "CALL dbms.components() "
                "YIELD name, versions "
                "RETURN name, versions"
            )

            for record in version_result:
                print(f"COMPONENT: {record['name']}")
                print(f"VERSION  : {record['versions']}")

            print("=" * 60)
            print("Neo4j connection test: SUCCESS")
            print("=" * 60)

    except Exception as exc:
        print("=" * 60)
        print("Neo4j connection test: FAILED")
        print("=" * 60)
        print(f"Error: {exc}")
        raise