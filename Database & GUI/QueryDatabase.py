import argparse
import os

import psycopg2
from psycopg2.extras import RealDictCursor


DEFAULT_HOST = os.getenv("PGHOST", "localhost")
DEFAULT_PORT = int(os.getenv("PGPORT", "5432"))
DEFAULT_USER = os.getenv("PGUSER", "postgres")
DEFAULT_PASSWORD = os.getenv("PGPASSWORD", "123sega")


def run_query(
    database_name: str,
    query: str,
    host: str,
    port: int,
    user: str,
    password: str,
) -> list[dict]:
    """Execute a SQL query and return the rows."""
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=database_name,
        user=user,
        password=password,
    )

    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)

            if cursor.description is None:
                connection.commit()
                return []

            return [dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a PostgreSQL query against a selected database."
    )
    parser.add_argument("database_name", help="PostgreSQL database name")
    parser.add_argument(
        "--query",
        default="SELECT current_database() AS database_name;",
        help="SQL query to execute",
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="PostgreSQL port")
    parser.add_argument("--user", default=DEFAULT_USER, help="PostgreSQL user")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="PostgreSQL password")
    args = parser.parse_args()

    rows = run_query(
        database_name=args.database_name,
        query=args.query,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    if not rows:
        print("Query executed successfully.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
