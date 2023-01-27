from pathlib import Path
import sqlite3
from string import Template

from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import dotenv_values

from sqlite_to_postgres.psql_saver import psql_conn_context
from sqlite_to_postgres.sqlite_extractor import (
    SQLiteExtractor,
    sqlite_conn_context,
)

BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB = f"{BASE_DIR}/db.sqlite"
PSQL_ENV = dotenv_values(f"{BASE_DIR}/env_files/postgres.env")

DATABASE = {
    "dbname": PSQL_ENV["POSTGRES_DB"],
    "user": PSQL_ENV["POSTGRES_USER"],
    "password": PSQL_ENV["POSTGRES_PASSWORD"],
    "host": PSQL_ENV["POSTGRES_HOST"],
    "port": PSQL_ENV["POSTGRES_PORT"],
}

tables = (
    "genre",
    "person",
    "film_work",
    "person_film_work",
    "genre_film_work",
)


def get_pg_table(table_name):
    return f"content.{table_name}"


def get_query(table, is_pg=0):
    sqlite_queries = {
        "genre": ("SELECT id, name, description FROM genre;"),
        "person": "SELECT id, full_name FROM person",
        "film_work": (
            "SELECT id, title, description, creation_date, rating, type FROM film_work;"
        ),
        "person_film_work": (
            "SELECT id, film_work_id, person_id, role FROM person_film_work"
        ),
        "genre_film_work": ("SELECT id, film_work_id, genre_id FROM genre_film_work"),
    }

    psql_queries = {
        "genre": ("SELECT id, name, description FROM content.genre;"),
        "person": "SELECT id, full_name FROM content.person",
        "film_work": (
            "SELECT id, title, description, creation_date, rating, type FROM content.film_work;"
        ),
        "person_film_work": (
            "SELECT id, film_work_id, person_id, role FROM content.person_film_work"
        ),
        "genre_film_work": (
            "SELECT id, film_work_id, genre_id FROM content.genre_film_work"
        ),
    }

    if is_pg:
        return psql_queries[table]
    return sqlite_queries[table]


def test_integrity(sqlite_curs, pg_curs):
    """Check number of returned rows between each pair of tables."""

    query = Template("SELECT COUNT(*) AS count FROM $table")
    for table in tables:
        pg_table = get_pg_table(table)
        sqlite_curs.execute(query.substitute(table=table))
        pg_curs.execute(query.substitute(table=pg_table))
        sqlite_count = sqlite_curs.fetchone()[0]
        pg_count = pg_curs.fetchone()[0]
        assert sqlite_count == pg_count, (
            f"Number of lines in SQLite ({sqlite_count}) is not equal to "
            f"number of lines in PostgreSQL ({pg_count}) for table ({table})"
        )


def test_tables_values(sqlite_curs, pg_curs):
    """Check table rows values from SQLite are equal ones in PostgreSQL."""

    for table in tables:
        sqlite_curs.execute(get_query(table))
        pg_curs.execute(get_query(table, 1))
        sqlite_rows = map(SQLiteExtractor.transform, sqlite_curs.fetchall())
        pg_rows = map(SQLiteExtractor.transform, pg_curs.fetchall())
        for sqlite_row, pg_row in zip(sqlite_rows, pg_rows):
            for sqlite_item, pg_item in zip(sqlite_row.items(), pg_row.items()):
                assert sqlite_item[1] == pg_item[1], (
                    f"In table ({table}) value of column ({sqlite_item[0]}) "
                    f"in SQLite ({sqlite_item[1]}) does not equal to the value "
                    f"in PostgreSQL ({pg_item[1]})"
                )


def check_consistency(connection: sqlite3.Connection, pg_conn: _connection):
    sqlite_curs = connection.cursor()
    pg_curs = pg_conn.cursor()

    test_integrity(sqlite_curs, pg_curs)
    test_tables_values(sqlite_curs, pg_curs)

    sqlite_curs.close()
    pg_curs.close()


if __name__ == "__main__":
    with sqlite_conn_context(
        SQLITE_DB, read_only=True
    ) as sqlite_conn, psql_conn_context(DATABASE, cursor_factory=DictCursor) as pg_conn:
        check_consistency(sqlite_conn, pg_conn)
