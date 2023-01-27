import logging
import sqlite3
import sys
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from dotenv import dotenv_values

from sqlite_to_postgres.models import (
    FilmWork,
    Genre,
    GenreFilmWork,
    Person,
    PersonFilmWork,
)
from sqlite_to_postgres.psql_saver import PostgresSaver, psql_conn_context
from sqlite_to_postgres.sqlite_extractor import SQLiteExtractor, sqlite_conn_context


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


def setup_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s, [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Main method of transfering data from SQLite to PostgreSQL"""

    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)
    tables = (Genre, Person, FilmWork, PersonFilmWork, GenreFilmWork)

    def load_and_save(table):
        with postgres_saver.prepare_insert_context(table):
            for batch_rows in sqlite_extractor.extract(table):
                postgres_saver.save(batch_rows, table)

    for table in tables:
        load_and_save(table)


if __name__ == "__main__":
    logger = setup_logger()

    with sqlite_conn_context(
        SQLITE_DB, read_only=True
    ) as sqlite_conn, psql_conn_context(
        DATABASE, cursor_factory=DictCursor
    ) as pg_conn:
        try:
            load_from_sqlite(sqlite_conn, pg_conn)
        except psycopg2.Error:
            logger.exception("Error in PostgreSQL during data import.")
        except sqlite3.Error:
            logger.exception("Error in SQLite during data export.")
        except Exception:
            logger.exception("Script internal error.")
