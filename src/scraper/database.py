# database.py
"""Functions that are accessing and modifying the database."""

from typing import List, Dict
import mysql.connector
import os
from dotenv import load_dotenv

from src.scraper.logger import get_logger

# Config
load_dotenv()

DB = os.getenv("DATABASE")
HOST = os.getenv("DB_HOST")
USER = os.getenv("DB_USER")
PSW = os.getenv("DB_PSW")

# Logging
my_logger = get_logger(__name__)


def connect_to_db(db=None):
    """
    Create a database connection to a MySQL database

    Returns:
        conn -- MySQL connection object
        cur -- database cursor for the current connection
    """
    try:
        conn = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PSW,
            database=db
        )

        cur = conn.cursor()
        return conn, cur

    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: connect_to_db: "
            "Exception was raised when trying to establish a connection to mysql."
        )


def close_db_connection(conn, cur) -> bool:
    """
    Close the database connection and the database cursor.

    Arguments:
        conn -- pymysql database connection object
        cur -- database cursor object
    """
    try:
        cur.close()
        conn.close()
        return True
    except:
        my_logger.error(
            "database: close_db_connection: "
            "Exception was raised when trying to close the connection/cursor."
        )
        return False


def create_db(db=None) -> bool:
    """ Create database """
    conn, cur = connect_to_db()

    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db};")
        return True
    except:
        my_logger.error(
            f"database: create_db: "
            f"Exception was raised when trying to create database {db}."
        )
        return False
    finally:
        close_db_connection(conn, cur)


def drop_info_table() -> None:
    conn, cur = connect_to_db(db=DB)

    try:
        cur.execute("DROP TABLE IF EXISTS info;")
    except:
        my_logger.error(
            f"database: create_info_table: "
            f"Exception was raised when trying to drop table info."
        )
    finally:
        close_db_connection(conn, cur)


def add_int_column_info_table(column) -> None:
    # Add column with int type
    if isinstance(column, int):
        conn, cur = connect_to_db(db=DB)

        try:
            cur.execute(f"ALTER TABLE info ADD COLUMN IF NOT EXISTS {column} INT;")
        except Exception as e:
            my_logger.error(e)
            my_logger.error(
                f"database: create_info_table: "
                f"Exception was raised when trying to add int column {column}."
            )
        finally:
            close_db_connection(conn, cur)


def add_string_column_info_table(column) -> None:
    # Add column with string type
    conn, cur = connect_to_db(db=DB)

    try:
        cur.execute(f"ALTER TABLE info ADD COLUMN IF NOT EXISTS {column} VARCHAR(50);")
    except:
        my_logger.error(
            f"database: create_info_table: "
            f"Exception was raised when trying to add string column {column}."
        )
    finally:
        close_db_connection(conn, cur)


def create_info_table() -> None:
    """
    Create a database table to hold general information about a player.
    This db table is created separately from the stats tables because
    this information is not in a html table like the other stats.
    """

    conn, cur = connect_to_db(db=DB)

    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS "
            "info (id VARCHAR(8) NOT NULL, "
            "created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "PRIMARY KEY(id));"
        )
    except:
        my_logger.error(
            "database: create_info_table: Exception was raised when trying to create a table."
        )
    finally:
        close_db_connection(conn, cur)


def add_info_columns() -> None:
    HEADER = {
        "name": "",
        "height": 0,
        "weight": 0,
        "dob": "",
        "cityob": "",
        "countryob": "",
        "club": "",
        "age": 0,
    }

    # Add columns
    for col in HEADER:

        # Add columns with int type
        if isinstance(HEADER[col], int):
            add_int_column_info_table(col)

        # Add columns with string type
        else:
            add_string_column_info_table(col)


def drop_stats_table(table: str) -> None:
    conn, cur = connect_to_db(db=DB)

    try:
        cur.execute(f"DROP TABLE IF EXISTS {table} ")
    except:
        my_logger.error(
            f"database: create_stats_table: "
            f"Exception was raised when trying to drop table {table}."
        )
    finally:
        close_db_connection(conn, cur)


def drop_stats_tables(tables: List[List[str]]) -> None:
    # Drop tables
    for table in tables:
        drop_stats_table(table[0])


def add_stats_columns(table: List[str]) -> None:
    # Add columns
    for index, column in enumerate(table):

        # Don't create columns for table name and other string columns
        if index == 0 or column in [
            "season",
            "squad",
            "team",
            "country",
            "comp_level",
            "lg_finish",
        ]:
            continue

        conn, cur = connect_to_db(db=DB)

        try:
            cur.execute(f"ALTER TABLE {table[0]} ADD COLUMN {column} FLOAT;")
        except:
            my_logger.error(
                f"database: create_stats_tables: "
                f"Exception was raised when trying to add a column {column}."
            )
        finally:
            close_db_connection(conn, cur)


def add_stats_columns_for_each_table(tables: List[List[str]]) -> None:
    # Add columns for each table
    for table in tables:
        add_stats_columns(table)


def create_stats_tables(tables: List[List[str]]) -> None:
    """
    Create the stats tables if they don't exist.

    Arguments:
        tables -- a list of string lists,
               -- tables[i][0] is the name of the i-th table
               -- tables[i][1:] are the column names for the i-th table
    """

    # Create tables
    for table in tables:
        conn, cur = connect_to_db(db=DB)

        try:
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {table[0]} "
                f"(id VARCHAR(8) NOT NULL, season VARCHAR(20) NOT NULL, country VARCHAR(30), comp_level VARCHAR(30), "
                f"lg_finish VARCHAR(10), squad VARCHAR(50) NOT NULL, PRIMARY KEY(id, season, squad), "
                f"FOREIGN KEY(id) REFERENCES info(id));"
            )
        except:
            my_logger.error(
                f"database: create_stats_table: "
                f"Exception was raised when trying to create table {table[0]}."
            )
        finally:
            close_db_connection(conn, cur)


def add_info(info: Dict) -> None:
    """
    Inserts the general information about a player (name, age, position, etc.) into
    the info table.

    Arguments:
        info -- A dictionary with column names as keys and player information as values.
             -- for example {'name':'Thibaut Courtois', 'position':'GK', ..., 'age':29}
    """
    # Add data into the info table
    for key in info:

        # Insert primary key
        if key == "id":
            continue

            # conn, cur = connect_to_db(db=DB)
            #
            # try:
            #     cur.execute(f"REPLACE INTO info ({key}) "
            #                 f"VALUES ('{info[key]}');")
            #     conn.commit()
            # except:
            #     my_logger.error(
            #         "database: add_info: "
            #         "Exception was raised when trying to insert primary key (id)."
            #     )
            # finally:
            #     close_db_connection(conn, cur)

        # Insert data
        else:
            conn, cur = connect_to_db(db=DB)

            try:
                cur.execute(
                    f"UPDATE info "
                    f'SET {key} = "{info[key]}"'
                    f'WHERE id = "{info["id"]}";'
                )
                conn.commit()
            except:
                my_logger.error(key)

                my_logger.error(info["id"])

                if key in info:
                    my_logger.error(info[key])
                else:
                    my_logger.error(f"{key} not in {info}")

                my_logger.error(
                    "database: add_info: Exception was raised when trying to update a column."
                )
            finally:
                close_db_connection(conn, cur)


def add_stats(stats: List[Dict]) -> None:
    """
    Insert player performance data into the appropriate table.

    Arguments:
        stats -- list of dictionaries
              -- each dictionary represents a row of a table
              -- (for example playing time for a player in a single season)
    """

    # Iterate over the dictionaries each of which represents one row of a table
    for row in stats:
        conn, cur = connect_to_db(db=DB)

        # Insert the string attributes from a table row
        try:
            statement = f"""REPLACE INTO {row['table']} (id, season, squad, country, comp_level, lg_finish) \
                        VALUES ('{row['id']}', '{row['season']}', "{row['team']}", '{row['country'].split()[1]}', \
                         '{row['comp_level']}', '{row['lg_finish']}');"""

            cur.execute(statement)
            conn.commit()
        except Exception as e:
            my_logger.error(e)

            my_logger.error(
                "database: add_stats: "
                "Exception was raised when trying to insert string columns."
            )
        finally:
            close_db_connection(conn, cur)

        # Iterate over the dict keys each of which represents a table's columns
        for column in row:

            # Skip the table name and string columns (already inserted)
            if column in [
                "table",
                "id",
                "season",
                "team",
                "country",
                "comp_level",
                "lg_finish",
            ]:
                continue

            conn, cur = connect_to_db(db=DB)

            # Insert data into appropriate columns
            try:
                team = row["team"].replace("'", "\\'")
                team = team.replace('"', '\\"')

                statement = f"""UPDATE {row['table']} SET {column} = {float(row[column].replace(',', ''))}
                            WHERE id = '{row['id']}' AND season = '{row['season']}' AND squad = "{team}";"""

                cur.execute(statement)
                conn.commit()
            except Exception as e:
                my_logger.error(e)

                my_logger.error(
                    f"database: add_stats: "
                    f"Exception was raised when trying to update column {column} for player {row['id']}."
                )
            finally:
                close_db_connection(conn, cur)
