# database.py
"""Functions that are accessing and modifying the database."""

from typing import List, Dict
import mysql.connector
import os

from src.scraper.logger import get_logger

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
    conn = cur = None

    try:
        conn = mysql.connector.connect(
            host=HOST,
            user=USER,
            password=PSW,
            database=db
        )

        cur = conn.cursor()
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: connect_to_db: "
            "Exception was raised when trying to establish a connection to mysql."
        )
    finally:
        return conn, cur


def close_db_connection(conn, cur) -> bool:
    """
    Close the database connection and the database cursor.

    Arguments:
        conn -- pymysql database connection object
        cur -- database cursor object
    """
    res = False

    try:
        cur.close()
        conn.close()
        res = True
    except:
        my_logger.error(
            "database: close_db_connection: "
            "Exception was raised when trying to close the connection/cursor."
        )
    finally:
        return res


def create_db(db) -> bool:
    """ Create database """
    conn, cur = connect_to_db()
    res = False

    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db};")
        res = True
    except:
        my_logger.error(
            f"database: create_db: "
            f"Exception was raised when trying to create database {db}."
        )
    finally:
        close_db_connection(conn, cur)
        return res


def create_info_table() -> bool:
    """
    Create a database table to hold general information about a player.
    This db table is created separately from the stats tables because
    this information is not in a html table like the other stats.
    """

    conn, cur = connect_to_db(db=DB)
    res = False

    try:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS "
            "info (id VARCHAR(8) NOT NULL, "
            "created TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "name VARCHAR(50), "
            "height INT, "
            "weight INT, "
            "dob VARCHAR(50), "
            "countryob VARCHAR(50), "
            "club VARCHAR(50), "
            "age INT, "
            "PRIMARY KEY(id));"
        )
        res = True
    except:
        my_logger.error(
            "database: create_info_table: Exception was raised when trying to create a table."
        )
    finally:
        close_db_connection(conn, cur)
        return res


def drop_info_table() -> bool:
    conn, cur = connect_to_db(db=DB)
    res = False

    try:
        cur.execute("DROP TABLE IF EXISTS info;")
        res = True
    except:
        my_logger.error(
            f"database: drop_info_table: "
            f"Exception was raised when trying to drop table info."
        )
    finally:
        close_db_connection(conn, cur)
        return res


# def add_int_column_info_table(column) -> bool:
#     # Add column with int type
#     res = False
#     conn, cur = connect_to_db(db=DB)
#
#     try:
#         cur.execute(f"ALTER TABLE info ADD COLUMN IF NOT EXISTS {column} INT;")
#         res = True
#     except Exception as e:
#         my_logger.error(e)
#         my_logger.error(
#             f"database: create_info_table: "
#             f"Exception was raised when trying to add int column {column}."
#         )
#     finally:
#         close_db_connection(conn, cur)
#         return res
#
#
# def add_string_column_info_table(column) -> None:
#     # Add column with string type
#     res = False
#     conn, cur = connect_to_db(db=DB)
#
#     try:
#         cur.execute(f"ALTER TABLE info ADD COLUMN IF NOT EXISTS {column} VARCHAR(50);")
#         res = True
#     except:
#         my_logger.error(
#             f"database: create_info_table: "
#             f"Exception was raised when trying to add string column {column}."
#         )
#     finally:
#         close_db_connection(conn, cur)
#         return res
#
#
# def add_info_columns() -> None:
#     HEADER = {
#         "name": "",
#         "height": 0,
#         "weight": 0,
#         "dob": "",
#         "cityob": "",
#         "countryob": "",
#         "club": "",
#         "age": 0,
#     }
#
#     # Add columns
#     for col in HEADER:
#
#         # Add columns with int type
#         if isinstance(HEADER[col], int):
#             add_int_column_info_table(col)
#
#         # Add columns with string type
#         else:
#             add_string_column_info_table(col)


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


def select_info(player_id: str):
    conn, cur = connect_to_db(db=DB)
    res = None

    try:
        cur.execute("SELECT * FROM info WHERE id = '( %s )';" % player_id)

        res = cur.fetchall()
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: select_info: "
            f"Exception was raised when trying to select from info where id = {player_id}."
        )
    finally:
        close_db_connection(conn, cur)

    return res


def add_info(info: Dict) -> bool:
    """
    Inserts the general information about a player (name, age, position, etc.) into
    the info table.

    Arguments:
        info -- A dictionary with column names as keys and player information as values.
             -- for example {'name':'Thibaut Courtois', 'position':'GK', ..., 'age':29}
    """
    # Add data into the info table
    conn, cur = connect_to_db(db=DB)
    res = True

    try:
        placeholders = ', '.join(['%s'] * len(info))
        columns = ', '.join(info.keys())
        sql = "REPLACE INTO info ( %s ) VALUES ( %s );" % (columns, placeholders)

        cur.execute(sql, list(info.values()))
        conn.commit()
    except:
        res = False
        my_logger.error(
            "database: add_info: "
            "Exception was raised when trying to insert primary key (id)."
        )
    finally:
        close_db_connection(conn, cur)

    return res


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
