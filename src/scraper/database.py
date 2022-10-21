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
        conn = mysql.connector.connect(host=HOST, user=USER, password=PSW, database=db)

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
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: close_db_connection: "
            "Exception was raised when trying to close the connection/cursor."
        )
    finally:
        return res


def create_db(db) -> bool:
    """Create database"""
    conn, cur = connect_to_db()
    res = False

    try:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {db};")
        res = True
    except Exception as e:
        my_logger.error(e)
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
    except Exception as e:
        my_logger.error(e)
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
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            f"database: drop_info_table: "
            f"Exception was raised when trying to drop table info."
        )
    finally:
        close_db_connection(conn, cur)

    return res


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


def select_info_all():
    conn, cur = connect_to_db(db=DB)
    res = None

    try:
        cur.execute("SELECT * FROM info;")

        res = cur.fetchall()
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: select_info_all: "
            f"Exception was raised when trying to select all from info."
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
        placeholders = ", ".join(["%s"] * len(info))
        columns = ", ".join(info.keys())
        sql = "REPLACE INTO info ( %s ) VALUES ( %s );" % (columns, placeholders)

        cur.execute(sql, list(info.values()))
        conn.commit()
    except Exception as e:
        res = False
        my_logger.error(e)
        my_logger.error(
            "database: add_info: "
            "Exception was raised when trying to insert primary key (id)."
        )
    finally:
        close_db_connection(conn, cur)

    return res


def create_stats_tables(tables: List[List[str]]) -> bool:
    """
    Create the stats tables if they don't exist.

    Arguments:
        tables -- a list of string lists,
               -- tables[i][0] is the name of the i-th table
               -- tables[i][1:] are the column names for the i-th table
    """
    res = True

    base_column = [
        "season",
        "squad",
        "team",
        "country",
        "comp_level",
        "lg_finish",
    ]

    # Create tables
    for table in tables:
        conn, cur = connect_to_db(db=DB)

        try:
            table_name = table[0]
            columns = table[1:]

            columns = list(set(columns) - set(base_column))

            sql_statement = f"""CREATE TABLE IF NOT EXISTS {table_name}
            (id VARCHAR(8) NOT NULL,
            season VARCHAR(20) NOT NULL,
            country VARCHAR(30),
            comp_level VARCHAR(30),
            lg_finish VARCHAR(10),
            squad VARCHAR(50) NOT NULL, """

            for column in columns:
                sql_statement += f"{column} FLOAT, "

            sql_statement += (
                "PRIMARY KEY(id, season, squad), FOREIGN KEY(id) REFERENCES info(id) "
                "ON DELETE CASCADE ON UPDATE CASCADE);"
            )

            cur.execute(sql_statement)
        except Exception as e:
            res = False

            my_logger.error(e)
            my_logger.error(
                f"database: create_stats_table: "
                f"Exception was raised when trying to create table {table[0]}."
            )
        finally:
            close_db_connection(conn, cur)

        return res


def drop_stats_table(table: str) -> bool:
    conn, cur = connect_to_db(db=DB)
    res = False

    try:
        cur.execute(f"DROP TABLE IF EXISTS {table} ")
        res = True
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            f"database: create_stats_table: "
            f"Exception was raised when trying to drop table {table}."
        )
    finally:
        close_db_connection(conn, cur)

    return res


def drop_stats_tables(tables: List[List[str]]) -> None:
    # Drop tables
    for table in tables:
        drop_stats_table(table[0])


def select_stats(player_id: str, table: str):
    conn, cur = connect_to_db(db=DB)
    res = None

    try:
        cur.execute("SELECT * FROM ( %s ) WHERE id = '( %s )';" % (table, player_id))

        res = cur.fetchall()
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: select_stats: "
            f"Exception was raised when trying to select from {table} where id = {player_id}."
        )
    finally:
        close_db_connection(conn, cur)

    return res


def select_stats_all(table: str) -> None:
    conn, cur = connect_to_db(db=DB)
    res = None

    try:
        cur.execute("SELECT * FROM ( %s );" % table)

        res = cur.fetchall()
    except Exception as e:
        my_logger.error(e)
        my_logger.error(
            "database: select_stats_all: "
            f"Exception was raised when trying to select all from {table}."
        )
    finally:
        close_db_connection(conn, cur)

    return res


def add_stats(stats: List[Dict]) -> bool:
    """
    Insert player performance data into the appropriate table.

    Arguments:
        stats -- list of dictionaries
              -- each dictionary represents a row of a table
              -- (for example playing time for a player in a single season)
    """
    res = True

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

            res = False
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

                res = False
            finally:
                close_db_connection(conn, cur)

    return res
