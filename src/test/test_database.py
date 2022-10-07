import os
from unittest import TestCase

from src.scraper.database import connect_to_db, \
    close_db_connection, \
    create_db

# SoccerStats DB
DB = os.getenv("DATABASE")


class TestConnection(TestCase):
    def test_connect_to_db(self):
        # Default DB
        conn, cur = connect_to_db()

        self.assertIsNotNone(conn)
        self.assertIsNotNone(cur)

    def test_connect_to_soccer_stats_db(self):
        conn, cur = connect_to_db(db=DB)

        self.assertIsNotNone(conn)
        self.assertIsNotNone(cur)

    def test_connect_to_wrong_db(self):
        # Wrong DB
        self.assertRaises(TypeError, connect_to_db(db="test"))

    def test_close_db_connection(self):
        conn, cur = connect_to_db()

        self.assertTrue(close_db_connection(conn, cur))

    def test_create_db(self):
        self.assertTrue(create_db())
        self.assertTrue(create_db(DB))


class TestInfo(TestCase):
    def test_create_info_table(self):
        self.fail()

    def test_drop_info_table(self):
        self.fail()

    def test_add_int_column_info_table(self):
        self.fail()

    def test_add_string_column_info_table(self):
        self.fail()

    def test_add_info_columns(self):
        self.fail()

    def test_add_info(self):
        self.fail()


class TestStats(TestCase):
    def test_drop_stats_table(self):
        self.fail()

    def test_drop_stats_tables(self):
        self.fail()

    def test_add_stats_columns(self):
        self.fail()

    def test_add_stats_columns_for_each_table(self):
        self.fail()

    def test_create_stats_tables(self):
        self.fail()

    def test_add_stats(self):
        self.fail()
