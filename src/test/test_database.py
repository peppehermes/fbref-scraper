import os
from unittest import TestCase
from dotenv import load_dotenv

load_dotenv(".env.test")

import src.scraper.database as db

# Test DB
DB = os.getenv("DATABASE")


class TestConnection(TestCase):
    def test_connect_to_db(self):
        # Default DB
        conn, cur = db.connect_to_db()

        self.assertIsNotNone(conn)
        self.assertIsNotNone(cur)

    def test_close_db_connection(self):
        conn, cur = db.connect_to_db()

        self.assertTrue(db.close_db_connection(conn, cur))

    def test_create_db(self):
        self.assertTrue(db.create_db(DB))

    def test_connect_to_wrong_db(self):
        # Wrong DB
        self.assertRaises(TypeError, db.connect_to_db(db="wrong_db"))

    def test_connect_to_test_schema(self):
        db.create_db(DB)

        conn, cur = db.connect_to_db(db=DB)

        self.assertIsNotNone(conn)
        self.assertIsNotNone(cur)


class TestInfo(TestCase):
    def test_create_info_table(self):
        self.assertTrue(db.create_info_table())

    def test_drop_info_table(self):
        self.assertTrue(db.drop_info_table())

    def test_add_info(self):
        player_info = {
            "id": "0d9b2d31",
            "name": "Pedri",
            "height": 174,
            "weight": 63,
            "dob": "2002-11-25",
            "countryob": "Spain",
            "club": "Barcelona",
            "age": 19,
        }

        db.create_info_table()
        self.assertTrue(db.add_info(player_info))

    def test_select_info(self):
        player_id = "0d9b2d31"

        self.assertIsNotNone(db.select_info(player_id))


class TestStats(TestCase):
    def test_create_stats_tables(self):
        player_tables = [
            [
                "standard",
                "age",
                "team",
                "country",
                "comp_level",
                "lg_finish",
                "games",
                "games_starts",
                "minutes",
                "minutes_90s",
                "goals",
                "assists",
                "goals_pens",
                "pens_made",
                "pens_att",
                "cards_yellow",
                "cards_red",
                "goals_per90",
                "assists_per90",
                "goals_assists_per90",
                "goals_pens_per90",
                "goals_assists_pens_per90",
                "xg",
                "npxg",
                "xa",
                "npxg_xa",
                "xg_per90",
                "xa_per90",
                "xg_xa_per90",
                "npxg_per90",
                "npxg_xa_per90",
            ]
        ]

        self.assertTrue(db.create_stats_tables(player_tables))

    def test_drop_stats_table(self):
        self.fail()

    def test_drop_stats_tables(self):
        self.fail()

    def test_add_stats(self):
        self.fail()
