import os
from unittest import TestCase
from dotenv import load_dotenv

load_dotenv(".env.test")

import src.scraper.database as db

# Test DB
DB = os.getenv("DATABASE")

# Config vars
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

player_stats = [{'table': 'standard', 'id': '0d9b2d31', 'season': '2007-2008', 'age': '17', 'team': 'Bayern Munich',
                 'country': 'de GER', 'comp_level': '1. Bundesliga', 'lg_finish': '1st', 'games': '12',
                 'games_starts': '3', 'minutes': '361', 'minutes_90s': '4.0', 'goals': '0', 'assists': '0',
                 'goals_pens': '0', 'pens_made': '0', 'pens_att': '0', 'cards_yellow': '0', 'cards_red': '0',
                 'goals_per90': '0.00', 'assists_per90': '0.00', 'goals_assists_per90': '0.00',
                 'goals_pens_per90': '0.00', 'goals_assists_pens_per90': '0.00'}]


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

    def test_add_info(self):
        db.create_info_table()

        self.assertTrue(db.add_info(player_info))

    def test_select_info(self):
        db.create_info_table()

        self.assertIsNotNone(db.select_info(player_info["id"]))

    def test_select_info_all(self):
        db.create_info_table()

        self.assertIsNotNone(db.select_info_all())

    def test_drop_info_table(self):
        db.create_info_table()

        self.assertTrue(db.drop_info_table())


class TestStats(TestCase):
    def test_create_stats_tables(self):
        # Create Info table, which is referenced by other tables
        db.create_info_table()

        self.assertTrue(db.create_stats_tables(player_tables))

    def test_drop_stats_table(self):
        self.assertTrue(db.drop_stats_table("standard"))

    def test_add_stats(self):
        # Create Info table and add info
        db.create_info_table()
        db.add_info(player_info)

        # Create Standard table
        db.create_stats_tables(player_tables)

        # Add stats for player
        self.assertTrue(db.add_stats(player_stats))

    def test_select_stats(self):
        # Create Info table and add info
        db.create_info_table()
        db.add_info(player_info)

        # Create Standard table
        db.create_stats_tables(player_tables)

        res = db.select_stats(player_info["id"], player_stats[0]["table"])

        self.assertIsNotNone(res)

    def test_select_stats_all(self):
        # Create Info table and add info
        db.create_info_table()
        db.add_info(player_info)

        # Create Standard table
        db.create_stats_tables(player_tables)

        self.assertIsNotNone(db.select_stats_all(player_stats[0]["table"]))

