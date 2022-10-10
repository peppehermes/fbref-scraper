# crawler.py
"""Driver program. Iterates over Leagues, Squads, and Players
 and stores their information into a database."""
import os
import time
from multiprocessing import Pool
from typing import List
from dotenv import load_dotenv

# Config
load_dotenv()

import database as db
from src.scraper.logger import get_logger
from requests import get_players, get_squads
from player_info import scrape_info
from player_stats import get_stats_headers, scrape_stats

my_logger = get_logger(__name__)

# List of leagues to crawl
LEAGUES = [
    "/en/comps/12/La-Liga-Stats",
    "/en/comps/13/Ligue-1-Stats",
    "/en/comps/9/Premier-League-Stats",
    "/en/comps/20/Bundesliga-Stats",
    "/en/comps/11/Serie-A-Stats",
]

# List of tables to collect per player
TABLES = [
    "stats_standard_dom_lg",
    # "stats_shooting_dom_lg",
    # "stats_passing_dom_lg",
    # "stats_passing_types_dom_lg",
    # "stats_gca_dom_lg",
    # "stats_defense_dom_lg",
    # "stats_possession_dom_lg",
    # "stats_playing_time_dom_lg",
    # "stats_misc_dom_lg",
    # "stats_keeper_dom_lg",
    # "stats_keeper_adv_dom_lg",
]


def scrape(player: str) -> None:
    """
    Function to be run by a process from the process pool.
    Scrapes and stores a single players' data.

    Arguments:
        player -- Unique player url path.
    """

    # time.sleep(0.5)
    player_start = time.time()

    player_info = scrape_info(player)
    my_logger.debug(f'Id: {player_info["id"]}, Name: {player_info["name"]}')

    db.add_info(player_info)
    db.add_stats(scrape_stats(player, TABLES))

    player_end = time.time()

    my_logger.info(
        f'Scraped and stored player data for Id: {player_info["id"]}, Name: {player_info["name"]}.'
        f" Elapsed time = {player_end - player_start:.2f}s."
    )


def crawl(leagues: List[str]) -> None:
    """
    Iteratively crawl a list of soccer leagues and scrape player data.
    Scrapes all teams in a league and all players in a team.

    Arguments:
         leagues -- list of URLs of soccer leagues to scrape
    """

    start = time.time()

    # A single player will be used to determine the table format
    PLAYER = "/en/players/1840e36d/Thibaut-Courtois"
    player_tables = get_stats_headers(PLAYER, TABLES)

    my_logger.info(player_tables)

    db.create_db(os.getenv("DATABASE"))
    db.create_info_table()
    # db.add_info_columns()
    db.create_stats_tables(player_tables)
    # db.add_stats_columns_for_each_table(player_tables)

    pool = Pool(processes=None)

    for league in leagues:
        for squad in get_squads(league):
            for index, player in enumerate(get_players(squad)):
                pool.apply_async(scrape, args=(player,))
    pool.close()
    pool.join()

    end = time.time()

    my_logger.info(
        f" Total elapsed time = {end - start:.2f}s."
    )


if __name__ == "__main__":
    crawl(LEAGUES)
