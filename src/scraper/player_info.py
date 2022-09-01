# player_info.py
"""Function to scrape the general player information."""

from asyncio.log import logger
from cmath import log
import re
from datetime import date
from time import strptime
from requests import get_soup
import logging
import json


def scrape_info(player):
    """
    Scrape general information about a player.

    Arguments:
        player  -- string part of the URL path that identifies a player.
    Returns:
        info    -- a dictionary of player information
                -- each key is a column (name, position, etc.)
                -- each value is a data point
    """
    url = f"https://fbref.com{player}"
    soup = get_soup(url)

    try:
        header = json.loads(soup.find("script", type="application/ld+json").string)
    except:
        logging.error("header error")

    # header = soup.find("div", {"itemtype": "https://schema.org/Person"})

    # Store general player info in a dictionary
    info = {}

    # Find the unique player ID
    try:
        info["id"] = player[12:20]
    except:
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player id."
        )

    # Find the player name
    try:
        firstname = player[21:]
        secondname = ""

        try:
            firstname = player[21:].split("-")[0]
        except:
            pass

        try:
            secondname = player[21:].split("-")[1]
        except:
            pass

        info["name"] = firstname

        if secondname != "":
            info["name"] = f"{firstname} {secondname}"

        logging.info(f"name: {info['name']}")
    except:
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player name."
        )
        logging.error(header)
        exit(0)

    # Find the player's preferred position(s)
    # try:
    #     info["position"] = (
    #         header.find(text="Position:")
    #         .parent.next_sibling.split("▪")[0][1:]
    #         .replace("\xa0", "")
    #     )
    # except:
    #     print(
    #         "playerInfo: scrape_info: Exception was raised when trying to scrape player position."
    #     )

    # Find the player's preferred foot
    # try:
    #     info["foot"] = header.find(text="Footed:").parent.next_sibling.lstrip()
    # except:
    #     print(
    #         "playerInfo: scrape_info: Exception was raised when trying to scrape player foot."
    #     )

    # Find player's height
    try:
        info["height"] = int(header["height"]["value"].split()[0])
    except:
        logging.error(header["height"]["value"])
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player height."
        )

    # Find player's weight
    try:
        info["weight"] = int(header["weight"]["value"].split()[0])
    except:
        logging.error(header["weight"]["value"])
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player weight."
        )

    # Find player's date of birth
    try:
        info["dob"] = header["birthDate"]
    except:
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player dob."
        )

    # Find player's city of birth
    try:
        info["cityob"] = header["birthPlace"].split(",")[0]
    except:
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player cityob."
        )

    # Find player;s country of birth
    try:
        splitted_birthPlace = header["birthPlace"].split(",")

        if len(splitted_birthPlace) > 1:
            country = header["birthPlace"].split(",")[1].strip()
        else:
            country = splitted_birthPlace[0].strip()

        info["countryob"] = country
    except:
        logging.error(header["birthPLace"])

        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player countryob."
        )

    # Find the national team the player plays for
    # try:
    #     info["nt"] = header.find(text="National Team:").parent.parent.a.get_text(
    #         strip=True
    #     )
    # except:
    #     print(
    #         "playerInfo: scrape_info: Exception was raised when trying to scrape player nt."
    #     )

    # Find the club the player currently plays for
    try:
        info["club"] = header["memberOf"]["name"]
    except:
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player club."
        )

    # Calculate the player's age from his date of birth
    try:
        info["age"] = get_age(info["dob"])
    except:
        print(
            "playerInfo: scrape_info: Exception was raised when trying to scrape player age."
        )

    return info


def get_age(birthdate: str) -> int:
    """
    # Calculate age from a player's DOB.

    Arguments:
        birthdate   -- string representing the player's date of birth (format: 'YYYY-MM-DD')
    Returns:
        age         -- player's age in years
    """
    try:
        dob_list = birthdate.split("-")
        birthdate_year = int(dob_list[0], 10)
        birthdate_day = int(dob_list[2], 10)
        birthdate_month = int(dob_list[1], 10)
        today = date.today()
        age = (
            today.year
            - birthdate_year
            - ((today.month, today.day) < (birthdate_month, birthdate_day))
        )
    except (IndexError, AttributeError, ValueError) as e:
        print("player_info: get_age: ", e)
        return None

    return age
