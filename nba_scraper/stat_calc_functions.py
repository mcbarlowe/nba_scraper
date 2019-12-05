"""
These are the functions to calculate various statistics or to pull out/parse certain data types from the play by play. They have nothing to do with the functionality of actually scraping the
data.
"""
import numpy as np
import pandas as pd


def made_shot(row):
    """
    function to determine whether shot was made or missed

    Input:
    row - pandas row

    Output:
    shot_made - binary variable
    """

    if row["event_type_de"] == "shot":
        return 1
    elif row["event_type_de"] == "missed_shot":
        return 0
    elif (row["event_type_de"] == "free-throw") & ("Missed" in row["de"]):
        return 0
    elif (row["event_type_de"] == "free-throw") & ("Missed" not in row["de"]):
        return 1
    else:
        return np.nan


def parse_foul(row):
    """
    function to determine what type of foul is being commited by the player

    Input:
    row - row of nba play by play

    Output:
    foul_type - the foul type of the fould commited by the player
    """

    if "Shooting" in row["de"]:
        return "shooting"
    if "Personal" in row["de"]:
        return "personal"
    if "Loose Ball" in row["de"]:
        return "loose_ball"
    if "Technical" in row["de"]:
        return "technical"
    if "Charge" in row["de"]:
        return "charge"
    if "Defense 3 Second" in row["de"]:
        return "3 second"
    if "Flagrant" in row["de"]:
        return "flagrant"
    if "Flagrant 2" in row["de"]:
        return "flagrant 2"
    return np.nan


def parse_shot_types(row):
    """
    function to parse what type of shot is being taken

    Inputs:
    row - pandas row of play by play dataframe

    Outputs:
    shot_type - returns a shot type of the values hook, jump, layup, dunk, tip
    """
    if pd.isnull(row["shot_made"]) == 0:
        if "Layup" in row["de"]:
            return "layup"
        elif "Hook" in row["de"]:
            return "hook"
        elif "Dunk" in row["de"]:
            return "dunk"
        elif "Free" in row["de"]:
            return "free"
        else:
            return "jump"
    else:
        return np.nan


def create_seconds_elapsed(row):
    """
    this function parses the string time column and converts it into game
    seconds elapsed

    Inputs:
    row - row of play by play dataframe

    Outputs:
    time_in_seconds - the elapsed game time expressed in seconds
    """

    time_list = row["pctimestring"].strip().split(":")
    max_time = 720
    ot_max_time = 300

    if row["period"] in [1, 2, 3, 4]:
        time_in_seconds = (max_time - (int(time_list[0]) * 60 + int(time_list[1]))) + (
            720 * (int(row["period"]) - 1)
        )
    elif row["period"] > 4:
        time_in_seconds = (
            (ot_max_time - (int(time_list[0]) * 60 + int(time_list[1])))
            + (300 * (int(row["period"]) - 5))
            + 2880
        )

    return time_in_seconds


def calc_points_made(row):
    """
    function to calculate the points earned by a team with each shot made

    Inputs:
    row - row of pbp dataframe

    Outputs - value of shot made
    """

    if row["is_three"] == 1 and row["shot_made"] == 1:
        return 3
    elif row["is_three"] == 0 and row["shot_made"] == 1 and row["shot_type"] != "free":
        return 2
    elif row["shot_type"] == "free" and row["shot_made"] == 1:
        return 1
    else:
        return 0
