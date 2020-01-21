"""
These are the functions to calculate various statistics or to pull out/parse certain data types from the play by play. They have nothing to do with the functionality of actually scraping the
data.
"""
import numpy as np


foul_dict = {
    0: "no foul",
    1: "personal",
    2: "shooting",
    3: "loose ball",
    4: "offensive",
    5: "inbound",
    6: "away from play",
    7: "elbow",
    8: "punching",
    9: "clear path",
    10: "double personal",
    11: "technical",
    12: "non-unsportsmanlike technical",
    13: "hanging technical",
    14: "flagrant type 1",
    15: "flagrant type 2",
    16: "double technical",
    17: "defense 3 second",
    18: "delay technical",
    19: "taunting technical",
    20: "player_control",
    21: "indirect technical",
    25: "excess timeout technical",
    26: "offensive charge",
    27: "personal block",
    28: "shooting block",
    30: "too many players technical",
}
SHOT_DICT = {
    1: {
        0: "No Shot",
        1: "Jump Shot",
        2: "Running Jump Shot",
        3: "Hook Shot",
        4: "Tip Shot",
        5: "Layup Shot",
        6: "Driving Layup Shot",
        7: "Dunk Shot",
        8: "Slam Dunk Shot",
        9: "Driving Dunk Shot",
        40: "Layup Shot",
        41: "Running Layup Shot",
        42: "Driving Layup Shot",
        43: "Alley Oop Layup shot",
        44: "Reverse Layup Shot",
        45: "Jump Shot",
        46: "Running Jump Shot",
        47: "Turnaround Jump Shot",
        48: "Dunk Shot",
        49: "Driving Dunk Shot",
        50: "Running Dunk Shot",
        51: "Reverse Dunk Shot",
        52: "Alley Oop Dunk Shot",
        53: "Tip Shot",
        54: "Running Tip Shot",
        55: "Hook Shot",
        56: "Running Hook Shot",
        57: "Driving Hook Shot",
        58: "Turnaround Hook Shot",
        59: "Finger Roll Shot",
        60: "Running Finger Roll Shot",
        61: "Driving Finger Roll Shot",
        62: "Turnaround Finger Roll Shot",
        63: "Fadeaway Jump Shot",
        64: "Follow Up Dunk Shot",
        65: "Jump Hook Shot",
        66: "Jump Bank Shot",
        67: "Hook Bank Shot",
        71: "Finger Roll Layup Shot",
        72: "Putback Layup Shot",
        73: "Driving Reverse Layup Shot",
        74: "Running Reverse Layup Shot",
        75: "Driving Finger Roll Layup Shot",
        76: "Running Finger Roll Layup Shot",
        77: "Driving Jump shot",
        78: "Floating Jump shot",
        79: "Pullup Jump shot",
        80: "Step Back Jump shot",
        81: "Pullup Bank shot",
        82: "Driving Bank shot",
        83: "Fadeaway Bank shot",
        84: "Running Bank shot",
        85: "Turnaround Bank shot",
        86: "Turnaround Fadeaway shot",
        87: "Putback Dunk Shot",
        88: "Driving Slam Dunk Shot",
        89: "Reverse Slam Dunk Shot",
        90: "Running Slam Dunk Shot",
        91: "Putback Reverse Dunk Shot",
        92: "Putback Slam Dunk Shot",
        93: "Driving Bank Hook Shot",
        94: "Jump Bank Hook Shot",
        95: "Running Bank Hook Shot",
        96: "Turnaround Bank Hook Shot",
        97: "Tip Layup Shot",
        98: "Cutting Layup Shot",
        99: "Cutting Finger Roll Layup Shot",
        100: "Running Alley Oop Layup Shot",
        101: "Driving Floating Jump Shot",
        102: "Driving Floating Bank Jump Shot",
        103: "Running Pull-Up Jump Shot",
        104: "Step Back Bank Jump Shot",
        105: "Turnaround Fadeaway Bank Jump Shot",
        106: "Running Alley Oop Dunk Shot",
        107: "Tip Dunk Shot",
        108: "Cutting Dunk Shot",
        109: "Driving Reverse Dunk Shot",
        110: "Running Reverse Dunk Shot",
    },
    2: {
        0: "No Shot",
        1: "Jump Shot",
        2: "Running Jump Shot",
        3: "Hook Shot",
        4: "Tip Shot",
        5: "Layup Shot",
        6: "Driving Layup Shot",
        7: "Dunk Shot",
        8: "Slam Dunk Shot",
        9: "Driving Dunk Shot",
        40: "Layup Shot",
        41: "Running Layup Shot",
        42: "Driving Layup Shot",
        43: "Alley Oop Layup shot",
        44: "Reverse Layup Shot",
        45: "Jump Shot",
        46: "Running Jump Shot",
        47: "Turnaround Jump Shot",
        48: "Dunk Shot",
        49: "Driving Dunk Shot",
        50: "Running Dunk Shot",
        51: "Reverse Dunk Shot",
        52: "Alley Oop Dunk Shot",
        53: "Tip Shot",
        54: "Running Tip Shot",
        55: "Hook Shot",
        56: "Running Hook Shot",
        57: "Driving Hook Shot",
        58: "Turnaround Hook Shot",
        59: "Finger Roll Shot",
        60: "Running Finger Roll Shot",
        61: "Driving Finger Roll Shot",
        62: "Turnaround Finger Roll Shot",
        63: "Fadeaway Jump Shot",
        64: "Follow Up Dunk Shot",
        65: "Jump Hook Shot",
        66: "Jump Bank Shot",
        67: "Hook Bank Shot",
        71: "Finger Roll Layup Shot",
        72: "Putback Layup Shot",
        73: "Driving Reverse Layup Shot",
        74: "Running Reverse Layup Shot",
        75: "Driving Finger Roll Layup Shot",
        76: "Running Finger Roll Layup Shot",
        77: "Driving Jump shot",
        78: "Floating Jump shot",
        79: "Pullup Jump shot",
        80: "Step Back Jump shot",
        81: "Pullup Bank shot",
        82: "Driving Bank shot",
        83: "Fadeaway Bank shot",
        84: "Running Bank shot",
        85: "Turnaround Bank shot",
        86: "Turnaround Fadeaway shot",
        87: "Putback Dunk Shot",
        88: "Driving Slam Dunk Shot",
        89: "Reverse Slam Dunk Shot",
        90: "Running Slam Dunk Shot",
        91: "Putback Reverse Dunk Shot",
        92: "Putback Slam Dunk Shot",
        93: "Driving Bank Hook Shot",
        94: "Jump Bank Hook Shot",
        95: "Running Bank Hook Shot",
        96: "Turnaround Bank Hook Shot",
        97: "Tip Layup Shot",
        98: "Cutting Layup Shot",
        99: "Cutting Finger Roll Layup Shot",
        100: "Running Alley Oop Layup Shot",
        101: "Driving Floating Jump Shot",
        102: "Driving Floating Bank Jump Shot",
        103: "Running Pull-Up Jump Shot",
        104: "Step Back Bank Jump Shot",
        105: "Turnaround Fadeaway Bank Jump Shot",
        106: "Running Alley Oop Dunk Shot",
        107: "Tip Dunk Shot",
        108: "Cutting Dunk Shot",
        109: "Driving Reverse Dunk Shot",
        110: "Running Reverse Dunk Shot",
    },
    3: {
        0: "No Shot",
        10: "Free Throw 1 of 1",
        11: "Free Throw 1 of 2",
        12: "Free Throw 2 of 2",
        13: "Free Throw 1 of 3",
        14: "Free Throw 2 of 3",
        15: "Free Throw 3 of 3",
        16: "Free Throw Technical",
        17: "Free Throw Clear Path",
        18: "Free Throw Flagrant 1 of 2",
        19: "Free Throw Flagrant 2 of 2",
        20: "Free Throw Flagrant 1 of 1",
        21: "Free Throw Technical 1 of 2",
        22: "Free Throw Technical 2 of 2",
        25: "Free Throw Clear Path 1 of 2",
        26: "Free Throw Clear Path 2 of 2",
        27: "Free Throw Flagrant 1 of 3",
        28: "Free Throw Flagrant 2 of 3",
        29: "Free Throw Flagrant 3 of 3",
    },
}


def wnba_made_shot(row):
    """
    function to determine whether shot was made or missed

    Input:
    row - pandas row

    Output:
    shot_made - binary variable
    """

    de = row["de"]

    if row["etype"] == 1:
        return 1
    elif row["etype"] == 2:
        return 0
    elif row["etype"] == 3 and ("Missed" in de):
        return 0
    elif row["etype"] == 3:
        return 1
    else:
        return np.nan


def made_shot(row):
    """
    function to determine whether shot was made or missed

    Input:
    row - pandas row

    Output:
    shot_made - binary variable
    """

    if row["homedescription"] == None:
        home_d = "false"
    else:
        home_d = row["homedescription"]

    if row["visitordescription"] == None:
        away_d = "false"
    else:
        away_d = row["visitordescription"]
    if row["eventmsgtype"] == 1:
        return 1
    elif row["eventmsgtype"] == 2:
        return 0
    elif row["eventmsgtype"] == 3 and ("MISS" in home_d or "MISS" in away_d):
        return 0
    elif row["eventmsgtype"] == 3:
        return 1
    else:
        return np.nan


def wnba_parse_foul(row):
    """
    function to determine what type of foul is being commited by the player

    Input:
    row - row of nba play by play

    Output:
    foul_type - the foul type of the fould commited by the player
    """

    try:
        if row["etype"] == 6:
            try:
                return foul_dict[row["mtype"]]
            except KeyError:
                return np.nan
        return np.nan
    except KeyError:
        return np.nan

def parse_foul(row):
    """
    function to determine what type of foul is being commited by the player

    Input:
    row - row of nba play by play

    Output:
    foul_type - the foul type of the fould commited by the player
    """

    try:
        if row["eventmsgtype"] == 6:
            try:
                return foul_dict[row["eventmsgactiontype"]]
            except KeyError:
                return np.nan
        return np.nan
    except KeyError:
        return np.nan


def wnba_shot_types(row):
    """
    function to parse what type of shot is being taken

    Inputs:
    row - pandas row of play by play dataframe

    Outputs:
    shot_type - returns a shot type of the values hook, jump, layup, dunk, tip
    """
    try:
        if row["etype"] in [1, 2, 3]:
            return SHOT_DICT[row["etype"]][row["mtype"]]
        else:
            return np.nan
    except KeyError:
        return np.nan


def parse_shot_types(row):
    """
    function to parse what type of shot is being taken

    Inputs:
    row - pandas row of play by play dataframe

    Outputs:
    shot_type - returns a shot type of the values hook, jump, layup, dunk, tip
    """
    try:
        if row["eventmsgtype"] in [1, 2, 3]:
            return SHOT_DICT[row["eventmsgtype"]][row["eventmsgactiontype"]]
        else:
            return np.nan
    except KeyError:
        return np.nan


def wnba_seconds_elapsed(row):
    """
    this function parses the string time column and converts it into game
    seconds elapsed

    Inputs:
    row - row of play by play dataframe

    Outputs:
    time_in_seconds - the elapsed game time expressed in seconds
    """

    time_list = row["cl"].strip().split(":")
    max_time = 600
    ot_max_time = 300

    if row["period"] in [1, 2, 3, 4]:
        time_in_seconds = (
            max_time - (int(time_list[0]) * 60 + float(time_list[1]))
        ) + (720 * (int(row["period"]) - 1))
    elif row["period"] > 4:
        time_in_seconds = (
            (ot_max_time - (int(time_list[0]) * 60 + float(time_list[1])))
            + (300 * (int(row["period"]) - 5))
            + 2880
        )

    return time_in_seconds


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


def wnba_points_made(row):
    """
    function to calculate the points earned by a team with each shot made

    Inputs:
    row - row of pbp dataframe

    Outputs - value of shot made
    """

    if row["is_three"] == 1 and row["shot_made"] == 1:
        return 3
    elif row["is_three"] == 0 and row["shot_made"] == 1 and row["etype"] != 3:
        return 2
    elif row["etype"] == 3 and row["shot_made"] == 1:
        return 1
    else:
        return 0


def calc_points_made(row):
    """
    function to calculate the points earned by a team with each shot made

    Inputs:
    row - row of pbp dataframe

    Outputs - value of shot made
    """

    if row["is_three"] == 1 and row["shot_made"] == 1:
        return 3
    elif row["is_three"] == 0 and row["shot_made"] == 1 and row["eventmsgtype"] != 3:
        return 2
    elif row["eventmsgtype"] == 3 and row["shot_made"] == 1:
        return 1
    else:
        return 0
