"""
Date: 2019-12-30
Contributor: Matthew Barlowe
twitter: @barloweanalytic
email: matt@barloweanalytics.com

This file contains the main functions to scrape and compile the WNBA api
"""
import sys
import json
import datetime
import requests
import time
import pandas as pd
import numpy as np

from nba_scraper.helper_functions import EVENT_TYPE_DICT, get_season
from nba_scraper.stat_calc_functions import (
    wnba_made_shot,
    wnba_parse_foul,
    wnba_shot_types,
    wnba_seconds_elapsed,
    wnba_points_made,
)

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:72.0) Gecko/20100101 Firefox/72.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.5",
    "X-NewRelic-ID": "VQECWF5UChAHUlNTBwgBVw==",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
    "Referer": "https://stats.nba.com/",
}


def get_player_name(player_id):
    """
    function to get the players name givn a player id

    Inputs:
    player_id   - id of player you want the name of

    Ouputs:
    player_name - full name of player with given player_id
    """
    player_url = f"https://a.data.nba.com/wnba/player/{player_id}"
    player_data = requests.get(player_url, headers=USER_AGENT)
    player_dict = json.loads(player_data.text)
    player_name = (
        f"{player_dict['data']['info']['fn']} {player_dict['data']['info']['ln']}"
    )

    return player_name


def get_team_ids(pbp_df):
    """
    this function gets the home and away team ids

    Inputs:
    pbp_df    - dataframe of the games play by play

    Outputs:
    home_team_id   - team id of the home team
    away_team_id   - team id of the away team
    """

    team_ids = pbp_df["tid"].unique()
    print(team_ids)
    team_ids = [t for t in team_ids if t > 0]
    team_url = f"https://stats.wnba.com/stats/teamdetails?TeamID={team_ids[0]}"
    team_data = requests.get(team_url, headers=USER_AGENT)
    team_data = json.loads(team_data.text)
    print(team_data)

    if (
        pbp_df["home_team_abbrev"].unique()[0]
        == team_data["resultSets"][0]["rowSet"][0][2]
    ):
        home_team_id, away_team_id = team_ids[0], team_ids[1]
    else:
        home_team_id, away_team_id = team_ids[1], team_ids[0]

    return home_team_id, away_team_id


def get_wnba_pbp_api(game_id, quarter, season):
    """
    function gets both JSON requests from the two different APIs if both
    are available and only the stats.nba.com api if not.

    Inputs:
    game_id          - String representing game id
    quarter          - number representing what quarter you want
    season           - number in the format of YYYY representing what the season is

    Outputs:
    wnba_dict         - Dictionary of the JSON response from data.wnba.com api
    """
    wnba_api_url = f"https://data.wnba.com/data/5s/v2015/json/mobile_teams/wnba/{season}/scores/pbp/1{game_id}_{quarter}_pbp.json"

    try:
        wnba_rep = requests.get(wnba_api_url)
    except json.decoder.JSONDecodeError as ex:
        print(ex)
        print(f"This is the stats.nba.com API's output: {wnba_rep.text}")
        sys.exit()

    wnba_dict = wnba_rep.json()

    return wnba_dict


def parse_wnba_pbp(game_id):
    """
    function to parse the JSON output of the api into a dataframe

    Inputs:
    game_id     - Id of game to be parsed
    season      - seaosn game is being played in

    Outputs:
    wnba_pbp_df   - wnba play by play dataframe
    """

    if game_id[2:4] in ["98", "99"]:
        season = f"19{game_id[2:4]}"
    else:
        season = f"20{game_id[2:4]}"
    results = []
    for x in range(1, 15):
        try:
            results.append(get_wnba_pbp_api(game_id, x, season))
        except ValueError:
            break
    dfs = []
    for period, v2_dict in enumerate(results):
        pbp_v2_df = pd.DataFrame(v2_dict["g"]["pla"])
        pbp_v2_df.columns = list(map(str.lower, pbp_v2_df.columns))
        pbp_v2_df["period"] = period + 1
        dfs.append(pbp_v2_df)

    pbp_df = pd.concat(dfs)

    pbp_df["game_date"] = results[0]["g"]["gcode"].split("/")[0]
    pbp_df["game_date"] = pd.to_datetime(pbp_df["game_date"], format="%Y%m%d")
    pbp_df["away_team_abbrev"] = results[0]["g"]["gcode"].split("/")[1][:3]
    pbp_df["home_team_abbrev"] = results[0]["g"]["gcode"].split("/")[1][3:]
    pbp_df["seconds_elapsed"] = pbp_df.apply(wnba_seconds_elapsed, axis=1)
    pbp_df["shot_type"] = pbp_df.apply(wnba_shot_types, axis=1)
    pbp_df["game_id"] = game_id

    # create column whether shot was succesful or not
    pbp_df["shot_made"] = pbp_df.apply(wnba_made_shot, axis=1)

    # calculate event length of each event in seconds
    pbp_df["event_length"] = pbp_df["seconds_elapsed"] - pbp_df[
        "seconds_elapsed"
    ].shift(1)

    # determine whether shot was a three pointer
    pbp_df["is_three"] = np.where(pbp_df["de"].str.contains("3pt").fillna(False), 1, 0,)

    pbp_df["event_type_de"] = pbp_df[["etype"]].replace({"etype": EVENT_TYPE_DICT})

    # create a column that says whether the shot was blocked or not
    pbp_df["is_block"] = np.where(pbp_df["de"].str.contains("BLK"), 1, 0,)

    # determine points earned
    pbp_df["points_made"] = pbp_df.apply(wnba_points_made, axis=1)

    # create columns that determine if rebound is offenseive or deffensive
    pbp_df["is_o_rebound"] = np.where(
        (pbp_df["event_type_de"] == "rebound")
        & (pbp_df["tid"] == pbp_df["tid"].shift(1))
        & (pbp_df["pid"] != 0),
        1,
        0,
    )
    pbp_df["is_d_rebound"] = np.where(
        (pbp_df["event_type_de"] == "rebound")
        & (pbp_df["tid"] != pbp_df["tid"].shift(1))
        & (pbp_df["pid"] != 0),
        1,
        0,
    )

    # create columns to determine turnovers and steals
    pbp_df["is_turnover"] = np.where(
        pbp_df["de"].str.contains("Turnover").fillna(False), 1, 0,
    )
    pbp_df["is_steal"] = np.where(
        pbp_df["de"].str.contains("Steal").fillna(False), 1, 0,
    )

    # determine what type of fouls are being commited
    pbp_df["foul_type"] = pbp_df.apply(wnba_parse_foul, axis=1)

    # determine if a shot is a putback off an offensive reboundk
    pbp_df["is_putback"] = np.where(
        (pbp_df["is_o_rebound"].shift(1) == 1) & (pbp_df["event_length"] <= 3), 1, 0
    )
    pbp_df["home_team_id"], pbp_df["away_team_id"] = get_team_ids(pbp_df)

    return pbp_df


def get_wnba_lineup(game_id, period):
    """
    function pulls the possible lineups for the given period and game id for
    both the away and home teams

    Inputs:
    game_id            - id of game
    period             - period of game

    Outputs:
    lineup_req_dict    - dictionary of lineup api request respons
    """

    if period <= 4:
        start_range = (((period - 1) * 600) * 10) + 5
    else:
        start_range = ((((period - 5) * 300) + 2880) * 10) + 5

    end_range = start_range + 2000

    url = (
        f"https://stats.wnba.com/stats/boxscoreadvancedv2/?gameId=1{game_id}&"
        f"startPeriod={period}&endPeriod={period}&startRange={start_range}&"
        f"endRange={end_range}&rangeType=2"
    )

    lineups_req = requests.get(url, headers=USER_AGENT)
    lineup_req_dict = json.loads(lineups_req.text)

    return lineup_req_dict


def get_lineup(period_df, lineups, dataframe):
    """
    this function calculates the lineups for each team at each event and then
    appends it to the current dataframe. This only works for one period at a
    time

    Inputs:
    period_df         - the main game pbp dataframe subsetted to only one period
                        in the game
    lineups           - lineup api response dictionary
    dataframe         - full game dataframe. This is passed to get players name from
                        id in case the player didn't have an event in that period.

    Outputs:
    lineup_df     - period_df with each teams lineups calculate and added to the
                    dataframe
    """

    home_team = period_df["home_team_id"].unique()[0]
    away_team = period_df["away_team_id"].unique()[0]
    players = lineups["resultSets"][0]["rowSet"]
    home_ids_names = [(p[4], p[5]) for p in players if p[1] == home_team]
    away_ids_names = [(p[4], p[5]) for p in players if p[1] == away_team]

    # gets the index of the first sub for home and away to get the players who started
    # the period by subsetting the dataframe to all actions before the first sub for
    # each team
    # pulls the unique values from the whole period dataframe if there are no subs
    # then it just pulls the unique ids from the from the dataframe itself because
    # the away/home indexes will be an empty list

    if len(away_ids_names) != 5:

        subs_df = period_df[(period_df.event_type_de == "substitution")]
        away_subs = subs_df[subs_df["tid"] == subs_df["away_team_id"]]
        away_indexes = list(away_subs.index)

        try:
            away_starting_line = list(
                period_df[
                    (period_df["tid"] == period_df["away_team_id"].unique()[0])
                    & (~pd.isnull(period_df["pid"]))
                    & (period_df.is_block == 0)
                    & (period_df.is_steal == 0)
                ]
                .loc[: away_indexes[0], :]["pid"]
                .unique()
            )
        except IndexError:
            away_starting_line = list(
                period_df[
                    (period_df["tid"] == period_df["away_team_id"].unique()[0])
                    & (~pd.isnull(period_df["pid"]))
                    & (period_df.is_block == 0)
                    & (period_df.is_steal == 0)
                ]["pid"].unique()
            )

        if away_starting_line != 5:
            starting_lineup = set()
            subs = set()
            for i in range(period_df.shape[0]):
                if (
                    period_df.iloc[i, :]["tid"] == period_df["away_team_id"].unique()[0]
                    and period_df.iloc[i, :]["is_block"] == 0
                    and period_df.iloc[i, :]["is_steal"] == 0
                ):
                    if period_df.iloc[i, :]["event_type_de"] != "substitution":
                        if (
                            period_df.iloc[i, :]["pid"] != 0
                            and period_df.iloc[i, :]["pid"] not in subs
                        ):
                            starting_lineup.add(period_df.iloc[i, :]["pid"])
                    else:
                        if period_df.iloc[i, :]["epid"] not in starting_lineup:
                            subs.add(period_df.iloc[i, :]["epid"])
                        if period_df.iloc[i, :]["pid"] not in subs:
                            starting_lineup.add(period_df.iloc[i, :]["pid"])

                    if len(starting_lineup) == 5:
                        break
            if len(away_ids_names) < 5:
                away_ids_names = [(x, get_player_name(x)) for x in starting_lineup]
            elif len(home_ids_names) > 5:
                away_ids_names = [
                    (p[0], p[1]) for p in away_ids_names if p[0] not in subs
                ]

    if len(home_ids_names) != 5:
        # subsets main dataframe by period and subsets into a home and away subs
        subs_df = period_df[(period_df.event_type_de == "substitution")]
        home_subs = subs_df[subs_df["tid"] == subs_df["home_team_id"]]
        home_indexes = list(home_subs.index)

        try:
            home_starting_line = list(
                period_df[
                    (period_df["tid"] == period_df["home_team_id"].unique()[0])
                    & (~pd.isnull(period_df["pid"]))
                    & (period_df.is_block == 0)
                    & (period_df.is_steal == 0)
                ]
                .loc[: home_indexes[0], :]["pid"]
                .unique()
            )
        except IndexError:
            home_starting_line = list(
                period_df[
                    (period_df["tid"] == period_df["home_team_id"].unique()[0])
                    & (~pd.isnull(period_df["pid"]))
                    & (period_df.is_block == 0)
                    & (period_df.is_steal == 0)
                ]["pid"].unique()
            )

        if home_starting_line != 5:
            starting_lineup = set()
            subs = set()
            for i in range(period_df.shape[0]):
                if (
                    period_df.iloc[i, :]["tid"] == period_df["home_team_id"].unique()[0]
                    and period_df.iloc[i, :]["is_block"] == 0
                    and period_df.iloc[i, :]["is_steal"] == 0
                ):
                    if period_df.iloc[i, :]["event_type_de"] != "substitution":
                        if (
                            period_df.iloc[i, :]["pid"] != 0
                            and period_df.iloc[i, :]["pid"] not in subs
                        ):
                            starting_lineup.add(period_df.iloc[i, :]["pid"])
                    else:
                        if period_df.iloc[i, :]["epid"] not in starting_lineup:
                            subs.add(period_df.iloc[i, :]["epid"])
                        if period_df.iloc[i, :]["pid"] not in subs:
                            starting_lineup.add(period_df.iloc[i, :]["pid"])

                    if len(starting_lineup) == 5:
                        break
            if len(home_ids_names) < 5:
                home_ids_names = [(x, get_player_name(x)) for x in starting_lineup]
            elif len(home_ids_names) > 5:
                home_ids_names = [
                    (p[0], p[1]) for p in home_ids_names if p[0] not in subs
                ]

    if period_df.shape[1] == 37:
        period_df.loc[:, "dummy_col"] = ""
    # creating columns to populate with players on the court
    period_df.loc[:, "home_player_1"] = ""
    period_df.loc[:, "home_player_1_id"] = ""
    period_df.loc[:, "home_player_2"] = ""
    period_df.loc[:, "home_player_2_id"] = ""
    period_df.loc[:, "home_player_3"] = ""
    period_df.loc[:, "home_player_3_id"] = ""
    period_df.loc[:, "home_player_4"] = ""
    period_df.loc[:, "home_player_4_id"] = ""
    period_df.loc[:, "home_player_5"] = ""
    period_df.loc[:, "home_player_5_id"] = ""
    period_df.loc[:, "away_player_1"] = ""
    period_df.loc[:, "away_player_1_id"] = ""
    period_df.loc[:, "away_player_2"] = ""
    period_df.loc[:, "away_player_2_id"] = ""
    period_df.loc[:, "away_player_3"] = ""
    period_df.loc[:, "away_player_3_id"] = ""
    period_df.loc[:, "away_player_4"] = ""
    period_df.loc[:, "away_player_4_id"] = ""
    period_df.loc[:, "away_player_5"] = ""
    period_df.loc[:, "away_player_5_id"] = ""
    # add players to the columns by looping through the dataframe and putting the
    # players in for each row using the starting lineup list. If there is a
    # substitution event then the player coming on replaces the player going off in
    # the list this is done for the whole period

    for i in range(period_df.shape[0]):
        if (
            period_df.iloc[i, :]["event_type_de"] == "substitution"
            and period_df.iloc[i, :]["tid"] == home_team
        ):
            home_ids_names = [
                ids for ids in home_ids_names if ids[0] != period_df.iloc[i, :]["pid"]
            ]
            player_url = (
                f"https://a.data.nba.com/wnba/player/{period_df.iloc[i, :]['epid']}"
            )
            player_data = requests.get(player_url, headers=USER_AGENT)
            player_dict = json.loads(player_data.text)
            player_name = f"{player_dict['data']['info']['fn']} {player_dict['data']['info']['ln']}"
            home_ids_names.append((period_df.iloc[i, :]["epid"], player_name))
            period_df.iat[i, 39] = home_ids_names[0][0]
            period_df.iat[i, 38] = home_ids_names[0][1]
            period_df.iat[i, 41] = home_ids_names[1][0]
            period_df.iat[i, 40] = home_ids_names[1][1]
            period_df.iat[i, 43] = home_ids_names[2][0]
            period_df.iat[i, 42] = home_ids_names[2][1]
            period_df.iat[i, 45] = home_ids_names[3][0]
            period_df.iat[i, 44] = home_ids_names[3][1]
            period_df.iat[i, 47] = home_ids_names[4][0]
            period_df.iat[i, 46] = home_ids_names[4][1]
            period_df.iat[i, 49] = away_ids_names[0][0]
            period_df.iat[i, 48] = away_ids_names[0][1]
            period_df.iat[i, 51] = away_ids_names[1][0]
            period_df.iat[i, 50] = away_ids_names[1][1]
            period_df.iat[i, 53] = away_ids_names[2][0]
            period_df.iat[i, 52] = away_ids_names[2][1]
            period_df.iat[i, 55] = away_ids_names[3][0]
            period_df.iat[i, 54] = away_ids_names[3][1]
            period_df.iat[i, 57] = away_ids_names[4][0]
            period_df.iat[i, 56] = away_ids_names[4][1]
        elif (
            period_df.iloc[i, :]["event_type_de"] == "substitution"
            and period_df.iloc[i, :]["tid"] == away_team
        ):
            away_ids_names = [
                ids for ids in away_ids_names if ids[0] != period_df.iloc[i, :]["pid"]
            ]
            player_url = (
                f"https://a.data.nba.com/wnba/player/{period_df.iloc[i, :]['epid']}"
            )
            player_data = requests.get(player_url, headers=USER_AGENT)
            player_dict = json.loads(player_data.text)
            player_name = f"{player_dict['data']['info']['fn']} {player_dict['data']['info']['ln']}"
            away_ids_names.append((period_df.iloc[i, :]["epid"], player_name))
            period_df.iat[i, 39] = home_ids_names[0][0]
            period_df.iat[i, 38] = home_ids_names[0][1]
            period_df.iat[i, 41] = home_ids_names[1][0]
            period_df.iat[i, 40] = home_ids_names[1][1]
            period_df.iat[i, 43] = home_ids_names[2][0]
            period_df.iat[i, 42] = home_ids_names[2][1]
            period_df.iat[i, 45] = home_ids_names[3][0]
            period_df.iat[i, 44] = home_ids_names[3][1]
            period_df.iat[i, 47] = home_ids_names[4][0]
            period_df.iat[i, 46] = home_ids_names[4][1]
            period_df.iat[i, 49] = away_ids_names[0][0]
            period_df.iat[i, 48] = away_ids_names[0][1]
            period_df.iat[i, 51] = away_ids_names[1][0]
            period_df.iat[i, 50] = away_ids_names[1][1]
            period_df.iat[i, 53] = away_ids_names[2][0]
            period_df.iat[i, 52] = away_ids_names[2][1]
            period_df.iat[i, 55] = away_ids_names[3][0]
            period_df.iat[i, 54] = away_ids_names[3][1]
            period_df.iat[i, 57] = away_ids_names[4][0]
            period_df.iat[i, 56] = away_ids_names[4][1]
        else:
            period_df.iat[i, 39] = home_ids_names[0][0]
            period_df.iat[i, 38] = home_ids_names[0][1]
            period_df.iat[i, 41] = home_ids_names[1][0]
            period_df.iat[i, 40] = home_ids_names[1][1]
            period_df.iat[i, 43] = home_ids_names[2][0]
            period_df.iat[i, 42] = home_ids_names[2][1]
            period_df.iat[i, 45] = home_ids_names[3][0]
            period_df.iat[i, 44] = home_ids_names[3][1]
            period_df.iat[i, 47] = home_ids_names[4][0]
            period_df.iat[i, 46] = home_ids_names[4][1]
            period_df.iat[i, 49] = away_ids_names[0][0]
            period_df.iat[i, 48] = away_ids_names[0][1]
            period_df.iat[i, 51] = away_ids_names[1][0]
            period_df.iat[i, 50] = away_ids_names[1][1]
            period_df.iat[i, 53] = away_ids_names[2][0]
            period_df.iat[i, 52] = away_ids_names[2][1]
            period_df.iat[i, 55] = away_ids_names[3][0]
            period_df.iat[i, 54] = away_ids_names[3][1]
            period_df.iat[i, 57] = away_ids_names[4][0]
            period_df.iat[i, 56] = away_ids_names[4][1]

    return period_df


def wnba_main_scrape(game_id):
    """
    This is the main function which ties everything together and will be imported
    into nba_scraper module as the hook to scrape nba games

    Inputs:
    game_id      - WNBA game id to be scraped

    Outputs:
    game_df      - WNBA dataframe of the play by play

    WNBA has 204 games in a season with the same game_id structure as nba
    they add a 1 to the front of the game_id for some reason
    """

    pbp_df = parse_wnba_pbp(game_id)

    periods = []

    for period in range(1, pbp_df["period"].max() + 1):
        lineups = get_wnba_lineup(game_id, period)
        periods.append(
            get_lineup(pbp_df[pbp_df["period"] == period].copy(), lineups, pbp_df,)
        )

    pbp_df = pd.concat(periods)

    return pbp_df


def main():
    # parse_wnba_pbp("021900122")
    wnba_main_scrape("021900001")


if __name__ == "__main__":
    main()
