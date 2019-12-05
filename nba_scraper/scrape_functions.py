"""
Date: 2019-01-02
Contributor: Matthew Barlowe
Twitter: @barloweanalytic
Email: matt@barloweanalytics.com

This file contains the main functions to scrape and compile the NBA api and
return a CSV file of the pbp for the provided game
"""
import sys
import json
import requests
import pandas as pd
import numpy as np

# TODO probably need to fix these to import modularly correctly
from helper_functions import SHOT_TYPE_DICT, EVENT_TYPE_DICT
from stat_calc_functions import (
    made_shot,
    parse_foul,
    parse_shot_types,
    create_seconds_elapsed,
    calc_points_made,
)

# TODO look at replacing this with the fake-useragent package Matt Barlowe 2019-12-04
# have to pass this to the requests function or the api will return a 403 code
USER_AGENT = {
    "Host": "stats.nba.com",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Referer": "stats.nba.com",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
}
# gameids that are regular season ids start with 002 while preseason starts
# with 001. the next two digits are the year represented by the first year
# in the sequence so for the 20172018 season it would be 17 and 20182019 it
# would be 18 etc. 004 represents the playoffs
# schedule_api = ('http://data.nba.com/data/10s/v2015/json/'
#                f'mobile_teams/nba/{season}/league/00_full_schedule.json')

# this api goes back to 20152016 season maybe use this one
# sched_api = 'http://data.nba.net/data/10s/prod/v1/{season}/schedule.json'


def scrape_pbp(v2_dict, pbp_dict):
    """
    This function scrapes both of the pbp urls and returns a joined/cleaned
    pbp dataframe

    Inputs:
    v2_dict     - stats.nba.com api response
    pbp_dict    - data.nba.com api response

    Outputs:
    clean_df - final cleaned dataframe
    """

    # converting stats.nba.com json into pandas dataframe
    pbp_v2_df = pd.DataFrame(
        v2_dict["resultSets"][0]["rowSet"], columns=v2_dict["resultSets"][0]["headers"]
    )
    pbp_v2_df.columns = list(map(str.lower, pbp_v2_df.columns))

    # converting data.nba.com json to dataframe
    pbp_df_list = []
    for qtr in range(len(pbp_dict["g"]["pd"])):
        pbp_df_list.append(pd.DataFrame(pbp_dict["g"]["pd"][qtr]["pla"]))
    pbp_df = pd.concat(pbp_df_list)

    # pulling the home and away team abbreviations and the game date
    gcode = pbp_dict["g"]["gcode"].split("/")

    # TODO find some way of getting date from stats.nba.com instead of
    # TODO data.nba.com so that way can extend to past seasons

    # joining the two dataframes together and only pulling in relavent columns
    clean_df = pbp_v2_df.merge(
        pbp_df[["evt", "locX", "locY", "hs", "vs", "de"]],
        left_on="eventnum",
        right_on="evt",
    )

    # add date and team abbrev columns to dataframe
    clean_df.loc[:, "home_team_abbrev"] = gcode[1][3:]
    clean_df.loc[:, "away_team_abbrev"] = gcode[1][:3]
    # NBA api has the game listed as playing on 20170107 on the link I get my
    # dates from
    if str(clean_df["game_id"].unique()[0]) == "0021600559":
        clean_df.loc[:, "game_date"] = str(int(gcode[0]) + 1)
    else:
        clean_df.loc[:, "game_date"] = gcode[0]
    clean_df.loc[:, "game_date"] = pd.to_datetime(
        clean_df["game_date"], format="%Y%m%d"
    )
    clean_df.loc[:, ("season")] = np.where(
        clean_df.game_date.dt.month.isin([10, 11, 12]),
        clean_df.game_date.dt.year + 1,
        clean_df.game_date.dt.year,
    )

    # code to properly get the team ids as the scientific notation cuts off some digits
    clean_df.loc[:, "home_team_id"] = (
        clean_df[clean_df["player1_team_abbreviation"] == gcode[1][3:]][
            "player1_team_id"
        ]
        .astype(int)
        .unique()
    )

    clean_df.loc[:, "away_team_id"] = (
        clean_df[clean_df["player1_team_abbreviation"] == gcode[1][:3]][
            "player1_team_id"
        ]
        .astype(int)
        .unique()
    )

    # create an event team colum
    clean_df["event_team"] = np.where(
        clean_df["homedescription"].isnull(),
        clean_df["away_team_abbrev"],
        np.where(
            clean_df["visitordescription"].isnull(),
            clean_df["home_team_abbrev"],
            np.where(
                (clean_df["homedescription"].str.contains("Turnover"))
                | (clean_df["homedescription"].str.contains("MISS")),
                clean_df["home_team_abbrev"],
                clean_df["away_team_abbrev"],
            ),
        ),
    )

    # create and event type description column
    clean_df["event_type_de"] = clean_df[["eventmsgtype"]].replace(
        {"eventmsgtype": EVENT_TYPE_DICT}
    )

    # create and shot type description column
    clean_df["shot_type_de"] = clean_df[["eventmsgtype", "eventmsgactiontype"]].apply(
        lambda x: SHOT_TYPE_DICT.get(int(x["eventmsgactiontype"]), np.nan)
        if np.isin(x["eventmsgtype"], [1, 2])
        else np.nan,
        axis=1,
    )

    # create column whether shot was succesful or not
    clean_df["shot_made"] = clean_df.apply(made_shot, axis=1)

    # create a column that says whether the shot was blocked or not
    clean_df["is_block"] = np.where(
        clean_df["homedescription"].str.contains("BLOCK")
        | clean_df["visitordescription"].str.contains("BLOCK"),
        1,
        0,
    )
    # parse mtype column to get all the shot types being taken
    clean_df["shot_type"] = clean_df.apply(parse_shot_types, axis=1)

    # Clean time to get a seconds elapsed column
    clean_df["seconds_elapsed"] = clean_df.apply(create_seconds_elapsed, axis=1)

    # calculate event length of each even in seconds
    clean_df["event_length"] = clean_df["seconds_elapsed"] - clean_df[
        "seconds_elapsed"
    ].shift(1)

    # determine whether shot was a three pointer
    clean_df["is_three"] = np.where(clean_df["de"].str.contains("3pt"), 1, 0)

    # determine points earned
    clean_df["points_made"] = clean_df.apply(calc_points_made, axis=1)

    # create columns that determine if rebound is offenseive or deffensive
    clean_df["is_o_rebound"] = np.where(
        (clean_df["event_type_de"] == "rebound")
        & (clean_df["event_team"] == clean_df["event_team"].shift(1))
        & (
            ~clean_df["player1_id"].isin(
                [clean_df.home_team_id.unique()[0], clean_df.away_team_id.unique()[0]]
            )
        ),
        1,
        0,
    )
    clean_df["is_d_rebound"] = np.where(
        (clean_df["event_type_de"] == "rebound")
        & (clean_df["event_team"] != clean_df["event_team"].shift(1))
        & (
            ~clean_df["player1_id"].isin(
                [clean_df.home_team_id.unique()[0], clean_df.away_team_id.unique()[0]]
            )
        ),
        1,
        0,
    )

    # create columns to determine turnovers and steals
    clean_df["is_turnover"] = np.where(clean_df["de"].str.contains("Turnover"), 1, 0)
    clean_df["is_steal"] = np.where(clean_df["de"].str.contains("Steal"), 1, 0)

    # determine what type of fouls are being commited
    clean_df["foul_type"] = clean_df.apply(parse_foul, axis=1)

    # determine if a shot is a putback off an offensive reboundk
    clean_df["is_putback"] = np.where(
        (clean_df["is_o_rebound"].shift(1) == 1) & (clean_df["event_length"] <= 3), 1, 0
    )

    return clean_df


def get_pbp_api(season_string, pbp_season, game_id, season_type):
    """
    function gets both JSON requests from the two different APIs if both
    are available and only the stats.nba.com api if not.

    Inputs:
    season_string    - String representing season for the stats.nba.com api
    pbp_season       - String representing season for the data.nba.com api
    game_id          - String representing game id
    season_type      - type of game played: Regular Season, Playoffs etc.

    Outputs:
    pbp_dict         - Dictionary of the JSON response from data.nba.com api
    v2_dict          - Dictionary of the JSON response from the stats.nba.com api
    """
    v2_api_url = (
        "https://stats.nba.com/stats/playbyplayv2?"
        f"EndPeriod=10&GameID={game_id}&StartPeriod=1"
    )

    pbp_api_url = (
        f"https://data.nba.com/data/10s/v2015/json/mobile_teams/"
        f"nba/{pbp_season}/scores/pbp/{game_id}_full_pbp.json"
    )

    # this will be the main url used for the v2 api url once testing is done
    # v2 api will contain all the player info for each play in the game while the
    # pbp_api_url will contain xy coords for each event

    try:
        v2_rep = requests.get(v2_api_url, headers=USER_AGENT)
    except json.decoder.JSONDecodeError as ex:
        print(ex)
        print(f"This is the stats.nba.com API's output: {v2_rep.text}")
        sys.exit()
    try:
        pbp_rep = requests.get(pbp_api_url)
    except json.decoder.JSONDecodeError as ex:
        print(ex)
        print(f"This is the data.nba.com API's output: {pbp_rep.text}")
        sys.exit()

    v2_dict = v2_rep.json()
    pbp_dict = pbp_rep.json()

    return v2_dict, pbp_dict


def get_lineup_api(
    season_string, away_team_id, home_team_id, season_type, period, game_date
):
    """
    function pulls the possible lineups for the given period and game id for
    both the away and home teams

    Inputs:
    season_string      - string representing season usually in form of 2017-18
    away_team_id       - away team team id
    home_team_id       - home team team id
    season_type        - type of game: Regular Season, playoffs etc.
    period             - period of game
    game_date          - date of game

    Outputs:
    home_dict          - dictionary of possible home lineups
    away_dict          - dicitonary of possible away lineups
    """

    home_lineup_api = (
        "https://stats.nba.com/stats/leaguedashlineups?"
        f"Conference=&DateFrom={game_date}&DateTo={game_date}"
        "&Division=&GameSegment=&GroupQuantity=5&LastNGames=0"
        "&LeagueID=&Location=&MeasureType=Base&Month=0&"
        f"OpponentTeamID={away_team_id}&Outcome=&PORound=&"
        f"PaceAdjust=N&PerMode=Totals&Period={period}&"
        f"PlusMinus=N&Rank=N&Season={season_string}&"
        f"SeasonSegment=&SeasonType={season_type}"
        "&ShotClockRange=&TeamID=&VsConference=&VsDivision="
    )

    away_lineup_api = (
        "https://stats.nba.com/stats/leaguedashlineups?"
        f"Conference=&DateFrom={game_date}&DateTo={game_date}"
        "&Division=&GameSegment=&GroupQuantity=5&LastNGames=0"
        "&LeagueID=&Location=&MeasureType=Base&Month=0&"
        f"OpponentTeamID={home_team_id}&Outcome=&PORound=&"
        f"PaceAdjust=N&PerMode=Totals&Period={period}&"
        f"PlusMinus=N&Rank=N&Season={season_string}&"
        f"SeasonSegment=&SeasonType={season_type}"
        "&ShotClockRange=&TeamID=&VsConference=&VsDivision="
    )

    try:
        home_lineup_req = requests.get(home_lineup_api, headers=USER_AGENT)
    except json.decoder.JSONDecodeError as ex:
        print(ex)
        print(f"This is the home_lineup API's output: {home_lineup_req.text}")
        sys.exit()
    try:
        away_lineup_req = requests.get(away_lineup_api, headers=USER_AGENT)
    except json.decoder.JSONDecodeError as ex:
        print(ex)
        print(f"This is the away_lineup API's output: {away_lineup_req.text}")
        sys.exit()

    home_dict = home_lineup_req.json()
    away_dict = away_lineup_req.json()

    return home_dict, away_dict


def get_lineup(period_df, home_lineup_dict, away_lineup_dict, dataframe):
    """
    this function calculates the lineups for each team at each event and then
    appends it to the current dataframe. This only works for one period at a
    time

    Inputs:
    period_df         - the main game pbp dataframe subsetted to only one period
                        in the game
    home_lineup_dict  - home team lineups API response from that period
    away_lineup_dict  - away team lineups API response from taht period
    dataframe         - full game dataframe. This is passed to get players name from
                        id in case the player didn't have an event in that period.

    Outputs:
    lineup_df     - period_df with each teams lineups calculate and added to the
                    dataframe
    """

    # subsets main dataframe by period and subsets into a home and away subs
    subs_df = period_df[(period_df.event_type_de == "substitution")]
    away_subs = subs_df[pd.isnull(subs_df["visitordescription"]) == 0]
    home_subs = subs_df[pd.isnull(subs_df["homedescription"]) == 0]

    # gets the index of the first sub for home and away to get the players who started
    # the period by subsetting the dataframe to all actions before the first sub for
    # each team
    away_indexes = list(away_subs.index)
    home_indexes = list(home_subs.index)

    # extract the player ids of each lineup
    home_lineups = []
    away_lineups = []

    for lineup in home_lineup_dict["resultSets"][0]["rowSet"]:
        home_lineups.append([lineup[1]])

    for lineup in away_lineup_dict["resultSets"][0]["rowSet"]:
        away_lineups.append([lineup[1]])
    # clean the id strings into a list of ids for each lineup and convert them to ints
    for i, _ in enumerate(home_lineups):
        home_lineups[i] = list(
            map(int, list(filter(None, home_lineups[i][0].split("-"))))
        )

    for i, _ in enumerate(away_lineups):
        away_lineups[i] = list(
            map(int, list(filter(None, away_lineups[i][0].split("-"))))
        )

    # pulls the unique values from the whole period dataframe if there are no subs
    # then it just pulls the unique ids from the from the dataframe itself because
    # the away/home indexes will be an empty list
    try:
        away_starting_line = list(
            period_df[
                (period_df.event_team == period_df["away_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["away_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]
            .loc[: away_indexes[0], :]["player1_id"]
            .unique()
        )
    except IndexError:
        away_starting_line = list(
            period_df[
                (period_df.event_team == period_df["away_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["away_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]["player1_id"].unique()
        )
    try:
        home_starting_line = list(
            period_df[
                (period_df.event_team == period_df["home_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["home_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]
            .loc[: home_indexes[0], :]["player1_id"]
            .unique()
        )
    except IndexError:
        home_starting_line = list(
            period_df[
                (period_df.event_team == period_df["home_team_abbrev"].unique()[0])
                & (~pd.isnull(period_df["player1_name"]))
                & (
                    period_df["player1_team_abbreviation"]
                    == period_df["home_team_abbrev"].unique()[0]
                )
                & (period_df.is_block == 0)
                & (period_df.is_steal == 0)
            ]["player1_id"].unique()
        )
    # if the lineups aren't equal to five players then this code loops through the
    # dataframe for the period and seperates players into sets of whether they
    # started the period or were a sub by looking at each substitution and event
    # it will stop if it reaches 5. If at the end there still aren't five players
    # it then looks at the lineups returned from the lineup api and removes each
    # lineup that has a player that is in the subs set which will return only one
    # lineup and that lineup is set for the starting lineup
    if len(away_starting_line) < 5:
        lineups = set()
        subs = set()
        for i in range(period_df.shape[0]):
            if (
                period_df.iloc[i, :]["event_team"]
                == period_df["away_team_abbrev"].unique()[0]
                and pd.isnull(period_df.iloc[i, :]["player1_name"]) != 1
                and period_df.iloc[i, :]["player1_team_abbreviation"]
                == period_df.iloc[i, :]["away_team_abbrev"]
                and period_df.iloc[i, :]["is_block"] == 0
                and period_df.iloc[i, :]["is_steal"] == 0
            ):
                # if event is not substitution and player is not already in the subs
                # set it adds the player to the lineups set
                if period_df.iloc[i, :]["event_type_de"] != "substitution":
                    if (
                        period_df.iloc[i, :]["player1_id"] != 0
                        and period_df.iloc[i, :]["player1_id"] not in subs
                    ):
                        lineups.add(period_df.iloc[i, :]["player1_id"])
                        # if event is a substitutoin then it adds the
                        # player coming out to lineups set
                        # if it is not in the subs set and adds player
                        # going in to the subs set if they
                        # are not already in the lineups set in case
                        # someone is subbed out and then subbed back in.
                else:
                    if period_df.iloc[i, :]["player2_id"] not in lineups:
                        subs.add(period_df.iloc[i, :]["player2_id"])
                    if period_df.iloc[i, :]["player1_id"] not in subs:
                        lineups.add(period_df.iloc[i, :]["player1_id"])

                if len(lineups) == 5:
                    break
                    # check here to see if we have five players once loop is done if not then
                    # the subs list is checked against the lineups api results and removes every
                    # every lineup combination that has a sub in it which should only leave the
                    # true starting lineup
        if len(lineups) == 5:
            away_ids_names = [
                (x, dataframe[dataframe["player1_id"] == x]["player1_name"].unique()[0])
                for x in lineups
            ]
        else:
            for player in subs:
                for lineup in list(away_lineups):
                    if player in lineup:
                        away_lineups.remove(lineup)
            away_ids_names = [
                (x, dataframe[dataframe["player1_id"] == x]["player1_name"].unique()[0])
                for x in away_lineups[0]
            ]
    else:
        away_ids_names = [
            (x, dataframe[dataframe["player1_id"] == x]["player1_name"].unique()[0])
            for x in away_starting_line
        ]
    # repeating the process for home players
    if len(home_starting_line) < 5:
        lineups = set()
        subs = set()
        for i in range(period_df.shape[0]):
            if (
                period_df.iloc[i, :]["event_team"]
                == period_df["home_team_abbrev"].unique()[0]
                and pd.isnull(period_df.iloc[i, :]["player1_name"]) != 1
                and period_df.iloc[i, :]["player1_team_abbreviation"]
                == period_df.iloc[i, :]["home_team_abbrev"]
                and period_df.iloc[i, :]["is_block"] == 0
                and period_df.iloc[i, :]["is_steal"] == 0
            ):
                if period_df.iloc[i, :]["event_type_de"] != "substitution":
                    if (
                        period_df.iloc[i, :]["player1_id"] != 0
                        and period_df.iloc[i, :]["player1_id"] not in subs
                    ):
                        lineups.add(period_df.iloc[i, :]["player1_id"])
                else:
                    if period_df.iloc[i, :]["player2_id"] not in lineups:
                        subs.add(period_df.iloc[i, :]["player2_id"])
                    if period_df.iloc[i, :]["player1_id"] not in subs:
                        lineups.add(period_df.iloc[i, :]["player1_id"])

                if len(lineups) == 5:
                    break

        if len(lineups) == 5:
            home_ids_names = [
                (x, dataframe[dataframe["player1_id"] == x]["player1_name"].unique()[0])
                for x in lineups
            ]
        else:
            for player in subs:
                for lineup in list(home_lineups):
                    if player in lineup:
                        home_lineups.remove(lineup)
            home_ids_names = [
                (x, dataframe[dataframe["player1_id"] == x]["player1_name"].unique()[0])
                for x in home_lineups[0]
            ]
    else:
        home_ids_names = [
            (x, period_df[period_df["player1_id"] == x]["player1_name"].unique()[0])
            for x in home_starting_line
        ]
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
            and pd.isnull(period_df.iloc[i, :]["visitordescription"]) == 1
        ):
            home_ids_names = [
                ids
                for ids in home_ids_names
                if ids[0] != period_df.iloc[i, :]["player1_id"]
            ]
            home_ids_names.append((period_df.iloc[i, 20], period_df.iloc[i, 21]))
            period_df.iat[i, 63] = home_ids_names[0][0]
            period_df.iat[i, 62] = home_ids_names[0][1]
            period_df.iat[i, 65] = home_ids_names[1][0]
            period_df.iat[i, 64] = home_ids_names[1][1]
            period_df.iat[i, 67] = home_ids_names[2][0]
            period_df.iat[i, 66] = home_ids_names[2][1]
            period_df.iat[i, 69] = home_ids_names[3][0]
            period_df.iat[i, 68] = home_ids_names[3][1]
            period_df.iat[i, 71] = home_ids_names[4][0]
            period_df.iat[i, 70] = home_ids_names[4][1]
            period_df.iat[i, 73] = away_ids_names[0][0]
            period_df.iat[i, 72] = away_ids_names[0][1]
            period_df.iat[i, 75] = away_ids_names[1][0]
            period_df.iat[i, 74] = away_ids_names[1][1]
            period_df.iat[i, 77] = away_ids_names[2][0]
            period_df.iat[i, 76] = away_ids_names[2][1]
            period_df.iat[i, 79] = away_ids_names[3][0]
            period_df.iat[i, 78] = away_ids_names[3][1]
            period_df.iat[i, 81] = away_ids_names[4][0]
            period_df.iat[i, 80] = away_ids_names[4][1]
        elif (
            period_df.iloc[i, :]["event_type_de"] == "substitution"
            and pd.isnull(period_df.iloc[i, :]["homedescription"]) == 1
        ):
            away_ids_names = [
                ids
                for ids in away_ids_names
                if ids[0] != period_df.iloc[i, :]["player1_id"]
            ]
            away_ids_names.append((period_df.iloc[i, 20], period_df.iloc[i, 21]))
            period_df.iat[i, 63] = home_ids_names[0][0]
            period_df.iat[i, 62] = home_ids_names[0][1]
            period_df.iat[i, 65] = home_ids_names[1][0]
            period_df.iat[i, 64] = home_ids_names[1][1]
            period_df.iat[i, 67] = home_ids_names[2][0]
            period_df.iat[i, 66] = home_ids_names[2][1]
            period_df.iat[i, 69] = home_ids_names[3][0]
            period_df.iat[i, 68] = home_ids_names[3][1]
            period_df.iat[i, 71] = home_ids_names[4][0]
            period_df.iat[i, 70] = home_ids_names[4][1]
            period_df.iat[i, 73] = away_ids_names[0][0]
            period_df.iat[i, 72] = away_ids_names[0][1]
            period_df.iat[i, 75] = away_ids_names[1][0]
            period_df.iat[i, 74] = away_ids_names[1][1]
            period_df.iat[i, 77] = away_ids_names[2][0]
            period_df.iat[i, 76] = away_ids_names[2][1]
            period_df.iat[i, 79] = away_ids_names[3][0]
            period_df.iat[i, 78] = away_ids_names[3][1]
            period_df.iat[i, 81] = away_ids_names[4][0]
            period_df.iat[i, 80] = away_ids_names[4][1]
        else:
            period_df.iat[i, 63] = home_ids_names[0][0]
            period_df.iat[i, 62] = home_ids_names[0][1]
            period_df.iat[i, 65] = home_ids_names[1][0]
            period_df.iat[i, 64] = home_ids_names[1][1]
            period_df.iat[i, 67] = home_ids_names[2][0]
            period_df.iat[i, 66] = home_ids_names[2][1]
            period_df.iat[i, 69] = home_ids_names[3][0]
            period_df.iat[i, 68] = home_ids_names[3][1]
            period_df.iat[i, 71] = home_ids_names[4][0]
            period_df.iat[i, 70] = home_ids_names[4][1]
            period_df.iat[i, 73] = away_ids_names[0][0]
            period_df.iat[i, 72] = away_ids_names[0][1]
            period_df.iat[i, 75] = away_ids_names[1][0]
            period_df.iat[i, 74] = away_ids_names[1][1]
            period_df.iat[i, 77] = away_ids_names[2][0]
            period_df.iat[i, 76] = away_ids_names[2][1]
            period_df.iat[i, 79] = away_ids_names[3][0]
            period_df.iat[i, 78] = away_ids_names[3][1]
            period_df.iat[i, 81] = away_ids_names[4][0]
            period_df.iat[i, 80] = away_ids_names[4][1]

    return period_df


def main_scrape(game_id):
    """
    this is the main function that runs and ties all them together. Doing it
    this way so I can better write tests that work on Travis CI due to their
    IP being blacklisted by NBA.com.

    Inputs:
    game_id     - NBA game id of game to be scraped

    Outputs:
    game_df     - pandas dataframe of the play by play
    """
    season_dict = {
        "1": "Pre+Season",
        "2": "Regular+Season",
        "3": "All+Star",
        "4": "Playoffs",
    }
    season_type = season_dict[game_id[2]]
    season = 2000 + int(game_id[3:5])
    v2_dict, pbp_dict = get_pbp_api(
        f"{season - 1}-{str(season)[2:]}", str(season), game_id, season_type
    )
    game_df = scrape_pbp(v2_dict, pbp_dict)
    periods = []
    for period in range(1, game_df["period"].max() + 1):
        home_lineup_dict, away_lineup_dict = get_lineup_api(
            (
                f"{game_df.season.unique()[0]-1}"
                f"-{str(game_df.season.unique()[0])[2:]}"
            ),
            game_df.away_team_id.unique()[0],
            game_df.home_team_id.unique()[0],
            season_type,
            period,
            str(game_df.game_date.unique()[0])[:10],
        )
        periods.append(
            get_lineup(
                game_df[game_df["period"] == period].copy(),
                home_lineup_dict,
                away_lineup_dict,
                game_df,
            )
        )
    game_df = pd.concat(periods).reset_index(drop=True)

    return game_df
