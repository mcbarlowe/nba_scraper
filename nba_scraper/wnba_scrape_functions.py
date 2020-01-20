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

from nba_scraper.stat_calc_functions import (
    made_shot,
    parse_foul,
    wnba_shot_types,
    wnba_seconds_elapsed,
    calc_points_made,
)

USER_AGENT = {
    "Host": "stats.wnba.com",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
    "Referer": "stats.nba.com",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
}


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

    print(pbp_df.head())


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


def main():
    parse_wnba_pbp("021900122")


if __name__ == "__main__":
    main()
