'''
unit tests for the nba_scraper module
'''
from datetime import datetime
import json
import pandas as pd
import nba_scraper.scrape_functions as sf

def test_pbp_scrape():
    '''
    test of the main_scrape function using pre downloaded JSON
    '''
    with open('v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)
    assert isinstance(game_df, pd.DataFrame)

def test_lineup_scrape():
    '''
    this will test the get_lineups function to make sure it is returning
    a dataframe of the pbp with the the players on the floor at each event
    '''

    with open('home_dict.json', 'r') as h_dict:
        home_lineups = json.load(h_dict)
    with open('away_dict.json', 'r') as a_dict:
        away_lineups = json.load(a_dict)

    with open('v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)

    game_df = sf.get_lineup(game_df[game_df['period'] == 1].copy(), home_lineups,
                            away_lineups, game_df)

    assert isinstance(game_df, pd.DataFrame)

def test_get_season():
    '''
    tests the get get_season function in scraper_functions to make sure it
    is returning the correct date
    '''
    assert sf.get_season(datetime.strptime('2018-09-01', "%Y-%m-%d")) == 2018
    assert sf.get_season(datetime.strptime('2018-04-01', "%Y-%m-%d")) == 2017
    assert sf.get_season(datetime.strptime('2018-01-01', "%Y-%m-%d")) == 2017
