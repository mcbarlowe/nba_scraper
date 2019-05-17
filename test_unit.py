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
    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)
    assert isinstance(game_df, pd.DataFrame)

def test_lineup_scrape():
    '''
    this will test the get_lineups function to make sure it is returning
    a dataframe of the pbp with the the players on the floor at each event
    '''

    with open('test_files/home_dict.json', 'r') as h_dict:
        home_lineups = json.load(h_dict)
    with open('test_files/away_dict.json', 'r') as a_dict:
        away_lineups = json.load(a_dict)

    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
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

def test_made_shot():
    '''
    test the made_shot funciton to make sure it is calculating correctly
    '''

    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)

    assert sf.made_shot(game_df.iloc[20, :].copy()) == 1
    assert sf.made_shot(game_df.iloc[13, :].copy()) == 1
    assert sf.made_shot(game_df.iloc[23, :].copy()) == 0
    assert sf.made_shot(game_df.iloc[123, :].copy()) == 0

def test_parse_foul():
    '''
    test for the parse_foul function
    '''

    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)
    assert sf.parse_foul(game_df.iloc[12, :].copy()) == '3 second'
    assert sf.parse_foul(game_df.iloc[108, :].copy()) == 'shooting'
    assert sf.parse_foul(game_df.iloc[125, :].copy()) == 'charge'
    assert sf.parse_foul(game_df.iloc[131, :].copy()) == 'loose_ball'
    assert sf.parse_foul(game_df.iloc[155, :].copy()) == 'personal'
    assert sf.parse_foul(game_df.iloc[256, :].copy()) == 'technical'

def test_shot_types():
    '''
    function to test the parse_shot_types function in scrape_functions
    '''
    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)

    assert sf.parse_shot_types(game_df.iloc[3, :].copy()) == 'layup'
    assert sf.parse_shot_types(game_df.iloc[7, :].copy()) == 'jump'
    assert sf.parse_shot_types(game_df.iloc[18, :].copy()) == 'dunk'
    assert sf.parse_shot_types(game_df.iloc[119, :].copy()) == 'hook'
    assert sf.parse_shot_types(game_df.iloc[119, :].copy()) == 'hook'
    assert sf.parse_shot_types(game_df.iloc[121, :].copy()) == 'free'

def test_seconds_elapsed():
    '''
    test create_seconds_elapsed function in scrape_functions
    '''
    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)

    assert sf.create_seconds_elapsed(game_df.iloc[8, :].copy()) == 61
    assert sf.create_seconds_elapsed(game_df.iloc[368, :].copy()) == 2197
    assert sf.create_seconds_elapsed(game_df.iloc[233, :].copy()) == 1450
    assert sf.create_seconds_elapsed(game_df.iloc[117, :].copy()) == 735

def test_calc_points():
    '''
    test calc_points_made function
    '''
    with open('test_files/v2_dict.json', 'r') as v2_file:
        v2_dict = json.load(v2_file)
    with open('test_files/pbp_dict.json', 'r') as pbp:
        pbp_dict = json.load(pbp)

    game_df = sf.scrape_pbp(v2_dict, pbp_dict)
    assert sf.calc_points_made(game_df.iloc[110, :].copy()) == 1
    assert sf.calc_points_made(game_df.iloc[195, :].copy()) == 2
    assert sf.calc_points_made(game_df.iloc[151, :].copy()) == 3



