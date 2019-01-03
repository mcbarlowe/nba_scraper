'''
Date: 2019-01-02
Contributor: Matthew Barlowe
Twitter: @matt_barlowe
Email: barloweanalytics@gmail.com

This file contains the main functions to scrape and compile the NBA api and
return a CSV file of the pbp for the provided game
'''
import json
import requests
import bs4
import pandas as pd

# have to pass this to the requests function or the api will return a 403 code
user_agent = {'User-agent': 'Mozilla/5.0'}

#this will catalog the shot types recorded in the NBA play by play
#not sure how accurate this is it seems to change for the same shots
shot_type_dict = {58: 'turnaround hook shot', 5: 'layup', 6: 'driving layup',
                  96: 'turnaround bank hook shot', 108: 'cutting dunk shot'
                  79: 'pullup jump shot', 72: 'putback layup', 1: 'jump shot',
                  57: 'driving hook shot', 75: 'driving finger roll layup',
                  76: 'running finger roll layup', 79: '3pt shot', 80: '3pt shot',
                  2: '3pt shot', 3: 'hook shot', 98: 'cutting layup', 67: 'hook bank shot',
                  101: 'driving floating jump shot', 102: 'driving floating bank shot jump shot',
                  73: 'driving reverse layup', 63: 'fadeaway jump shot', 47: 'turnaround jump shot',
                  52: 'alley oop dunk', 97: 'tip layup', 66: 'jump bank shot',
                  50: 'running dunk shot', 41: 'running layup', 93: 'driving bank hook shot',
                  87: 'putback dunk shot', 99:'cutting finger roll layup'
                  }

#this dictionary will categorize the event types that happen in the NBA
#play by play
event_type_dict = {1: 'shot', 2: 'shot', 4: 'rebound', 5: 'turnover',
                   20: 'stoppage: out-of-bounds', 6: 'foul', 3: 'free-throw',
                   8: 'substitution', 12: 'period-start', 10: 'jump-ball',
                   9: 'team-timeout', 18: 'instant-replay', 13: 'period-end',
                   7: 'goal-tending', 0: 'game-end'
                  }
def scrape_pbp(game_id, user_agent):
    '''
    This function scrapes both of the pbp urls and returns a joined/cleaned
    pbp dataframe

    Inputs:
    game_id - integer id of the nba game you want to scrape in question
    user_agent - this is the user agent to pass to the requests function

    Outputs:
    clean_df - final cleaned dataframe
    '''

#hard coding these in for testing purposes
    v2_api_url = 'https://stats.nba.com/stats/playbyplayv2?EndPeriod=10&EndRange=55800&GameID=0021800549&RangeType=2&Season=2018-19&SeasonType=Regular+Season&StartPeriod=1&StartRange=0kk'
    pbp_api_url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2018/scores/pbp/0021800549_full_pbp.json'

# this will be the main url used for the v2 api url once testing is done
#v2 api will contain all the player info for each play in the game while the
#pbp_api_url will contain xy coords for each event
#   v2_api_url = 'https://stats.nba.com/stats/playbyplayv2?EndPeriod=10&EndRange=55800&GameID={game_id}&RangeType=2&Season=2018-19&SeasonType=Regular+Season&StartPeriod=1&StartRange=0kk'
#   pbp_api_url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2018/scores/pbp/{game_id}_full_pbp.json'
# have to pass this to the requests function or the api will return a 403 code
    user_agent = {'User-agent': 'Mozilla/5.0'}
    v2_rep = requests.get(v2_api_url, headers=user_agent)
    v2_dict = v2_rep.json()

#this pulls the v2 stats.nba play by play api
    pbp_v2_headers = v2_dict['resultSets'][0]['headers']
    pbp_v2_data = v2_dict['resultSets'][0]['rowSet']
    pbp_v2_df = pd.DataFrame(pbp_v2_data, columns=pbp_v2_headers)

#this pulls the data.nba api end play by play
    pbp_rep = requests.get(pbp_api_url, headers=user_agent)
    pbp_dict = pbp_rep.json()

#this will be used to concat each quarter from the play by play
    pbp_df_list = []

    for qtr in range(len(pbp_dict['g']['pd'])):
        pbp_df_list.append(pd.DataFrame(pbp_dict['g']['pd'][qtr]['pla']))

    pbp_df = pd.concat(pbp_df_list)

#joining the two dataframes together and only pulling in relavent columns
    clean_df = pbp_v2_df.merge(pbp_df[['evt', 'locX', 'locY', 'hs', 'vs', 'de']],
                               left_on = 'EVENTNUM', right_on='evt')

#TODO Clean time to get a seconds elapsed column

#TODO parse etype column to get summary description of event happening for factoring

#TODO parse mtype column to get all the shot types being taken

#TODO create column whether shot was succesful or not

#TODO create columns to tell which players are on the court for every given event
#probably need to get starting lineups and player ids of each quarter for this and join on period starts
    kkkkkkkk
