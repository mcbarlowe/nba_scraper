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
event_type_dict = {1: 'shot', 2: 'missed_shot', 4: 'rebound', 5: 'turnover',
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

#pulling the home and away team abbreviations and the game date
    gcode = pbp_dict['g']['gcode'].split('/')
    date = gcode[0]
    teams = gcode[1]
    home_team = teams[3:]
    away_team = teams[:3]
    pbp_df = pd.concat(pbp_df_list)

#joining the two dataframes together and only pulling in relavent columns
    clean_df = pbp_v2_df.merge(pbp_df[['evt', 'locX', 'locY', 'hs', 'vs', 'de']],
                               left_on = 'EVENTNUM', right_on='evt')

#add date and team abbrev columns to dataframe
    clean_df.loc[:, 'home_team_abbrev'] = home_team_abbrev
    clean_df.loc[:, 'away_team_abbrev'] = away_team_abbrev
    clean_df.loc[:, 'game_date'] = date

#code to properly get the team ids as the scientific notation cuts off some digits
    home_team_id = clean_df[clean_df.PLAYER1_TEAM_ABBREVIATION == home_team_abbrev].PLAYER1_TEAM_ID.astype(int).unique()
    away_team_id = clean_df[clean_df.PLAYER1_TEAM_ABBREVIATION == away_team_abbrev].PLAYER1_TEAM_ID.astype(int).unique()
    clean_df.loc[:, 'home_team_id'] = home_team_id
    clean_df.loc[:, 'away_team_id'] = away_team_id

#create an event team colum
    clean_df['event_team'] = np.where(clean_df['HOMEDESCRIPTION'].isnull(),
                                    clean_df['home_team_abbrev'], clean_df['away_team_abbrev'])

#create and event type description column
    clean_df['event_type_de'] = clean_df[['etype']].replace({'etype': event_type_dict})

#create an event team colum
    clean_df['event_team'] = np.where(clean_df['HOMEDESCRIPTION'].isnull(),
                                      clean_df['home_team_abbrev'],
                                      clean_df['away_team_abbrev'])

#create column whether shot was succesful or not
    def made_shot(row):
    '''
    function to determine whether shot was made or missed

    Input:
    row - pandas row

    Output:
    shot_made - binary variable
    '''

    if row['event_type_de'] == 'shot':
        return 1
    elif row['event_type_de'] == 'missed_shot':
        return 0
    elif (row['event_type_de'] == 'free-throw') & ('Missed' in row['de']):
        return 0
    elif (row['event_type_de'] == 'free-throw') & ('Missed' not in row['de']):
        return 1
    else:
        return np.nan

    clean_df['shot_made'] = clean_df.apply(made_shot, axis=1)

#create a column that says whether the shot was blocked or not
    clean_df['is_block'] = np.where(clean_df.HOMEDESCRIPTION.str.contains('BLOCK') |
                                    clean_df.VISITORDESCRIPTION.str.contains('BLOCK'),
                                    1, 0)

#TODO Clean time to get a seconds elapsed column


#TODO parse mtype column to get all the shot types being taken


#TODO create columns to tell which players are on the court for every given event
#probably need to get starting lineups and player ids of each quarter for this and join on period starts

#this is the api endpoint to get the starting lineup for each respective
#period. It takes a team id in the OpponentTeamID variable, a period number
#for the period variable this can be 1-4 or 5, 6, 7 if OT was played. A season
#variable in the form of YYYY-YY so for the 2019 season it would be 2018-19.
#It aslo will need a start and end date for the lineups. Since this scrape will
#only want the line ups for the game played on said date we are scraping the
#DateFrom and DateTo variables will be the same. These will be to be in the form
#YYYY-MM-DD
#TODO create the variables from the play by play dataframe needed for the
    lineup_api = ('https://stats.nba.com/stats/leaguedashlineups?Conference=&'
                 f'DateFrom={start_date}&DateTo={end_date}&Division=&'
                 'GameSegment=&GroupQuantity=5&LastNGames=0&LeagueID=&Location=&'
                 f'MeasureType=Base&Month=0&OpponentTeamID={team_id}&Outcome=&PORound=&'
                 f'PaceAdjust=N&PerMode=Totals&Period={period}&PlusMinus=N&Rank=N&'
                 f'Season={season}&SeasonSegment=&SeasonType=Regular+'
                 'Season&ShotClockRange=&TeamID=&VsConference=&VsDivision=')
