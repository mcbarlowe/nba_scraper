# this will catalog the shot types recorded in the NBA play by play
# not sure how accurate this is it seems to change for the same shots
# I think I have them all added but could be wrong.
# TODO get all the shot types from the hackathon data they sent out and update
# this dictionary
import datetime
import time
import requests


# this dictionary will categorize the event types that happen in the NBA
# play by play
EVENT_TYPE_DICT = {
    1: "shot",
    2: "missed_shot",
    4: "rebound",
    5: "turnover",
    20: "stoppage: out-of-bounds",
    6: "foul",
    3: "free-throw",
    8: "substitution",
    12: "period-start",
    10: "jump-ball",
    9: "team-timeout",
    18: "instant-replay",
    13: "period-end",
    7: "goal-tending",
    0: "game-end",
}


def get_date_games(from_date, to_date):
    """
    Get all the game_ids in a valid date range

    Inputs:
    date_from   - Date to scrape from
    date_to     - Date to scrape to

    Outputs:
    game_ids - List of game_ids in range
    """
    game_ids = []
    from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
    to_date = datetime.datetime.strptime(to_date, "%Y-%m-%d")

    # Must check each season in between date range
    for season in range(get_season(from_date), get_season(to_date) + 1):
        url = (
            "http://data.nba.com/data/10s/v2015/json/mobile_teams"
            f"/nba/{season}/league/00_full_schedule.json"
        )
        schedule = requests.get(url).json()
        time.sleep(1)

        for month in schedule["lscd"]:
            if month["mscd"]["g"]:
                # Assume games in order so first in list is first game in month
                cur_month = datetime.datetime.strptime(
                    month["mscd"]["g"][0]["gdte"], "%Y-%m-%d"
                )

                # If first game in month doesn't fall in range no need to check each game for rest of month
                # Convert from_date to beginning of month as that is where cur_month starts
                if (
                    to_date
                    >= cur_month
                    >= (from_date - datetime.timedelta(from_date.day - 1))
                ):
                    for game in month["mscd"]["g"]:
                        # print(game['gdte'])
                        # Check if individual game in date range
                        if (
                            to_date
                            >= datetime.datetime.strptime(game["gdte"], "%Y-%m-%d")
                            >= from_date
                        ):
                            game_ids.append(game["gid"])
    return game_ids


def get_season(date):
    """
    Get Season based on date

    Inputs:
    date  -  time_struct of date

    Outputs:
    season - e.g. 2018
    """
    year = str(date.year)[:4]

    # TODO Refactor this I don't think we need all these if/else statements
    if date > datetime.datetime.strptime("-".join([year, "01-01"]), "%Y-%m-%d"):
        if date < datetime.datetime.strptime("-".join([year, "09-01"]), "%Y-%m-%d"):
            return int(year) - 1
        else:
            return int(year)
    else:
        if date > datetime.datetime.strptime("-".join([year, "07-01"]), "%Y-%m-%d"):
            return int(year)
        else:
            return int(year) - 1


# 2019-05-12 by Matthew Barlowe
# this was the old way to computed the lineups for each event. I'm leaving it in
# because using .iloc it was about 30 times faster than my refactor. Using .iat
# it seems to be about the same if not a little slower and I'm hoping someday I
# actually figure out why that is.
"""
def get_lineups(dataframe, season_type):
    for each player on the court for each event of the play by play

    Inputs:
    dataframe     - the nba dataframe that's been computed up to this point
    season_type   - type of game to be scraped: Regular Season, Playoffs, etc.

    Outputs:
    lineup_df     - the dataframe with lineups computed

    periods = []
    for period in range(1, dataframe['period'].max() + 1):
        start = time.time()
        #subsets main dataframe by period and subsets into a home and away subs
        period_df = dataframe[dataframe['period'] == period].reset_index()
        subs_df = period_df[(period_df.event_type_de == 'substitution')]
        away_subs = subs_df[pd.isnull(subs_df['visitordescription']) == 0]
        home_subs = subs_df[pd.isnull(subs_df['homedescription']) == 0]
        #gets the index of the first sub for home and away to get the players who started
        #the period by subsetting the dataframe to all actions before the first sub for
        #each team
        away_indexes = list(away_subs.index)
        home_indexes = list(home_subs.index)

        start = time.time()
        home_lineup_dict, away_lineup_dict = \
                get_lineup_api((f'{period_df.season.unique()[0]-1}'
                                f'-{str(period_df.season.unique()[0])[2:]}'),
                               period_df.away_team_id.unique()[0],
                               period_df.home_team_id.unique()[0],
                               season_type, period,
                               str(period_df.game_date.unique()[0])[:10])

        start = time.time()
        #extract the player ids of each lineup
        home_lineups = []
        away_lineups = []

        for lineup in home_lineup_dict['resultSets'][0]['rowSet']:
            home_lineups.append([lineup[1]])

        for lineup in away_lineup_dict['resultSets'][0]['rowSet']:
            away_lineups.append([lineup[1]])

        #clean the id strings into a list of ids for each lineup and convert them to ints
        for i, _ in enumerate(home_lineups):
            home_lineups[i] = list(map(int, list(filter(None, home_lineups[i][0].split('-')))))

        for i, _ in enumerate(away_lineups):
            away_lineups[i] = list(map(int, list(filter(None, away_lineups[i][0].split('-')))))

        #pulls the unique values from the whole period dataframe if there are no subs
        #then it just pulls the unique ids from the from the dataframe itself because
        #the away/home indexes will be an empty list
        start = time.time()
        try:
            away_starting_line = list(
                period_df[(period_df.event_team ==
                           period_df['away_team_abbrev'].unique()[0])
                          & (~pd.isnull(period_df['player1_name']))
                          & (period_df['player1_team_abbreviation'] ==
                             period_df['away_team_abbrev'].unique()[0])
                          & (period_df.is_block == 0)
                          & (period_df.is_steal == 0)]
                .loc[:away_indexes[0], :]['player1_id'].unique())
        except IndexError:
            away_starting_line = list(
                period_df[(period_df.event_team ==
                           period_df['away_team_abbrev'].unique()[0])
                          & (~pd.isnull(period_df['player1_name']))
                          & (period_df['player1_team_abbreviation'] ==
                             period_df['away_team_abbrev'].unique()[0])
                          & (period_df.is_block == 0)
                          & (period_df.is_steal == 0)]['player1_id'].unique())
        try:
            home_starting_line = list(
                period_df[(period_df.event_team ==
                           period_df['home_team_abbrev'].unique()[0])
                          & (~pd.isnull(period_df['player1_name']))
                          & (period_df['player1_team_abbreviation'] ==
                             period_df['home_team_abbrev'].unique()[0])
                          & (period_df.is_block == 0)
                          & (period_df.is_steal == 0)]
                .loc[:home_indexes[0], :]['player1_id'].unique())
        except IndexError:
            home_starting_line = list(
                period_df[(period_df.event_team ==
                           period_df['home_team_abbrev'].unique()[0])
                          & (~pd.isnull(period_df['player1_name']))
                          & (period_df['player1_team_abbreviation'] ==
                             period_df['home_team_abbrev'].unique()[0])
                          & (period_df.is_block == 0)
                          & (period_df.is_steal == 0)]['player1_id'].unique())

        # if the lineups aren't equal to five players then this code loops through the
        # dataframe for the period and seperates players into sets of whether they
        # started the period or were a sub by looking at each substitution and event
        # it will stop if it reaches 5. If at the end there still aren't five players
        # it then looks at the lineups returned from the lineup api and removes each
        # lineup that has a player that is in the subs set which will return only one
        # lineup and that lineup is set for the starting lineup
        start = time.time()
        if len(away_starting_line) < 5:
            lineups = set()
            subs = set()
            for i in range(period_df.shape[0]):
                if (
                        period_df.iloc[i, :]['event_team'] ==
                        period_df['away_team_abbrev'].unique()[0] and
                        pd.isnull(period_df.iloc[i, :]['player1_name']) != 1 and
                        period_df.iloc[i, :]['player1_team_abbreviation'] ==
                        period_df.iloc[i, :]['away_team_abbrev'] and
                        period_df.iloc[i, :]['is_block'] == 0 and
                        period_df.iloc[i, :]['is_steal'] == 0):
                    # if event is not substitution and player is not already in the subs
                    # set it adds the player to the lineups set
                    if period_df.iloc[i, :]['event_type_de'] != 'substitution':
                        if (period_df.iloc[i, :]['player1_id'] != 0 and
                                period_df.iloc[i, :]['player1_id'] not in subs):
                            lineups.add(period_df.iloc[i, :]['player1_id'])
                            # if event is a substitutoin then it adds the
                            # player coming out to lineups set
                            # if it is not in the subs set and adds player
                            # going in to the subs set if they
                            # are not already in the lineups set in case
                            # someone is subbed out and then subbed back in.
                    else:
                        if period_df.iloc[i, :]['player2_id'] not in lineups:
                            subs.add(period_df.iloc[i, :]['player2_id'])
                        if period_df.iloc[i, :]['player1_id'] not in subs:
                            lineups.add(period_df.iloc[i, :]['player1_id'])

                    if len(lineups) == 5:
                        break
                        # check here to see if we have five players once loop is done if not then
                        # the subs list is checked against the lineups api results and removes every
                        # every lineup combination that has a sub in it which should only leave the
                        # true starting lineup
            if len(lineups) == 5:
                away_ids_names = [
                    (x, dataframe[dataframe['player1_id'] == x]
                     ['player1_name'].unique()[0]) for x in lineups]
            else:
                for player in subs:
                    for lineup in list(away_lineups):
                        if player in lineup:
                            away_lineups.remove(lineup)
                away_ids_names = [
                    (x, dataframe[dataframe['player1_id'] == x]
                     ['player1_name'].unique()[0]) for x in away_lineups[0]]
        else:
            away_ids_names = [
                (x, dataframe[dataframe['player1_id'] == x]
                 ['player1_name'].unique()[0]) for x in away_starting_line]
        start = time.time()
        # repeating the process for home players
        if len(home_starting_line) < 5:
            lineups = set()
            subs = set()
            for i in range(period_df.shape[0]):
                if (
                        period_df.iloc[i, :]['event_team'] ==
                        period_df['home_team_abbrev'].unique()[0] and
                        pd.isnull(period_df.iloc[i, :]['player1_name']) != 1 and
                        period_df.iloc[i, :]['player1_team_abbreviation'] ==
                        period_df.iloc[i, :]['home_team_abbrev'] and
                        period_df.iloc[i, :]['is_block'] == 0 and
                        period_df.iloc[i, :]['is_steal'] == 0):
                    if period_df.iloc[i, :]['event_type_de'] != 'substitution':
                        if (period_df.iloc[i, :]['player1_id'] != 0 and
                                period_df.iloc[i, :]['player1_id'] not in subs):
                            lineups.add(period_df.iloc[i, :]['player1_id'])
                    else:
                        if period_df.iloc[i, :]['player2_id'] not in lineups:
                            subs.add(period_df.iloc[i, :]['player2_id'])
                        if period_df.iloc[i, :]['player1_id'] not in subs:
                            lineups.add(period_df.iloc[i, :]['player1_id'])

                    if len(lineups) == 5:
                        break

            if len(lineups) == 5:
                home_ids_names = [
                    (x, dataframe[dataframe['player1_id'] == x]
                     ['player1_name'].unique()[0]) for x in lineups]
            else:
                for player in subs:
                    for lineup in list(home_lineups):
                        if player in lineup:
                            home_lineups.remove(lineup)
                home_ids_names = [
                    (x, dataframe[dataframe['player1_id'] == x]
                     ['player1_name'].unique()[0]) for x in home_lineups[0]]
        else:
            home_ids_names = [
                (x, period_df[period_df['player1_id'] == x]
                 ['player1_name'].unique()[0]) for x in home_starting_line]

        start = time.time()
        # creating columns to populate with players on the court
        period_df['home_player_1'] = ''
        period_df['home_player_1_id'] = ''
        period_df['home_player_2'] = ''
        period_df['home_player_2_id'] = ''
        period_df['home_player_3'] = ''
        period_df['home_player_3_id'] = ''
        period_df['home_player_4'] = ''
        period_df['home_player_4_id'] = ''
        period_df['home_player_5'] = ''
        period_df['home_player_5_id'] = ''
        period_df['away_player_1'] = ''
        period_df['away_player_1_id'] = ''
        period_df['away_player_2'] = ''
        period_df['away_player_2_id'] = ''
        period_df['away_player_3'] = ''
        period_df['away_player_3_id'] = ''
        period_df['away_player_4'] = ''
        period_df['away_player_4_id'] = ''
        period_df['away_player_5'] = ''
        period_df['away_player_5_id'] = ''
        # add players to the columns by looping through the dataframe and putting the
        # players in for each row using the starting lineup list. If there is a
        # substitution event then the player coming on replaces the player going off in
        # the list this is done for the whole period
        start = time.time()

        for i in range(period_df.shape[0]):
            if (period_df.iloc[i, :]['event_type_de'] == 'substitution' and
                    pd.isnull(period_df.iloc[i, :]['visitordescription']) == 1):
                home_ids_names = [ids for ids in home_ids_names if
                                  ids[0] != period_df.iloc[i, :]['player1_id']]
                home_ids_names.append((period_df.iloc[i, 21], period_df.iloc[i, 22]))

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
            elif (period_df.iloc[i, :]['event_type_de'] == 'substitution' and
                  pd.isnull(period_df.iloc[i, :]['homedescription']) == 1):
                away_ids_names = [ids for ids in away_ids_names if
                                  ids[0] != period_df.iloc[i, :]['player1_id']]
                away_ids_names.append((period_df.iloc[i, 21], period_df.iloc[i, 22]))
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
                period_df.iloc[i, 63] = home_ids_names[0][0]
                period_df.iloc[i, 62] = home_ids_names[0][1]
                period_df.iloc[i, 65] = home_ids_names[1][0]
                period_df.iloc[i, 64] = home_ids_names[1][1]
                period_df.iloc[i, 67] = home_ids_names[2][0]
                period_df.iloc[i, 66] = home_ids_names[2][1]
                period_df.iloc[i, 69] = home_ids_names[3][0]
                period_df.iloc[i, 68] = home_ids_names[3][1]
                period_df.iloc[i, 71] = home_ids_names[4][0]
                period_df.iloc[i, 70] = home_ids_names[4][1]
                period_df.iloc[i, 73] = away_ids_names[0][0]
                period_df.iloc[i, 72] = away_ids_names[0][1]
                period_df.iloc[i, 75] = away_ids_names[1][0]
                period_df.iloc[i, 74] = away_ids_names[1][1]
                period_df.iloc[i, 77] = away_ids_names[2][0]
                period_df.iloc[i, 76] = away_ids_names[2][1]
                period_df.iloc[i, 79] = away_ids_names[3][0]
                period_df.iloc[i, 78] = away_ids_names[3][1]
                period_df.iloc[i, 81] = away_ids_names[4][0]
                period_df.iloc[i, 80] = away_ids_names[4][1]
        periods.append(period_df)

    # concatting all the periods into one dataframe and returning it
    lineup_df = pd.concat(periods).reset_index(drop=True)
    return lineup_df
"""
