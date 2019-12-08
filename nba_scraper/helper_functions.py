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
