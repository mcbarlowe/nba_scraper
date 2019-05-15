import os
from datetime import datetime
import pandas as pd
import nba_scraper.scrape_functions as sf


def check_format(data_format):
    '''
    Check that the format for the data is either csv or pandas

    Inputs:
    data_format - String of format

    Outputs:
    '''
    possible_formats = ['pandas', 'csv']
    if data_format.lower() not in possible_formats:
        print(f"You passed {data_format} to scrape_game function as a data format.\n"
              "This is an unaccepted format. Please either pass 'pandas' or 'csv'.\n")


def check_valid_dates(from_date, to_date):
    '''
    Check if it's a valid date range. If not raise ValueError

    Inputs:
    date_from   - Date to scrape form
    date_to     - Date to scrape to

    Outputs:
    '''
    try:
        if datetime.strptime(to_date, "%Y-%m-%d") < datetime.strptime(from_date, "%Y-%m-%d"):
            raise ValueError("Error: The second date input is earlier than the first one")
    except ValueError:
        raise ValueError(
            "Error: Incorrect format given for dates. They must be given like 'yyyy-mm-dd' (ex: '2016-10-01').")


def scrape_date_range(date_from, date_to, data_format='pandas', data_dir=f"{os.environ['HOME']}/nbadata.csv"):
    '''
    Function scrapes all `regular-season` nba games between two dates

    Inputs:
    date_from   - Date to scrape from
    date_to     - Date to scrape to
    data_format - the format of the data the user wants returned. This is either
                  a pandas dataframe or a csv file
    data_dir    - a filepath which to write the csv file if that option is chosen.
                  If no filepath is passed then it will attempt to write to the
                  user's home directory

    Outputs:
    nba_df     - If pandas is chosen then this function will
                 return this pandas dataframe object. If csv then
                 a csv file will be written but None will be returned
    '''
    check_format(data_format)
    check_valid_dates(date_from, date_to)

    game_ids = sf.get_date_games(date_from, date_to)
    scraped_games = []

    for game in game_ids:
        print(f"Scraping game id: {game}")
        scraped_games.append(sf.main_scrape(game))

    if data_format == 'pandas':
        return pd.concat(scraped_games)
    else:
        pd.concat(scraped_games).to_csv(data_dir, index=False)
        return None


def scrape_game(game_ids, data_format='pandas', data_dir=f"{os.environ['HOME']}/nbadata.csv"):
    '''
    function scrapes nba games and returns them in the data format requested
    by the user.

    Inputs:
    game_ids    - list of nba game ids to scrape
    data_format - the format of the data the user wants returned. This is either
                  a pandas dataframe or a csv file
    data_dir    - a filepath which to write the csv file if that option is chosen.
                  If no filepath is passed then it will attempt to write to the
                  user's home directory

    Outputs:
    nba_df     - If pandas is chosen then this function will
                 return this pandas dataframe object. If csv then
                 a csv file will be written but None will be returned
    '''
    check_format(data_format)

    scraped_games = []
    for game in game_ids:
        print(f"Scraping game id: 00{game}")
        scraped_games.append(sf.main_scrape(f"00{game}"))

    nba_df = pd.concat(scraped_games)

    if data_format == 'pandas':
        return nba_df
    else:
        nba_df.to_csv(data_dir, index=False)
        return None


def scrape_season(season, data_format='pandas', data_dir=f"{os.environ['HOME']}/"):
    '''
    This function scrapes and entire season and either returns it as a pandas
    dataframe or writes it to file as a csv file

    Inputs:
    season      - season to be scraped must be an integer
    data_format - the format of the data the user wants returned. This is either
                  a pandas dataframe or a csv file
    data_dir    - a filepath which to write the csv file if that option is chosen.
                  If no filepath is passed then it will attempt to write to the
                  user's home directory

    Outputs:
    nba_df     - If pandas is chosen then this function will
                 return this pandas dataframe object. If csv then
                 a csv file will be written but None will be returned
    '''
    check_format(data_format)

    scraped_games = []
    game_ids = list(range(int(f"2{season-2001}00001"), int(f"2{season-2001}01231")))

    for game in game_ids:
        print(f"Scraping game id: 00{game}")
        scraped_games.append(sf.main_scrape(f"00{game}"))

    nba_df = pd.concat(scraped_games)

    if data_format == 'pandas':
        return nba_df
    else:
        print
        nba_df.to_csv(f"{data_dir}/nba{season}.csv", index=False)
        return None


def main():
    return None


if __name__ == '__main__':
    main()
