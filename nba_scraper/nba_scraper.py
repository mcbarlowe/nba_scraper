import os
import pandas as pd
import nba_scraper.scrape_functions as sf


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
#checks to make sure correct format is passed
    possible_formats = ['pandas', 'csv']
    if data_format.lower() not in possible_formats:
        print(f"You passed {data_format} to scrape_game function as a data format.\n"
               "This is an unaccepted format. Please either pass 'pandas' or 'csv'.\n")

    scraped_games = []
    for game in game_ids:
        print(f"Scraping game id: 00{game}")
        scraped_games.append(sf.scrape_pbp(f"00{game}"))

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

    scraped_games = []
    possible_formats = ['pandas', 'csv']
    if data_format.lower() not in possible_formats:
        print(f"You passed {data_format} to scrape_game function as a data format.\n"
               "This is an unaccepted format. Please either pass 'pandas' or 'csv'.\n")
    game_ids = list(range(int(f"2{season-2001}00001"), int(f"2{season-2001}01231")))

    for game in game_ids:
        print(f"Scraping game id: 00{game}")
        scraped_games.append(sf.scrape_pbp(f"00{game}"))

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
