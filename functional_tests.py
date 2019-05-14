'''
Functional tests for nba_scraper module
'''
import pandas as pd
import nba_scraper.nba_scraper as ns

def test_scrape_return():
    '''
    this test the scrape_game function for regular season game
    '''
    dataframe = ns.scrape_game([21800001])
    assert isinstance(dataframe, pd.DataFrame)

def test_scrape_playoffs_return():
    '''
    this tests the scrape_game function for playoff game
    '''
    dataframe = ns.scrape_game([41700151])
    assert isinstance(dataframe, pd.DataFrame)

def test_scrape_date_range():
    '''
    this tests the scrape_date_range function to make sure it works
    '''
    dataframe = ns.scrape_date_range('2018-01-01', '2018-01-01')
    assert isinstance(dataframe, pd.DataFrame)
