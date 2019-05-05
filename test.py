'''
Test functions for my nba_scraper module
'''
import pandas as pd
import nba_scraper.nba_scraper as ns

def test_test():
    print("this is a test")

def test_scrape_return():
    dataframe = ns.scrape_game([21800001])
    assert isinstance(dataframe, pd.DataFrame)
