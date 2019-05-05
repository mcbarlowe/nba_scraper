'''
Test functions for my nba_scraper module
'''
import pandas as pd
import nba_scraper.nba_scraper as ns

def test_scrape_return():
    print("starting test")
    dataframe = ns.scrape_game([21800001])
    print("testing assertion that dataframe was returned")
    assert isinstance(dataframe, pd.DataFrame)
