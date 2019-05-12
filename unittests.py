'''
unit tests for the nba_scraper module
'''
import pandas as pd
import nba_scraper.scrape_functions as sf

def test_master_scrape():
    '''
    test of the main_scrape function using pre downloaded JSON
    '''
