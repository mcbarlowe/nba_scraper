'''
These are integration tests of the API calls to the NBA api
'''
import nba_scraper.scrape_functions as ns

def test_pbp_request():
    '''
    this test checks to make sure the API is returning a proper JSON string
    that can be converted into a python dictionary
    '''
    v2_dict, pbp_dict = ns.get_pbp_api('201718', '2017',
                                       '0021700001', 'Regular+Season')
    assert isinstance(v2_dict, dict)
    assert isinstance(pbp_dict, dict)

def test_lineup_request():
    '''
    test checks to make sure lineup API is returning a proper JSON string
    that can be converted into a python dictionary
    '''
    home_dict, away_dict = ns.get_lineup_api('2017-18', 1610612750, 1610612739,
                                             'Regular+Season', 1, '2017-10-17')
    assert isinstance(home_dict, dict)
    assert isinstance(away_dict, dict)
