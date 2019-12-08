"""
These are integration tests of the API calls to the NBA api
"""
import nba_scraper.scrape_functions as sf


def test_pbp_request():
    """
    this test checks to make sure the API is returning a proper JSON string
    that can be converted into a python dictionary
    """
    v2_dict = sf.get_pbp_api("0021700001")
    assert isinstance(v2_dict, dict)


def test_lineup_request():
    """
    test checks to make sure lineup API is returning a proper JSON string
    that can be converted into a python dictionary
    """
    lineups = sf.get_lineup_api("0021800404", 1)
    assert isinstance(lineups, dict)


def test_get_date_games():
    """
    test to determine that the function to get game ids by date is returning
    the proper game ids
    """
    correct_ids_1 = ["0021800001", "0021800002"]
    correct_ids_2 = ["0021800014", "0021800015", "0021800016"]
    correct_ids_3 = [
        "0021800163",
        "0021800164",
        "0021800165",
        "0021800166",
        "0021800167",
        "0021800168",
        "0021800169",
        "0021800170",
        "0021800171",
        "0021800172",
        "0021800173",
    ]
    game_ids_1 = sf.get_date_games("2018-10-16", "2018-10-16")
    game_ids_2 = sf.get_date_games("2018-10-18", "2018-10-18")
    game_ids_3 = sf.get_date_games("2018-11-08", "2018-11-09")
    assert game_ids_1 == correct_ids_1
    assert game_ids_2 == correct_ids_2
    assert game_ids_3 == correct_ids_3
