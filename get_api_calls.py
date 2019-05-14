'''
script to write api calls to json for unittests
'''
import json
import nba_scraper.scrape_functions as sf


HOME_DICT, AWAY_DICT = sf.get_lineup_api('2018-19', 1610612755, 1610612738,
                                         'Regular+Season', 1, '2018-10-16')
V2_DICT, PBP_DICT = sf.get_pbp_api('201718', '2017',
                                   '0021700001', 'Regular+Season')

with open('home_dict.json', 'w') as home:
    json.dump(HOME_DICT, home)

with open('away_dict.json', 'w') as away:
    json.dump(AWAY_DICT, away)

with open('v2_dict.json', 'w') as v2:
    json.dump(V2_DICT, v2)

with open('pbp_dict.json', 'w') as pbp:
    json.dump(PBP_DICT, pbp)
