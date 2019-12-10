[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/mcbarlowe/nba_scraper/commits/master)
[![PyPI version](https://badge.fury.io/py/nba-scraper.svg)](https://badge.fury.io/py/nba-scraper)
[![Downloads](https://pepy.tech/badge/nba-scraper)](https://pepy.tech/project/nba-scraper)
[![Build Status](https://travis-ci.org/mcbarlowe/nba_scraper.svg?branch=master)](https://travis-ci.org/mcbarlowe/nba_scraper)
[![codecov](https://codecov.io/gh/mcbarlowe/nba_scraper/branch/master/graph/badge.svg)](https://codecov.io/gh/mcbarlowe/nba_scraper)

# `nba_scraper`

This is a package written in Python to scrape the NBA's api and produce the
play by play of games either in a `csv` file or a `pandas` dataframe. This package
has two main functions `scrape_game` which scrapes an individual game or a list
of specific games, and `scrape_season` which scrapes an entire season of regular
season games.

The scraper goes back to the 1999-2000 season and will pull the play by play along
with who was on the court at the time of each play. Some other various statistics may
be calculated as well.

# Installation

To install this package just type this at the command line:

    pip install nba_scraper

# Usage

## `scrape_game`

The default data format is a pandas dataframe you can change this to csv
with the `data_format` parameter. The default file path is the
users home directory you can change this with the `data_dir` parameter

    import nba_scraper.nba_scraper as ns

    # if you want to return a dataframe
    # you can pass the function a list of strings or integers
    # all nba game ids have two leading zeros but you can omit these
    # to make it easier to create lists of game ids as I add them on
    nba_df = ns.scrape_game([21800001, 21800002])

    # if you want a csv if you don't pass a file path the default is home
    # directory
    ns.scrape_game([21800001, 21800002], data_format='csv', data_dir='file/path')

## `scrape_season`

The `data_format` and `data_dir` key words are used the excat same way as
`scrape_game`. Instead of game ids though, you would pass the season you want
scraped to the function. This season is a four digit year that must be an
integer.

    import nba_scraper.nba_scraper as ns

    #scrape a season
    nba_df = ns.scrape_season(2019)

    # if you want a csv if you don't pass a file path the default is home
    # directory
    ns.scrape_season(2019, data_format='csv', data_dir='file/path')

## `scrape_date_range`

This allows you to scrape all **regular season** games in the date range passed to
the function. As of right now it will not scrape playoff games. Date format must
be passed in the format `YYYY-MM-DD`.

    import nba_scraper.nba_scraper as ns

    #scrape a season
    nba_df = ns.scrape_date_range('2019-01-01', 2019-01-03')

    # if you want a csv if you don't pass a file path the default is home
    # directory
    ns.scrape_date_range('2019-01-01', 2019-01-03', data_format='csv', data_dir='file/path')

# Contact

If you have any troubles or bugs please **open an issue/bug report**. If you have
any improvements/suggestions please **submit a pull request**. If it falls outside
those two areas please feel free to email me at
[matt@barloweanalytics.com](mailto:matt@barloweanalytics.com).




