from distutils.core import setup
setup(
  name = 'nba_scraper',
  packages = ['nba_scraper'],
  version = '0.1',
  license='GNU General Public License v3.0',
  description = 'A Python package to scraper the NBA api and return a play by play file',
  author = 'Matthew Barlowe',
  author_email = 'matt@barloweanalytics.com',
  url = 'https://https://github.com/mcbarlowe/nba_scraper',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/user/reponame/archive/v_01.tar.gz',    # I explain this later on
  keywords = ['basketball', 'NBA', 'scraper'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'requests',
          'pandas',
          'numpy'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Scientists/Analysts',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: GNU General Public License v3.0',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
