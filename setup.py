from distutils.core import setup
setup(
  name = 'nba_scraper',
  packages = ['nba_scraper'],
  version = '0.1',
  license='GNU General Public License v3.0',
  description = 'A Python package to scraper the NBA api and return a play by play file',
  author = 'Matthew Barlowe',
  author_email = 'matt@barloweanalytics.com',
  url = 'https://https://github.com/mcbarlowe/nba_scraper',
  download_url = 'https://github.com/mcbarlowe/nba_scraper/archive/v_01.tar.gz',
  keywords = ['basketball', 'NBA', 'scraper'],
  install_requires=[
          'requests',
          'pandas',
          'numpy'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Science/Research',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
