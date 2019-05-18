from setuptools import setup
with open("README.md", "r") as fh:
    long_description = fh.read()
setup(
  name = 'nba_scraper',
  packages = ['nba_scraper'],
  version = '0.2.8',
  license='GNU General Public License v3.0',
  description = 'A Python package to scrape the NBA api and return a play by play file',
  long_description=long_description,
  long_description_content_type="text/markdown",
  author = 'Matthew Barlowe',
  author_email = 'matt@barloweanalytics.com',
  url = 'https://github.com/mcbarlowe/nba_scraper',
  download_url = 'https://github.com/mcbarlowe/nba_scraper/archive/v0.2.8.tar.gz',
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
    'Programming Language :: Python :: 3.6',
  ],
)
