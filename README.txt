usage: pubget_scrape.py [-h] pmcids_file output_dir

Get articles from PMC and use pubget to extract stereotactic coordinates. This
downloads articles from the PMC website -- not the API. It is meant to get
coordinates for articles not in the PMC Open Access subset (which are not
available in XML form through the API). It is a scratch proof of concept and
offers very limited functionality and error handling. As scraping HTML pages
is forbidden by PMC this is not meant to be widely distributed nor used for
large numbers of articles. To avoid overloading the PMC website we wait at
least 2s and 10s on average between requests.

positional arguments:
  pmcids_file  File containing PMCIDs to download, one per line (without the
               'PMC' prefix).
  output_dir   Directory where to store outputs (will be created if
               necessary).

options:
  -h, --help   show this help message and exit
