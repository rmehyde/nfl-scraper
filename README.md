# nfl-scraper
This project scrapes game logs and player data from nfl.com. Check out the `get_all.py` script for a demonstration of how to pull all points from 2001 to present. If you just want the data you can download a SQLite database containing those records [here](http://rmehyde.com/files/nfl.db) (75Mb).

## modules
The `getter` module contains the `LinkGetter` and `PlayerGetter` classes used to retrieve links to player pages and data from those pages respectively.
The `formatter` module contains the `GameFormatter` and `PlayerFormatter` classes for cleaning and storing raw game log and player profile data.