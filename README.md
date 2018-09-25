# nfl-scraper
This project scrapes game logs and player data from nfl.com. Check out the `get_all.py` script for a demonstration of how to pull all points from 2001 to present. If you just want the data you can download a SQLite database containing those records [here](http://rmehyde.com/files/nfl.db) (75MB).

## dataset
The program can produce two tables. The Game Logs table stores statistics for each player for each game played (yards, touchdowns, tackles, etc), along with metadata about the game (date, score, teams, etc.) for about 500,000 data points in 78 dimensions. A unique PlayerID stored with each entry can be used to identify the player referenced. The Players table is indexed by these PlayerIDs and contains records on the position, height, weight, name, etc. of each player.

## modules
The `getter` module contains the `LinkGetter` and `PlayerGetter` classes used to retrieve links to player pages and data from those pages respectively.
The `formatter` module contains the `GameFormatter` and `PlayerFormatter` classes for cleaning and storing raw game log and player profile data.
