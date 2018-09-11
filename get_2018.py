import sqlite3
from LinkGetter import LinkGetter
from PlayerGetter import PlayerGetter
import queue
import logging

##### LOGGING SETUP ######
logger = logging.getLogger('nfl')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(ch)
####### ~~~~~~~~~~ #######

season_types = ('PRE', 'REG', 'POST')
fantasypositions = ("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END", "FIELD_GOAL_KICKER")
positions = ("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END", "DEFENSIVE_LINEMAN", "LINEBACKER",
             "DEFENSIVE_BACK", "KICKOFF_KICKER", "KICK_RETURNER", "PUNTER", "PUNT_RETURNER", "FIELD_GOAL_KICKER")

lg = LinkGetter()
logger.info("Building initial index urls...")
ind_urls = lg.gen_init_urls()
logger.info("Getting next pages...")
ind_urls.extend(lg.get_next_pages(ind_urls))
logger.info("Pulling player URLs from index pages...")
player_urls = lg.get_player_urls(ind_urls)
logger.info("Got %d player pages" % len(player_urls))

# create player objects and pull stats in parallel
player_getters = []
for url in player_urls:
	player_getters.append(PlayerGetter(url))
