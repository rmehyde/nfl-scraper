import sqlite3
import getter
import queue
import logging
import pandas as pd

##### LOGGING SETUP ######
logger = logging.getLogger('nfl')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
fh = logging.FileHandler('nfl.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)
####### ~~~~~~~~~~ #######

season_types = ('PRE', 'REG', 'POST')
fantasypositions = ("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END", "FIELD_GOAL_KICKER")
positions = ("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END", "DEFENSIVE_LINEMAN", "LINEBACKER",
             "DEFENSIVE_BACK", "KICKOFF_KICKER", "KICK_RETURNER", "PUNTER", "PUNT_RETURNER", "FIELD_GOAL_KICKER")

lg = getter.LinkGetter(season_start=2018, season_end=2018, season_types=("PRE", "REG", "POST"))
logger.info("Building initial index urls...")
ind_urls = lg.gen_init_urls()
logger.info("Getting next pages...")
ind_urls.extend(lg.get_next_pages(ind_urls))
logger.info("Pulling player URLs from index pages...")
player_urls = lg.get_player_urls(ind_urls)
logger.info("Got %d player pages" %len(player_urls))

# create player objects and pull stats in parallel
player_getters = []
for url in player_urls:
	player_getters.append(getter.PlayerGetter(url))

player_data = []
game_data = []
# pull stats
for pg in player_getters[:10]:
	player_gls = pg.get()
	logger.debug("got player %s" %pg.player_name)
	player = {}
	player["PlayerID"] = pg.player_id
	player["PlayerNum"] = pg.player_num
	player["Name"] = pg.player_name
	player["PrettyName"] = pg.player_prettyname
	player["Position"] = pg.position
	player["Height"] = pg.height
	player["Weight"] = pg.weight
	player["Birthday"] = pg.birthday
	player["JerseyNum"] = pg.jerseynum
	player_data.append(player)
	game_data.extend(player_gls)

game_df = pd.DataFrame(game_data)
player_df = pd.DataFrame(player_data)
game_df.to_pickle("data/gamelogs.pkl")
player_df.to_pickle("data/players.pkl")