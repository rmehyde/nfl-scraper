import sqlite3
import linkget
import Player
import queue
import logging

##### LOGGING SETUP ######
logger = logging.getLogger('nfl.data_processor')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(ch)
####### ~~~~~~~~~~ #######

class DataProcessor():

    DEF_SEASONS = [str(i) for i in range(2000, 2019)]
    DEF_SEASON_TYPES = ["Preseason", "Regular Season", "Postseason"]

    CURR_SEASON = '2018'

    def get_player_stats(self, player_url):


    def collect_all_games(self, seasons=DEF_SEASONS, seasontypes=DEF_SEASON_TYPES):
        #first get all player urls
        player_urls = linkget.scrape_all_urls(seasons)
        #lets instantiate a new player object and add it to the queue
        player_q = queue.Queue()
        for url in player_urls:
            player_q.put(Player(url))
