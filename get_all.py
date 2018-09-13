import getter
import logging
import pandas as pd
import pickle
import queue
import threading
import time
import urllib

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

def get_links():
    lg = getter.LinkGetter(season_start=2001, season_end=2018, season_types=("PRE", "REG", "POST"))
    logger.info("Building initial index urls...")
    ind_urls = lg.gen_init_urls()
    logger.info("Getting next pages...")
    ind_urls.extend(lg.get_next_pages(ind_urls))
    logger.info("Pulling player URLs from index pages...")
    player_urls = lg.get_player_urls(ind_urls)
    logger.info("Got %d player pages" %len(player_urls))

    # save players
    with open('data/player_urls.pkl', 'wb') as f:
        f.write(pickle.dumps(player_urls))

def player_worker(q, player_data, game_data, i):
    while True:
        pg = q.get()
        try:
            player_gls = pg.get()
        # if we fail to connect wait 10s and try again
        except urllib.error.URLError:
            logger.info("Thread %d encountered URLError, sleeping 20s and trying again..." %i)
            time.sleep(20)
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
        player_data[i].append(player)
        game_data[i].extend(player_gls)


def get_data(num_threads):
    # read player urls file from get_links
    with open('data/player_urls.pkl', 'rb') as f:
        player_urls = pickle.loads(f.read())

    # initialize a queue of playergetter objects
    q = queue.Queue()
    for url in player_urls:
        q.put(getter.PlayerGetter(url, season_start=2001, season_end=2018))

    # they say lists are thread safe but lets let everyone work on their own and flatten after
    player_data = [[]] * num_threads
    game_data = [[]] * num_threads
    threads = [None] * num_threads
    # pull stats
    for i in range(num_threads):
        threads[i]  = threading.Thread(target=player_worker, args=(q, player_data, game_data, i), \
            name='player-worker-{}'.format(i))
        threads[i].setDaemon(True)
        threads[i].start()
    q.join()
    # flatten arrays
    game_data = [x for sub in game_data for x in sub]
    player_data = [x for sub in player_data for x in sub]

    game_df = pd.DataFrame(game_data)
    player_df = pd.DataFrame(player_data)
    game_df.to_pickle("data/gamelogs.pkl")
    player_df.to_pickle("data/players.pkl")

get_data(5)