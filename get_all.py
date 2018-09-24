import getter
import formatter
import logging
import pandas as pd
import pickle
import queue
import threading
import time
import urllib
from random import randint
import sqlite3

##### LOGGING SETUP ######
logger = logging.getLogger('nfl')
logger.setLevel(logging.DEBUG)
logformatter = logging.Formatter('%(asctime)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logformatter)
fh = logging.FileHandler('nfl.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(logformatter)
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
    count = 0
    done = False
    while not done:
        logger.debug("thread %d: top of while loop" %i)
        # if theres no more work to do exit while loop
        if q.empty():
            logger.debug("thread %d: queue is empty, setting done")
            done = True
            continue
        url = q.get(block=False)
        logger.debug("thread %d got url %s" %(i, url))
        try:
            logger.debug("thread %d trying" %i)
            pg = getter.PlayerGetter(url, season_start=2001, season_end=2018)
            player_gls = pg.get_game_logs()
            player = {}
            player["WebID"] = pg.web_id
            player["PlayerID"] = pg.player_id
            player["Name"] = pg.player_name
            player["PrettyName"] = pg.player_prettyname
            player["Position"] = pg.position
            player["Height"] = pg.height
            player["Weight"] = pg.weight
            player["Birthday"] = pg.birthday
            player["JerseyNum"] = pg.jerseynum
            player_data.append(player)
            game_data.extend(player_gls)
            q.task_done()
            logger.debug("thread %d got player %s" %(i, pg.player_prettyname))
        # if we fail to connect re add to queue and wait before resuming work
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            q.put(url)
            q.task_done()
            naptime = randint(5,30)
            logger.debug("Thread %d encountered URLError or ConnectionError. PlayerGetter re-added to queue, taking a  %d second nap..." %(i, naptime))
            time.sleep(naptime)
        logger.debug("%d active threads, queue has size %d" %(threading.active_count(), q.qsize()))
        
def get_data(num_threads):
    # read player urls file from get_links
    with open('data/player_urls.pkl', 'rb') as f:
        player_urls = pickle.loads(f.read())

    # initialize a queue of player urls
    q = queue.Queue()
    for url in player_urls:
        q.put(url)

    player_data = []
    game_data = []
    threads = [None] * num_threads
    # pull stats
    for i in range(num_threads):
        threads[i]  = threading.Thread(target=player_worker, args=(q, player_data, game_data, i), \
            name='player-worker-{}'.format(i))
        threads[i].start()
    # main thread waits for all tasks to finish then kills threads
    q.join()

    with open('data/gamelogs_raw.pkl', 'wb') as f:
        f.write(pickle.dumps(game_data))
    with open('data/players_raw.pkl', 'wb') as f:
        f.write(pickle.dumps(player_data))

def format_gamelogs():
    fm = formatter.GameLogsFormatter()
    raw = fm.read_raw("data/gamelogs_raw.pkl")
    new = fm.format_raw(raw)
    df = pd.DataFrame(new)
    return fm.rename_columns(df)

def format_players():
    fm = formatter.PlayersFormatter()
    raw = fm.read_raw('data/players_raw.pkl')
    new = fm.format_raw(raw)
    df = pd.DataFrame(new).set_index('PlayerID')
    return df

def write_to_db(gl_df, players_df):
    conn = sqlite3.connect('data/nfl.db')
    gl_df.to_sql("GameLogs", conn, if_exists='fail')
    players_df.to_sql("Players", conn, if_exists='fail')

get_links()
get_data(50)
gl_df = format_gamelogs()
players_df = format_players()
write_to_db(gl_df, players_df)