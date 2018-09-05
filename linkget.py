from lxml import html
import requests
import logging
import threading
import queue

##### LOGGING SETUP ######
logger = logging.getLogger('nfl.link_getter')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
####### ~~~~~~~~~~ #######

CURR_SEASON = '2018'


def get_tree(url):
    logger.debug('Getting page at %s' % url)
    page = requests.get(url)
    logger.debug('Building tree from %s' % url)
    tree = html.fromstring(page.content)
    return tree


def worker(q, results, i):
    while True:
        url = q.get()
        results[i].append(get_tree(url))
        q.task_done()


def get_trees(urls, num_threads):
    logger.info('Initializing lists and queue')
    ret = [[]] * num_threads
    threads = [None] * num_threads
    q = queue.Queue()
    logger.info('Filling queue...')
    for url in urls:
        q.put(url)
    logger.info('Instantiating threads...')
    for i in range(num_threads):
        threads[i] = threading.Thread(target=worker, args=(q,ret, i), name='worker-{}'.format(i))
        threads[i].setDaemon(True)
        threads[i].start()
    logger.info('Joining queue....')
    q.join()
    logger.info('Flattening array')
    return [x for sub in ret for x in sub]


seasontypes = ('PRE', 'REG', 'POST')
fantasypositions = ("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END", "FIELD_GOAL_KICKER")
positions = ("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END", "DEFENSIVE_LINEMAN", "LINEBACKER",
             "DEFENSIVE_BACK", "KICKOFF_KICKER", "KICK_RETURNER", "PUNTER", "PUNT_RETURNER", "FIELD_GOAL_KICKER")


def get_player_urls(index_urls):
    player_urls = []

    trees = get_trees(index_urls, 100)
    for ind_url in index_urls:
        try:
            rows = get_tree(ind_url).find('body/div/div/div/div/div/div/div/form/table/tbody').findall('tr')
        except AttributeError:
            logging.info("No player pages found on %s \n Probably just because there aren't any." %ind_url)
        for row in rows:
            for link in row.findall('td/a'):
                if 'http://nfl.com' + link.get('href') not in player_urls:
                    player_urls.append('http://nfl.com'+link.get('href'))
    # remove urls that arent to players
    for i in range(len(player_urls))[::-1]:
        if player_urls[i][15] != 'p':
            player_urls.pop(i)
    return player_urls


def get_next_pages(start_urls):
    urls = []
    for url in start_urls:
        tree = get_tree(url)
        othIndPages = tree.findall('body[@id="com-nfl"]/div[@id="com-nfl-doc"]/div[@id="doc"]/div[@id="doc-wrap"]/div[@id="main-content"]/div[@class="c"]/div[@class="grid"]/div[@class="col span-12"]/form/span/a')
        for elt in othIndPages:
            if 'http://nfl.com'+elt.get('href') not in urls:
                urls.append('http://nfl.com'+elt.get('href'))
    return urls


def _get_next_pages(start_urls):
    urls = []
    trees = get_trees(start_urls, 10)
    for tree in trees:
        othIndPages = tree.findall('body[@id="com-nfl"]/div[@id="com-nfl-doc"]/div[@id="doc"]/div[@id="doc-wrap"]/div[@id="main-content"]/div[@class="c"]/div[@class="grid"]/div[@class="col span-12"]/form/span/a')
        for elt in othIndPages:
            if 'http://nfl.com'+elt.get('href') not in urls:
                urls.append('http://nfl.com'+elt.get('href'))


def gen_init_urls(seasons, positions, seasontypes, current_season=2016):
    urls = []
    for season in seasons:
        for seasontype in seasontypes:
            for pos in positions:
                urls.append(
                    'http://www.nfl.com/stats/categorystats?archive=%s&conference=null&statisticPositionCategory=%s&season=%s&seasonType=%s&experience=&tabSeq=1&qualified=true&Submit=Go' % (str(season != current_season).lower(), pos, season, seasontype))
    return urls


def scrape_all_urls(seasons=(CURR_SEASON,)):
    logger.info("Building initial index urls.")
    ind_urls = gen_init_urls(seasons, positions, seasontypes)
    logger.info("Retreiving next pages.....")
    ind_urls.extend(get_next_pages(ind_urls))
    logger.info("Pulling player urls from index pages.....")
    player_urls = get_player_urls(ind_urls)
    logger.info("Done. Returning.")
    return player_urls
