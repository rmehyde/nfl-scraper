from lxml import html
import requests
import logging
import threading
import queue

# Use this class to obtain URLs you want to scrape
class LinkGetter:

    # constructor defaults to regular 2018 season
    def __init__(self, season_start=2018, season_end=2018, season_types=('REG',),
        positions=("QUARTERBACK", "RUNNING_BACK", "WIDE_RECEIVER", "TIGHT_END",
            "DEFENSIVE_LINEMAN", "LINEBACKER", "DEFENSIVE_BACK", "KICKOFF_KICKER",
            "KICK_RETURNER", "PUNTER", "PUNT_RETURNER", "FIELD_GOAL_KICKER")):
        seasons = []
        for i in range(season_start, season_end+1):
            seasons.append(str(i))
        self.seasons = seasons
        self.season_types = season_types
        self.positions = positions
        self.CURR_SEASON = '2018'
        self.logger = logging.getLogger('nfl')

    # gets from url and returns parsed tree
    def get_tree(self, url):
        self.logger.debug('Getting page at %s' % url)
        page = requests.get(url)
        self.logger.debug('Building tree from %s' % url)
        tree = html.fromstring(page.content)
        return tree

    # thread target
    def worker(self, q, results, i):
        while True:
            url = q.get()
            results[i].append(self.get_tree(url))
            q.task_done()

    # returns array of parsed html trees from list of urls
    def get_trees(self, urls, num_threads):
        self.logger.info('Initializing lists and queue')
        ret = [[]] * num_threads
        threads = [None] * num_threads
        q = queue.Queue()
        self.logger.info('Filling queue...')
        for url in urls:
            q.put(url)
        self.logger.info('Instantiating threads...')
        for i in range(num_threads):
            threads[i] = threading.Thread(target=self.worker, args=(q, ret, i), name='worker-{}'.format(i))
            threads[i].setDaemon(True)
            threads[i].start()
        self.logger.info('Joining queue....')
        q.join()
        self.logger.info('Flattening array')
        return [x for sub in ret for x in sub]

    # gets player urls from complete list of player index pages
    def get_player_urls(self, index_urls):
        player_urls = []

        trees = self.get_trees(index_urls, 10)
        for ind_url in index_urls:
            try:
                rows = self.get_tree(ind_url).find('body/div/div/div/div/div/div/div/form/table/tbody').findall('tr')
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

    # return the subsequent index pages after page 1 of a category
    def get_next_pages(self, start_urls):
        urls = []
        for url in start_urls:
            tree = self.get_tree(url)
            othIndPages = tree.findall('body[@id="com-nfl"]/div[@id="com-nfl-doc"]/div[@id="doc"]/div[@id="doc-wrap"]/div[@id="main-content"]/div[@class="c"]/div[@class="grid"]/div[@class="col span-12"]/form/span/a')
            for elt in othIndPages:
                if 'http://nfl.com'+elt.get('href') not in urls:
                    urls.append('http://nfl.com'+elt.get('href'))
        return urls

    # generate the initial index pages based on seasons and positions
    def gen_init_urls(self):
        urls = []
        for season in self.seasons:
            for seasontype in self.season_types:
                for pos in self.positions:
                    urls.append(
                        'http://www.nfl.com/stats/categorystats?&conference=null&statisticPositionCategory=%s&season=%s&seasonType=%s&experience=&tabSeq=1&qualified=true&Submit=Go' % (pos, season, seasontype))
        return urls