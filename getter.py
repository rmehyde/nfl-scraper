from lxml import html
from urllib import request
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
        res = request.urlopen(url)
        tree = html.fromstring(res.read())
        return tree

    # thread target
    def worker(self, q, results, i):
        while True:
            url = q.get()
            results.append(self.get_tree(url))
            q.task_done()

    # returns array of parsed html trees from list of urls
    def get_trees(self, urls, num_threads):
        ret = [] * num_threads
        threads = [None] * num_threads
        q = queue.Queue()
        for url in urls:
            q.put(url)
        for i in range(num_threads):
            threads[i] = threading.Thread(target=self.worker, args=(q, ret, i), name='worker-{}'.format(i))
            threads[i].setDaemon(True)
            threads[i].start()
        q.join()
        return ret

    # gets player urls from complete list of player index pages
    def get_player_urls(self, index_urls):
        player_urls = []
        trees = self.get_trees(index_urls, 10)
        for ind_url in index_urls:
            try:
                rows = self.get_tree(ind_url).find('body/div/div/div/div/div/div/div/form/table/tbody').findall('tr')
                for row in rows:
	                for link in row.findall('td/a'):
	                    if 'http://nfl.com' + link.get('href') not in player_urls:
	                        player_urls.append('http://nfl.com'+link.get('href'))
            except AttributeError:
                self.logger.info("No player pages found on %s \n Probably just because there aren't any." %ind_url)
        # remove urls that arent to players
        for i in range(len(player_urls))[::-1]:
            if player_urls[i][15] != 'p':
                player_urls.pop(i)
        # remove duplicates
        player_urls = list(set(player_urls))
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

class PlayerGetter:
    STATCAT_KEY = {"Preseason": 4,
                   "Regular Season": 4,
                   "Postseason": 4,
                   "Games": 2,
                   "Passing": 10,
                   "Rushing": 4,
                   "Fumbles": 0,
                   "Receiving": 5,
                   "Tackles": 5,
                   "Interceptions": 6,
                   "Punter": 15,
                   "Overall FGs": 5,
                   "PAT": 4,
                   "Kickoffs": 5,
                   }
    SEASONTYPE_KEY = {"PRE": "Preseason",
                      "REG": "Regular Season",
                      "POST": "Postseason"
                      }
    DEF_SEASONS = []
    for i in range(2000, 2019):
        DEF_SEASONS.append(str(i))
    DEF_SEASON_TYPES = ["Preseason", "Regular Season", "Postseason"]
    CURR_SEASON = '2018'

    def __init__(self, url, season_start, season_end):
        if '?' not in url:
            raise ValueError('Unrecognized url format. url provided: %s' %url)
        self.allowed_seasons = []
        for i in range(season_start, season_end+1):
            self.allowed_seasons.append(str(i))
        DEF_SEASONS = self.allowed_seasons  #ew im sorry
        self.init_url = url
        self.profile_url = None
        self.player_id = None
        self.player_name = None
        self.player_prettyname = None
        self.player_num = None
        self.gls_url = None
        self.current_url = None
        self.position = None
        self.height = None
        self.weight = None
        self.jerseynum = None
        self.birthday = None
        self.logger = logging.getLogger("nfl")
        self.build_playervars(self.init_url)

    def build_playervars(self, url):
        self.profile_url = url
        self.player_id = url.split('?')[-1][3:]
        self.player_name = url.split('/')[-2]
        gl_res = request.urlopen('http://nfl.com/players/' + self.player_name + '/gamelogs?id=' + self.player_id)
        self.gls_url = gl_res.geturl()
        prof_res = request.urlopen(url)
        self.player_num = prof_res.geturl().split('/')[-2]
        tree = html.fromstring(prof_res.read())
        self.player_prettyname = tree.find_class("player-name")[0].text.strip()
        # some players are missing position and jersey number, need diff parsing
        try:
            self.jerseynum = tree.find_class("player-number")[0].text.strip().split(" ")[0]
            height = tree.find_class("player-info")[0].getchildren()[2].find("strong").tail[1:].strip().split('-')
            self.height = str(int(height[0])*12 + int(height[1]))
            self.weight = tree.find_class("player-info")[0].getchildren()[2].findall("strong")[1].tail[1:].strip()
            self.birthday = tree.find_class("player-info")[0].getchildren()[3].find("strong").tail.split(" ")[1]
        except IndexError:
            self.logger.debug("Failed to get jersey number for %s" %self.profile_url)
            height = tree.find_class("player-info")[0].getchildren()[1].find("strong").tail[1:].strip().split('-')
            self.height = str(int(height[0])*12 + int(height[1]))
            self.weight = tree.find_class("player-info")[0].getchildren()[1].findall("strong")[1].tail[1:].strip()
            self.birthday = tree.find_class("player-info")[0].getchildren()[2].find("strong").tail.split(" ")[1]
        self.position = tree.find("head/title").text.split(',')[1].split(' ')[1].strip()

    def check_keys(self, gd):
        try:
            key = gd[0].keys()
            for pt in gd:
                if pt.keys() != key:
                    self.logger.warning("For player %s game data features are not alligned (game_data keys dont match)" %self.player_name)
        except IndexError:
            self.logger.debug("Empty game logs for %s" %self.player_name)

    def get_game_logs(self, seasons= DEF_SEASONS, seasontypes=DEF_SEASON_TYPES):
        gl_tree = html.fromstring(request.urlopen(self.gls_url).read())
        valid_seasons = self.get_valid_seasons(gl_tree, seasons)
        gl_urls = []
        for sea in seasons:
            gl_urls.append(self.gls_url + '?season=' + sea)
        data = []
        for url in gl_urls:
            self.current_url = url
            tree = html.fromstring(request.urlopen(url).read())
            data.extend(self.parse_gl_page(tree, seasontypes))
        self.check_keys(data)
        return data

    def parse_gl_page(self, gl_tree, seasontypes=("Preseason", "Regular Season", "Postseason")):
        tables = gl_tree.find_class('data-table1')
        ret = []
        for table in tables:
            statcats = table.find('thead/tr')
            statkey = table.findall('thead/tr')[1]
            if statcats.find('td').text not in seasontypes:
                continue
            rows = []
            rows.extend(table.findall('tbody/tr')[:-1])
            for row in rows:
                elts = row.findall('td')
                # ignore border rows and bye weeks
                if len(elts) == 1 or elts[1].text == "Bye":
                    continue
                point =self.parse_row(elts, statcats, statkey)
                point["SeasonType"] = statcats.find('td').text
                ret.append(point)
        return ret

# TODO !! NO NEED FOR DICT KEYS EVERY TIME USE AN ARRAY !! ##
    def parse_row(self, elts, statcats, statkey):
        point = {}
        curr = 0
        next_ind = 4

        # get team game (meta)data first
        while curr < next_ind:
            key = statkey[curr].text
            if key == "Opp":
                point[key], point["GameAtHome"] = self.parse_opp(elts[curr])
            elif key == "Result":
                point["Result"], point["TeamScore"], point["OppScore"], point["Team"], point["Opponent"] \
                    = self.parse_res(elts[curr], point["GameAtHome"])
            else:
                point[key] = elts[curr].text
            curr += 1

        # now lets do the others including the statcat prefix
        for statcat in statcats[1:]:
            next_ind += self.STATCAT_KEY[statcat.text]
            while curr < next_ind:
                try:
                    key = statkey[curr].text
                except IndexError:
                    self.logger.warning(("BAD INDEX \n Current Statcat: %s \n Current index: %s \n Statkey size: %s \n NextInd: %s" % (statcat.text, curr, len(statkey), next_ind)))
 #                   print("BAD INDEX \n Current Statcat: %s \n Current index: %s \n Statkey size: %s \n NextInd: %s" % (statcat.text, curr, len(statkey), next_ind))
                point[statcat.text + key] = elts[curr].text
                curr += 1

        # process fumbles
        while curr < len(statkey):
            key = statkey[curr].text
            point["Fumbles" + key] = elts[curr].text
            curr += 1

        # tack on our current season
        point["Season"] = self.current_url.split("=")[-1]
        point["PlayerID"] = self.player_num
        return point

    def parse_opp(self, elt):
        home = None
        opp = None
        # with the video
        if len(elt.findall('a')) == 2:
            link_elt = elt.findall('a')[1]
        elif len(elt.findall('a')) == 1:
            link_elt = elt.find('a')
        else:
            err_text = "Can't parse opp. Element has %d children and %d link tags and text is %s \n %s" % (len(elt.getchildren()), len(elt.findall('a')), elt.text, self.current_url)
            raise ValueError(err_text)
        if '@' in link_elt.text:
            home = False
        else:
            home = True
        opp = link_elt.get('href').split('=')[1]
        if home is None or opp is None:
            raise ValueError('unable to assign value to home or opp')
        return opp, home

    def parse_res(self, elt, home):
        win_loss =  None
        team_score = None
        opp_score = None
        team = None
        opp = None
        try:
            win_loss = elt.find('span').text
        # tie result is not in a span
        except AttributeError:
            win_loss = elt.text.strip()

        team_score, opp_score = elt.find('a').text.strip().split('-')
        teams = elt.find('a').get('href').split('/')[-1].split('@')
        if home:
            team = teams[1]
            opp = teams[0]
        else:
            team = teams[0]
            opp = teams[1]
        if win_loss is None or team_score is None or opp_score is None:
            raise ValueError('unable to assign opp value')

        return win_loss, team_score, opp_score, team, opp

    def get_valid_seasons(self, gl_tree, seasons=DEF_SEASONS):
        valid_seasons = []
        for opt in gl_tree.findall('body/div/div/div/div/div/div/div/div/div/form/div/select/option'):
            valid_seasons.append(opt.text)
        for i in range(len(valid_seasons))[::-1]:
            if valid_seasons[i] not in seasons:
                del valid_seasons[i]
        return valid_seasons