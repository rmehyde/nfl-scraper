import requests
from lxml import html
import logging

##### LOGGING SETUP ######
logger = logging.getLogger('nfl.player')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(ch)
####### ~~~~~~~~~~ #######


class PlayerThread:
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
    for i in range(2000, 2017):
        DEF_SEASONS.append(str(i))
    DEF_SEASON_TYPES = ["Preseason", "Regular Season", "Postseason"]

    CURR_SEASON = '2018'


    def __init__(self, url):
        if '?' not in url:
            raise ValueError('Unrecognized url format. url provided: %s' %url)
        self.init_url = url
        self.profile_url = None
        self.playerID = None
        self.player_name = None
        self.player_num = None
        self.gls_url = None
        self.current_url = None
        super().__init__(name="Player %s Thread" %self.playerID)

    def build_playervars(self, url):
        self.profile_url = url
        self.playerID = url.split('?')[-1][3:]
        self.player_name = url.split('/')[-2]
        self.player_num = requests.get(url).url.split('/')[-2]
        gl_page = requests.get('http://nfl.com/players/' + self.player_name + '/gamelogs?id=' + self.playerID)
        self.gls_url = gl_page.url

    def run(self, seasons=DEF_SEASONS, seasontypes=DEF_SEASON_TYPES):
        self.build_playervars(self.init_url)
        if not isinstance(seasons[0], str):
            raise ValueError("Seasons provided should be strings!!!")
        gl_tree = html.fromstring(requests.get(self.gls_url).content)
        profile_tree = html.fromstring(requests.get(self.profile_url).content)
        team_record, gl_seasons = self.get_team_record(profile_tree, seasons), self.get_valid_seasons(gl_tree, seasons)
        if gl_seasons != sorted(list(team_record), reverse=True):
            err_txt = "team record keys do not match valid seasons. \n TR keys: %s \n VS: %s" %(sorted(list(team_record), reverse=True), gl_seasons)
            raise ValueError(err_txt)
        game_data = self.get_player_games(gl_seasons, seasontypes)
        self.check_keys(game_data)
        return game_data, team_record

    def check_keys(self, gd):
        key = gd[0].keys()
        for pt in gd:
            if pt.keys() != key:
                logger.warning("GAME_DATA KEYS DO NOT ALL MATCH!!!!!")

    def get_player_games(self, seasons, seasontypes):
        gl_urls = []
        for sea in seasons:
            gl_urls.append(self.gls_url + '?season=' + sea)
        data = []
        for url in gl_urls:
            self.current_url = url
            tree = html.fromstring(requests.get(url).content)
            data.extend(self.get_game_data(tree, seasontypes))
        return data

    def get_game_data(self, gl_tree, seasontypes=("Preseason", "Regular Season", "Postseason")):
        tables = gl_tree.findall('body/div/div/div/div/div/div/div/div/div/table')
        ret = []
        for table in tables:
            statcats = table.find('thead/tr')
            statkey = table.findall('thead/tr')[1]
            if statcats.find('td').text not in seasontypes:
                continue
            data = []
            data.extend(table.findall('tbody/tr')[:-1])
            for row in data:
                elts = row.findall('td')
                if len(elts) == 1:
                    continue
                ret.append(self.parse_row(elts, statcats, statkey))
        return ret

# TODO !! NO NEED FOR DICT KEYS EVERY TIME USE AN ARRAY !! ##
    def parse_row(self, elts, statcats, statkey):
        point = {}
        curr = 0
        next_ind = 4

        # we will process all in seasontype category first so that seasontype is not included in category
        while curr < next_ind:
            key = statkey[curr].text
            if key == "Opp":
                point[key], point["GameAtHome"] = self.parse_opp(elts[curr])
            elif key == "Result":
                point["Result"], point["TeamScore"], point["OppScore"] = self.parse_res(elts[curr], point["GameAtHome"])
            else:
                point[key] = elts[curr].text
            curr += 1

        # now lets do the others including the statcat prefix
        # hey look its prettier too because all special cases are handled in seatype statcat :)
        for statcat in statcats[1:]:
            next_ind += self.STATCAT_KEY[statcat.text]
            while curr < next_ind:
                try:
                    key = statkey[curr].text
                except IndexError:
                    logging.warning(("BAD INDEX \n Current Statcat: %s \n Current index: %s \n Statkey size: %s \n NextInd: %s" % (statcat.text, curr, len(statkey), next_ind)))
 #                   print("BAD INDEX \n Current Statcat: %s \n Current index: %s \n Statkey size: %s \n NextInd: %s" % (statcat.text, curr, len(statkey), next_ind))
                point[statcat.text + key] = elts[curr].text
                curr += 1

        # process fumbles
        while curr < len(statkey):
            key = statkey[curr].text
            point["Fumbles" + key] = elts[curr].text
            curr += 1
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
            raise ValueError(self.current_url)
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
        win_loss = elt.find('span').text
        team_score, opp_score = elt.find('a').text.split('-')
        team_score = team_score[-2:]
        opp_score = opp_score[:2]
        if win_loss is None or team_score is None or opp_score is None:
            raise ValueError('unable to assign opp value')
        return win_loss, team_score, opp_score

    def get_team_record(self, profile_tree, seasons=DEF_SEASONS):
        team_record = {}
        career_table = None
        tables = profile_tree.findall('body/div/div/div/div/div/div/div/div/table[@class="data-table1"]')
        for table in tables:
            if(table.find('thead/tr/td/span')).text == 'Career Stats':
                career_table = table
                break
        if career_table is None:
            logger.warning("No career stats found on %s !!!" % self.profile_url)

        for row in career_table.findall('tr'):
            if row is not None and row.find('td/a') is not None:
                team_record[row.find('td').text] = row.find('td/a').get('href').split('=')[1]
        for row in career_table.findall('tbody/tr'):
            if row is not None and row.find('td/a') is not None:
                team_record[row.find('td').text] = row.find('td/a').get('href').split('=')[1]
        team_record[self.CURR_SEASON] = profile_tree.find('body/div/div/div/div/div/div/div/div/div/div/div/div/p/a').get('href').split('=')[1]
        for key in list(team_record):
            if key not in seasons:
                del team_record[key]
        return team_record


    def get_valid_seasons(self, gl_tree, seasons=DEF_SEASONS):
        valid_seasons = []
        for opt in gl_tree.findall('body/div/div/div/div/div/div/div/div/div/form/div/select/option'):
            valid_seasons.append(opt.text)
        for i in range(len(valid_seasons))[::-1]:
            if valid_seasons[i] not in seasons:
                del valid_seasons[i]
        return valid_seasons
