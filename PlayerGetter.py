from urllib import request
from lxml import html
import logging

##### LOGGING SETUP ######
logger = logging.getLogger('nfl')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(ch)
####### ~~~~~~~~~~ #######


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


    def __init__(self, url):
        if '?' not in url:
            raise ValueError('Unrecognized url format. url provided: %s' %url)
        self.init_url = url
        self.profile_url = None
        self.player_id = None
        self.player_name = None
        self.player_num = None
        self.gls_url = None
        self.current_url = None

    def build_playervars(self, url):
        self.profile_url = url
        self.player_id = url.split('?')[-1][3:]
        self.player_name = url.split('/')[-2]
        self.player_num = request.urlopen(url).geturl().split('/')[-2]
        gl_page = request.urlopen('http://nfl.com/players/' + self.player_name + '/gamelogs?id=' + self.player_id)
        self.gls_url = gl_page.url

    def get(self, seasons=DEF_SEASONS, seasontypes=DEF_SEASON_TYPES):
        self.build_playervars(self.init_url)
        if not isinstance(seasons[0], str):
            raise ValueError("Seasons provided should be strings!!!")
        gl_tree = html.fromstring(request.urlopen(self.gls_url).read())
        profile_tree = html.fromstring(request.urlopen(self.profile_url).read())
        team_record = self.get_team_record(profile_tree, seasons)
        gl_seasons = self.get_valid_seasons(gl_tree, seasons)
        # handle discrepancy between career profile info and game logs
        if gl_seasons != sorted(list(team_record), reverse=True):
            # find missing seasons
            missing = list(set(team_record).difference(set(gl_seasons)))
            warn_text = "Found game logs but not team record for %s for seasons " % self.player_name
            for sea in missing:
                warn_text += sea + " "
            warn_text += "(likely missed for injury)"
            logger.warning(warn_text)
        game_data = self.get_player_games(gl_seasons, seasontypes)
        self.check_keys(game_data)
        return game_data, team_record

    def check_keys(self, gd):
        try:
            key = gd[0].keys()
            for pt in gd:
                if pt.keys() != key:
                    logger.warning("GAME_DATA KEYS DO NOT ALL MATCH!!!!!")
        except IndexError:
            logger.warning("Empty game logs for %s" %self.player_name)

    def get_player_games(self, seasons, seasontypes):
        gl_urls = []
        for sea in seasons:
            gl_urls.append(self.gls_url + '?season=' + sea)
        data = []
        for url in gl_urls:
            self.current_url = url
            tree = html.fromstring(request.urlopen(url).read())
            data.extend(self.get_game_data(tree, seasontypes))
        return data

    def get_game_data(self, gl_tree, seasontypes=("Preseason", "Regular Season", "Postseason")):
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
                ret.append(self.parse_row(elts, statcats, statkey))
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
                    logger.warning(("BAD INDEX \n Current Statcat: %s \n Current index: %s \n Statkey size: %s \n NextInd: %s" % (statcat.text, curr, len(statkey), next_ind)))
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

    def get_team_record(self, profile_tree, seasons=DEF_SEASONS):
        team_record = {}
        career_table = None
        tables = profile_tree.find_class('data-table1')
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
