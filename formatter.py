import pickle

class GameLogsFormatter():

    dtype_key = {
        "FumblesFF": "Integer", "FumblesFUM": "Integer", "FumblesLost": "Integer",
        "FumblesTD": "Integer", "Game Date": "String", "GameAtHome": "Boolean",
        "GamesG": "Integer", "GamesGS": "Integer", "InterceptionsAvg": "Float",
        "InterceptionsInt": "Integer", "InterceptionsLng": "Integer", "InterceptionsPDef": "Integer",
        "InterceptionsTDs": "Integer", "InterceptionsYds": "Integer", "KickoffsAvg": "Float",
        "KickoffsKO": "Integer", "KickoffsRet": "Integer", "KickoffsTB": "Integer", "Opp": "String",
        "OppScore": "Integer", "Opponent": "String", "Overall FGsBlk": "Integer",
        "Overall FGsFG Att": "Integer", "Overall FGsFGM": "Integer", "Overall FGsLng": "Integer",
        "Overall FGsPct": "Float", "PATBlk": "Integer", "PATPct": "Float", "PatXP Att": "Integer",
        "PatXPM": "Integer", "PassingAtt": "Integer", "PassingAvg": "Float","PassingComp": "Integer",
        "PassingInt": "Integer", "PassingPct": "Float", "PassingRate": "Float", "PassingSck": "Integer",
        "PassingSckY": "Integer", "PassingTD": "Integer", "PassingYds": "Integer", "PlayerID": "Integer",
        "PunterAvg": "Float", "PunterBlk": "Integer", "PunterDn": "Integer", "PunterNet Yds": "Integer",
        "PunterOOB": "Integer"   "PunterPunts": "Integer", "PunterRet": "Integer", "PunterRetY": "Integer",
        "PunterTB": "Integer", "PunterTD": "Integer", "PunterYds": "Integer", "ReceivingAvg": "Float",
        "ReceivingLng": "Integer", "ReceivingRec": "Integer", "ReceivingTD": "Integer",
        "ReceivingYds": "Integer": "Result": "String" "RushingAtt": "Integer", "RushingAvg": "Float",
        "RushingLng": "Integer", "RushingTD": "Integer", "RushingYds": "Integer", "Season": "Integer",
        "SeasonType": "String", "TacklesAst": "Integer", "TacklesComb": "Integer", "TacklesSFTY": "Integer",
        "TacklesSck": "Integer", "TacklesTotal": "Integer", "Team": "String", "TeamScore": "Integer",
        "WK": "Integer"
    }

    def __init__(self):
        with open("data/players_raw.pkl", "rb") as f:
            self.players = pickle.loads(f.read())

    def read_raw(self, filename):
    	''' load the raw gamelogs file that getters make into memory'''
        ret = None
        with open(filename, "rb'") as f:
            ret = pickle.loads(f.read())
        return ret

    def save_as_pickle(self, obj, filename):
    	''' take in an object and pickle it saving as filename'''
    	with open(filename, "wb") as f:
    		f.write(pickle.dumps(obj))

    def _convert_val(self, key, val):
    	''' take in a key and value and return the parsed value'''
    	dest_type = dtype_key[key]
    	if dest_type == "Integer":
    		return int(val)
    	else if dest_type == "Float":
    		return float(val)
    	else if dest_type == "String":
    		return str(val)  #should already be string but why not
    	else if dest_type == "Boolean":
    		return bool(val)
    	else:
    		raise ValueError("not sure how to convert to %s" %dest_type)

    def format_raw(self, orig):
    	''' use the datatype key to format values in a raw dataset and return a new one'''
    	ret = []
    	for point in orig:
    		new_point = {}
    		for key in point.keys():
    			new_point[key] = self._convert_val(key, point[key])
    		ret.append(new_point)
    	return ret

    def split_by_pos(self, orig):
    	''' takes in a raw dataset and returns a dict of datasets split by player position'''
    	ret = {}
    	for point in orig:
    		pos = get_position_by_num(point["PlayerID"])
    		if pos not in ret.keys():
    			ret[pos] = []
    		ret[pos].append(point)

    def _get_position_by_num(self, player_id):
        res = list(self.players[self.players.PlayerID == player_id].Position)
        if len(res) == 0:
            return None
        else:
            return res[0]