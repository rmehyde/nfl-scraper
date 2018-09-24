import pickle
import numpy as np

""" TODO: tease out a base class """

class GameLogsFormatter():

    dtype_map = {
        "FumblesFF": "Integer", "FumblesFUM": "Integer", "FumblesLost": "Integer",
        "FumblesTD": "Integer", "Game Date": "String", "GameAtHome": "Boolean",
        "GamesG": "Boolean", "GamesGS": "Boolean", "InterceptionsAvg": "Float",
        "InterceptionsInt": "Integer", "InterceptionsLng": "String", "InterceptionsPDef": "Integer",
        "InterceptionsTDs": "Integer", "InterceptionsYds": "Integer", "KickoffsAvg": "Float",
        "KickoffsKO": "Integer", "KickoffsRet": "Integer", "KickoffsTB": "Integer", "Opp": "String",
        "OppScore": "Integer", "Opponent": "String", "Overall FGsBlk": "Integer",
        "Overall FGsFG Att": "Integer", "Overall FGsFGM": "Integer", "Overall FGsLng": "Integer",
        "Overall FGsPct": "Float", "PATBlk": "Integer", "PATPct": "Float", "PATXP Att": "Integer",
        "PATXPM": "Integer", "PassingAtt": "Integer", "PassingAvg": "Float","PassingComp": "Integer",
        "PassingInt": "Integer", "PassingPct": "Float", "PassingRate": "Float", "PassingSck": "Integer",
        "PassingSckY": "Integer", "PassingTD": "Integer", "PassingYds": "Integer", "PlayerID": "Integer",
        "PunterAvg": "Float", "PunterBlk": "Integer", "PunterDn": "Integer",  "PunterFC": "Integer",
        "PunterIN 20":  "Integer", "PunterLng": "Integer", "PunterNet Avg": "Float", "PunterNet Yds": "Integer",
        "PunterOOB": "Integer", "PunterPunts": "Integer", "PunterRet": "Integer", "PunterRetY": "Integer",
        "PunterTB": "Integer", "PunterTD": "Integer", "PunterYds": "Integer", "ReceivingAvg": "Float",
        "ReceivingLng": "String", "ReceivingRec": "Integer", "ReceivingTD": "Integer",
        "ReceivingYds": "Integer", "Result": "String", "RushingAtt": "Integer", "RushingAvg": "Float",
        "RushingLng": "String", "RushingTD": "Integer", "RushingYds": "Integer", "Season": "Integer",
        "SeasonType": "String", "TacklesAst": "Integer", "TacklesComb": "Integer", "TacklesSFTY": "Integer",
        "TacklesSck": "Float", "TacklesTotal": "Integer", "Team": "String", "TeamScore": "Integer",
        "WK": "Integer"
    }

    colname_map = {
        "Game Date": "GameDate",
        "GamesG": "GamePlayed",
        "GamesGS": "GameStarted",
        "Overall FGsBlk": "FieldGoalsBlk",
        "Overall FGsFG Att": "FieldGoalsAtt",
        "Overall FGsFGM": "FieldGoalsMade",
        "Overall FGsLng": "FieldGoalsLng",
        "Overall FGsPct": "FieldGoalsPct",
        "PATXP Att": "PATXPAtt",
        "PunterIN 20": "PunterInside20",
        "PunterNet Avg": "PunterNetAvg",
        "PunterNet Yds": "PunterNetYds"
    }

    def __init__(self):
        with open("data/players_raw.pkl", "rb") as f:
            self.players = pickle.loads(f.read())

    def read_raw(self, filename):
        ''' load the raw gamelogs file that getters make into memory'''
        ret = None
        with open(filename, "rb") as f:
            ret = pickle.loads(f.read())
        return ret

    def save_as_pickle(self, obj, filename):
        ''' take in an object and pickle it saving as filename'''
        with open(filename, "wb") as f:
            f.write(pickle.dumps(obj))

    def _convert_val(self, key, val):
        ''' take in a key and value and return the parsed value'''
        dest_type = self.dtype_map[key]
        if val == "--":
            return np.nan
        elif dest_type == "Integer":
            return int(val)
        elif dest_type == "Float":
            return float(val)
        elif dest_type == "String":
            return str(val)  #should already be string but why not
        elif dest_type == "Boolean":
            return bool(int(val))
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

    def set_colname_map(self, newmap):
        self.colname_map = newmap

    def rename_columns(self, df):
        ''' rename some columns of pandas dataframe to improve readability '''
        return df.rename(self.colname_map, axis='columns')

class PlayersFormatter:

    dtype_map = {
        "Birthday": "String",
        "Height": "Integer",
        "JerseyNum": "String",
        "Name": "String",
        "PlayerID": "Integer",
        "Position": "String",
        "PrettyName": "String",
        "WebID": "String",
        "Weight": "Integer"
    }

    def __init__(self):
        pass

    def _convert_val(self, key, val):
        ''' take in a key and value and return the parsed value'''
        dest_type = self.dtype_map[key]
        if val == None or val == "--":
            return np.nan
        elif dest_type == "Integer":
            return int(val)
        elif dest_type == "Float":
            return float(val)
        elif dest_type == "String":
            return str(val)  #should already be string but why not
        elif dest_type == "Boolean":
            return bool(int(val))
        else:
            raise ValueError("not sure how to convert to %s" %dest_type)

    def format_raw(self, orig):
        ''' use the datatype key to format values in a raw dataset and return a new one'''
        ret = []
        for point in orig:
            new_point = {}
            for key in point.keys():
                try:
                    new_point[key] = self._convert_val(key, point[key])
                except ValueError:
                    import pdb; pdb.set_trace()
            ret.append(new_point)
        return ret

    def read_raw(self, filename):
        ''' load the raw gamelogs file that getters make into memory'''
        ret = None
        with open(filename, "rb") as f:
            ret = pickle.loads(f.read())
        return ret