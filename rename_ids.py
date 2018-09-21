import pickle

with open("data/players_raw.pkl", 'rb') as f:
	players = pickle.loads(f.read())

for i in range(len(players)):
	point = players[i]
	point["WebID"] = point["PlayerID"]
	point["PlayerID"] = point["PlayerNum"]
	del point["PlayerNum"]
	players[i] = point

with open("data/players_raw_new.pkl", "wb") as f:
	f.write(pickle.dumps(players))
