import sqlite3
import json
from unidecode import unidecode

db = sqlite3.connect("../BaseballStats.db")
cursor = db.cursor()

playerData = cursor.execute("SELECT mlbId, useFirstName, useLastName FROM Player").fetchall()
playerMaps = []
for id, firstName, lastName in playerData:
    if firstName is None or lastName is None:
        continue
    m = {"id":id,"first":unidecode(firstName),"last":unidecode(lastName)}
    playerMaps.append(m)
    
json_maps = json.dumps(playerMaps, indent=2)
with open("player_names.json", "w") as file:
    file.write(json_maps)