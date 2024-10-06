import sqlite3
from tqdm import tqdm
import json

#def Create_TeamMap_Json(db : sqlite3.Connection) -> None:
db = sqlite3.connect("../BaseballStats.db")
cursor = db.cursor()
teams = cursor.execute("SELECT * FROM Team_Parents").fetchall()
team_orgs = cursor.execute("SELECT * FROM Team_OrganizationMap").fetchall()

map = {"teams":{}, "orgs":{}}
for id, abbr, name in teams:
    map["orgs"][id] = {"abbr":abbr, "name":name}
    
for id, year, parentId in team_orgs:
    if not id in map["teams"].keys():
        map["teams"][id] = {"years":{}}
    map["teams"][id]["years"][year] = parentId
    
json_data = json.dumps(map, indent=2)
with open(f"../../../ProspectRankingsSite2/public/assets/org_map.json", "w") as file:
    file.write(json_data)