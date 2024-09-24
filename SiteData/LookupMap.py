import sqlite3
import json
from unidecode import unidecode

db = sqlite3.connect("../BaseballStats.db")
cursor = db.cursor()

playerData = cursor.execute("SELECT DISTINCT mlbId, useFirstName, useLastName FROM Player").fetchall()
playerMaps = []
for id, firstName, lastName in playerData:
    if firstName is None or lastName is None:
        continue
    mlbYears = cursor.execute('''
                              SELECT DISTINCT year
                              FROM Player_Hitter_MonthAdvanced
                              WHERE mlbId=? AND levelId='1'
                              UNION
                              SELECT DISTINCT year
                              FROM Player_Pitcher_GameLog
                              WHERE mlbId=? AND level='1'
                              ORDER BY year ASC
                              ''', (id,id)).fetchall()
    if len(mlbYears) > 0:
        firstYear = mlbYears[0]
        lastYear = mlbYears[-1]
    else:
        firstYear = None
        lastYear = None

    allYears = cursor.execute('''
                              SELECT DISTINCT year
                              FROM Player_Hitter_MonthAdvanced
                              WHERE mlbId=?
                              UNION
                              SELECT DISTINCT year
                              FROM Player_Pitcher_GameLog
                              WHERE mlbId=?
                              ORDER BY year ASC
                              ''', (id,id)).fetchall()
    firstTotalYear = allYears[0]
    lastTotalYear = allYears[-1]
    
    teams = cursor.execute('''
                        SELECT DISTINCT teamId
                        FROM Player_Hitter_MonthAdvanced
                        WHERE mlbId=?
                        UNION
                        SELECT DISTINCT teamId
                        FROM Player_Pitcher_GameLog
                        WHERE mlbId=?
                           ''', (id,id)).fetchall()
    teams = list(teams)
    
    m = {
        "id":id,
        "first":unidecode(firstName),
        "last":unidecode(lastName),
        "fmlb":firstYear,
        "lmlb":lastYear,
        "fyear":firstTotalYear,
        "lyear":lastTotalYear,
        "teams":teams
        }
    playerMaps.append(m)
    
json_maps = json.dumps(playerMaps, indent=2)
with open("player_names.json", "w") as file:
    file.write(json_maps)