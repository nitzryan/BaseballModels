import sqlite3
from tqdm import tqdm
import json

db = sqlite3.connect("../BaseballStats.db")
cursor = db.cursor()

# Go player by player
ids = cursor.execute("SELECT DISTINCT mlbId FROM Output_AggregatePitcherWar").fetchall()
for id, in tqdm(ids):
    data = {"models_pitcher":[]}
    # Go Model by Model
    models = cursor.execute("SELECT DISTINCT model FROM Output_AggregatePitcherWar WHERE mlbId=?", (id,)).fetchall()
    all_stats = []
    for n, (model,) in enumerate(models):
        this_model = {"name":model, "data":[], "tainted":False}
        resultData = cursor.execute("SELECT year, month FROM Output_AggregatePitcherWar WHERE mlbId=? AND model=? ORDER BY year ASC, month ASC", (id, model)).fetchall()
        for year, month in resultData:
            wars = cursor.execute("SELECT prob0, prob1, prob2, prob3, prob4, prob5, prob6 FROM Output_AggregatePitcherWar WHERE mlbId=? AND model=? AND year=? AND month=?", (id, model, year, month)).fetchone()
            wars = tuple(round(x * 100, 1) for x in wars)
            
            if n == 0:
                #statsData = cursor.execute("SELECT LevelId, LeagueId, TeamId, PA, AVG, OBP, SLG, ISO, wOBA, HRPerc, BBPerc, KPerc, SBRate, SBPerc FROM Player_Hitter_MonthAdvanced WHERE mlbId=? AND year=? AND month=? ORDER BY LevelId DESC", (id,year,month)).fetchall()
                statsData = cursor.execute("SELECT LevelId, LeagueId, TeamId, Outs, GBRatio, ERA, FIP, KPerc, BBPerc, HRPerc, wOBA FROM Player_Pitcher_MonthAdvanced WHERE mlbId=? AND year=? AND month=? ORDER BY LevelId DESC", (id,year,month)).fetchall()
                stats = []
                for s in statsData:
                    s = tuple(round(x,3) for x in s)
                    stats.append(list(s))
                if len(stats) > 0:
                    all_stats.append({"stats":stats, "year":year, "month":month})
            this_model["data"].append({"war":list(wars), "year":year, "month":month})
            
        data["models_pitcher"].append(this_model)
            
    
    first_name, last_name, draft_pick, signing_year = cursor.execute("SELECT DISTINCT useFirstName, useLastName, draftPick, signingYear FROM Player WHERE mlbId=?", (id,)).fetchone()
    data["first"] = first_name
    data["last"] = last_name
    data["draft"] = draft_pick
    data["year"] = signing_year
    data["stats"] = all_stats
    data["player_type"] = "P"
    
    birth_year, birth_month, birth_date = cursor.execute("SELECT birthYear, birthMonth, birthDate FROM Player WHERE mlbId=? LIMIT 1", (id,)).fetchone()
    data["birth_year"] = birth_year
    data["birth_month"] = birth_month
    data["birth_date"] = birth_date
    
    json_data = json.dumps(data, indent=2)
    with open(f"../../../ProspectRankingsSite2/public/assets/player_data/{id}.json", "w") as file:
        file.write(json_data)