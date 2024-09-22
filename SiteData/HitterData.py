import sqlite3
from tqdm import tqdm
import json

db = sqlite3.connect("../BaseballStats.db")
cursor = db.cursor()

# Go player by player
ids = cursor.execute("SELECT DISTINCT mlbId FROM Output_HitterResult").fetchall()
for id, in tqdm(ids):
    data = {"models_hitter":[]}
    
    # Go Model by Model
    models = cursor.execute("SELECT DISTINCT modelVersion,isTainted FROM Output_HitterResult WHERE mlbId=?", (id,)).fetchall()
    for (model,isTainted) in models:
        this_model = {"name":model, "data":[], "tainted":isTainted}
        resultData = cursor.execute("SELECT year, month, outputId FROM Output_HitterResult WHERE mlbId=? AND modelVersion=? ORDER BY year ASC, month ASC", (id, model)).fetchall()
        for year, month, outputId in resultData:
            levels = cursor.execute("SELECT prob0, prob1, prob2, prob3, prob4, prob5, prob6, prob7 FROM Output_HitterLevel WHERE outputId=?", (outputId,)).fetchone()
            levels = tuple(round(x, 3) for x in levels)
            pas = cursor.execute("SELECT prob0, prob1, prob2, prob3, prob4, prob5 FROM Output_HitterPA WHERE outputId=?", (outputId,)).fetchone()
            pas = tuple(round(x, 3) for x in pas)
            wars = cursor.execute("SELECT prob0, prob1, prob2, prob3, prob4, prob5, prob6 FROM Output_HitterWar WHERE outputId=?", (outputId,)).fetchone()
            wars = tuple(round(x,3) for x in wars)
            
            statsData = cursor.execute("SELECT LevelId, LeagueId, TeamId, PA, AVG, OBP, SLG, ISO, wOBA, HRPerc, BBPerc, KPerc, SBRate, SBPerc FROM Player_Hitter_MonthAdvanced WHERE mlbId=? AND year=? AND month=? ORDER BY LevelId ASC", (id,year,month)).fetchall()
            stats = []
            for s in statsData:
                s = tuple(round(x,3) for x in s)
                stats.append(list(s))
            this_model["data"].append({"levels":list(levels), "pas":list(pas), "war":list(wars), "stats":stats, "year":year, "month":month})
            
        data["models_hitter"].append(this_model)
            
    
    first_name, last_name, draft_pick, signing_year = cursor.execute("SELECT DISTINCT useFirstName, useLastName, draftPick, signingYear FROM Player WHERE mlbId=?", (id,)).fetchone()
    data["first"] = first_name
    data["last"] = last_name
    data["draft"] = draft_pick
    data["year"] = signing_year
    
    json_data = json.dumps(data, indent=2)
    with open(f"Hitters/{id}.json", "w") as file:
        file.write(json_data)