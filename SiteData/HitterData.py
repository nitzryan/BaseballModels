import sqlite3
from tqdm import tqdm
import json

db = sqlite3.connect("../BaseballStats.db")
cursor = db.cursor()

# Go player by player
ids = cursor.execute('''SELECT DISTINCT o.mlbId 
                     FROM Output_PlayerWar AS o
                     INNER JOIN Model_TrainingHistory AS m
                     ON o.modelIdx = m.ModelIdx
                     WHERE m.IsHitter="1"''').fetchall()
for id, in tqdm(ids, desc="Creating Hitter JSON Data"):
    data = {"models_hitter":[]}
    # Go Model by Model
    models = cursor.execute('''
                            SELECT DISTINCT opw.modelIdx
                            FROM Output_PlayerWar AS opw
                            INNER JOIN Model_TrainingHistory AS mth
                            ON opw.ModelIdx=mth.ModelIdx
                            WHERE opw.mlbId=?
                            AND mth.IsHitter=?''', (id,True)).fetchall()
    for n, (model,) in enumerate(models):
        model_full_name = cursor.execute("SELECT ModelName FROM Model_TrainingHistory WHERE ModelIdx=? LIMIT 1", (model,)).fetchone()[0].split('_H_')[0]
        this_model = {"name":model_full_name, "data":[], "tainted":False}
        resultData = cursor.execute("SELECT year, month FROM Output_PlayerWar WHERE mlbId=? AND modelIdx=? ORDER BY year ASC, month ASC", (id, model)).fetchall()
        for year, month in resultData:
            wars = cursor.execute("SELECT prob0, prob1, prob2, prob3, prob4, prob5, prob6 FROM Output_PlayerWar WHERE mlbId=? AND modelIdx=? AND year=? AND month=?", (id, model, year, month)).fetchone()
            wars = tuple(round(x * 100, 1) for x in wars)
            this_model["data"].append({"war":list(wars), "year":year, "month":month})
            
        data["models_hitter"].append(this_model)
            
    statsData = cursor.execute("SELECT DISTINCT year, month FROM Player_Hitter_MonthAdvanced WHERE mlbId=? ORDER BY year ASC, month ASC", (id,)).fetchall()
    
    all_stats = []
    last_year = 0
    for year, month in statsData:
        monthStats = cursor.execute('''
                                SELECT a.LevelId, a.LeagueId, a.TeamId, a.PA, a.AVG, a.OBP, a.SLG, a.ISO, a.wOBA, SUM(gl.HR), a.BBPerc, a.KPerc, SUM(gl.SB), SUM(gl.CS)
                                FROM Player_Hitter_MonthAdvanced AS a
                                INNER JOIN Player_Hitter_GameLog AS gl ON a.mlbId=gl.mlbId AND a.Year=gl.Year AND a.Month=gl.Month AND a.LevelId=gl.Level AND a.TeamId=gl.TeamId
                                WHERE a.mlbId=? AND a.year=? AND a.month=? 
                                GROUP BY LevelId
                                ORDER BY LevelId DESC''', (id,year,month)).fetchall()
        stats = []
        
        if year != last_year:
            yearStatsData = cursor.execute("SELECT LevelId, LeagueId, TeamId, PA, AVG, OBP, SLG, ISO, wOBA, HR, BBPerc, KPerc, SB, CS FROM Player_Hitter_YearAdvanced WHERE mlbId=? AND year=? ORDER BY LevelId DESC", (id,year)).fetchall()
            yearStats = []
            for ysd in yearStatsData:
                ysd = tuple(round(x,3) for x in ysd)
                yearStats.append(list(ysd))
            all_stats.append({"stats":yearStats, "year":year, "month":0})
            last_year = year
        
        for s in monthStats:
            if s[0] is not None:
                s = tuple(round(x,3) for x in s)
                stats.append(list(s))
        if len(stats) > 0:
            all_stats.append({"stats":stats, "year":year, "month":month})
    
    first_name, last_name, draft_pick, signing_year = cursor.execute("SELECT DISTINCT useFirstName, useLastName, draftPick, signingYear FROM Player WHERE mlbId=?", (id,)).fetchone()
    data["first"] = first_name
    data["last"] = last_name
    data["draft"] = draft_pick
    data["year"] = signing_year
    data["stats"] = all_stats
    
    birth_year, birth_month, birth_date = cursor.execute("SELECT birthYear, birthMonth, birthDate FROM Player WHERE mlbId=? LIMIT 1", (id,)).fetchone()
    data["birth_year"] = birth_year
    data["birth_month"] = birth_month
    data["birth_date"] = birth_date
    data["player_type"] = "H"
    
    # Generate Data for end of rookie and service eligibility
    try:
        rookie_year, rookie_month = cursor.execute("SELECT mlbRookieYear, mlbRookieMonth FROM Player_CareerStatus WHERE mlbId=?", (id,)).fetchone()
        data["rookie_year"] = rookie_year
        data["rookie_month"] = rookie_month
    except:
        data["rookie_year"] = 0
        data["rookie_month"] = 0
    
    service_end_year = cursor.execute("SELECT serviceEndYear FROM Player_CareerStatus WHERE mlbId=? AND isHitter IS NOT NULL", (id,)).fetchone()
    if service_end_year is None:
        data["service_end_year"] = 0
    else:
        data["service_end_year"] = service_end_year
        
    data["bucket_names"] = ["<= 0", "0-1", "1-5", "5-10", "10-20", "20-30", "30+"]
    data["bucket_values"] =[0, 0.005, 0.03, 0.075, 0.15, 0.25, 0.35]
    
    json_data = json.dumps(data, indent=2)
    with open(f"../../../ProspectRankingsSite2/public/assets/player_data/{id}.json", "w") as file:
        file.write(json_data)