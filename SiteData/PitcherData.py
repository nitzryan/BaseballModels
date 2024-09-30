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
    for n, (model,) in enumerate(models):
        this_model = {"name":model, "data":[], "tainted":False}
        resultData = cursor.execute("SELECT year, month FROM Output_AggregatePitcherWar WHERE mlbId=? AND model=? ORDER BY year ASC, month ASC", (id, model)).fetchall()
        for year, month in resultData:
            wars = cursor.execute("SELECT prob0, prob1, prob2, prob3, prob4, prob5, prob6 FROM Output_AggregatePitcherWar WHERE mlbId=? AND model=? AND year=? AND month=?", (id, model, year, month)).fetchone()
            wars = tuple(round(x * 100, 1) for x in wars)
            
            
            this_model["data"].append({"war":list(wars), "year":year, "month":month})
            
        data["models_pitcher"].append(this_model)
            
    all_stats = []
    last_year = 0
    statsData = cursor.execute("SELECT DISTINCT year, month FROM Player_Pitcher_MonthAdvanced WHERE mlbId=? ORDER BY year ASC, month ASC, levelId ASC, teamId ASC", (id,)).fetchall()
    for year, month in statsData:
        monthStats = cursor.execute('''
                                SELECT a.LevelId, a.LeagueId, a.TeamId, a.Outs, a.GBRatio, a.ERA, a.FIP, a.KPerc, a.BBPerc, gl.HR, a.wOBA 
                                FROM Player_Pitcher_MonthAdvanced AS a
                                INNER JOIN Player_Pitcher_GameLog AS gl ON a.mlbId=gl.mlbId AND a.Year=gl.Year AND a.Month=gl.Month AND a.LevelId=gl.Level AND a.TeamId=gl.TeamId
                                WHERE a.mlbId=? AND a.year=? AND a.month=?
                                GROUP BY LevelId
                                ORDER BY LevelId DESC''', (id,year,month)).fetchall()
        stats = []
        
        if year != last_year:
            yearStatsData = cursor.execute("SELECT LevelId, LeagueId, TeamId, Outs, GBRatio, ERA, FIP, KPerc, BBPerc, HR, wOBA FROM Player_Pitcher_YearAdvanced WHERE mlbId=? AND year=? ORDER BY LevelId DESC", (id,year)).fetchall()
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
    data["player_type"] = "P"
    
    birth_year, birth_month, birth_date = cursor.execute("SELECT birthYear, birthMonth, birthDate FROM Player WHERE mlbId=? LIMIT 1", (id,)).fetchone()
    data["birth_year"] = birth_year
    data["birth_month"] = birth_month
    data["birth_date"] = birth_date
    
    # Generate Data for end of rookie and service eligibility
    try:
        rookie_year, rookie_month = cursor.execute("SELECT year, month FROM Player_RookieEligibility WHERE mlbId=?", (id,)).fetchone()
        data["rookie_year"] = rookie_year
        data["rookie_month"] = rookie_month
    except:
        data["rookie_year"] = 0
        data["rookie_month"] = 0
    
    service_end_year = cursor.execute("SELECT serviceEndYear FROM Player_CareerStatus WHERE mlbId=? AND isPrimaryPosition=?", (id, 1)).fetchone()
    if service_end_year is None:
        data["service_end_year"] = 0
    else:
        data["service_end_year"] = service_end_year
    
    data["bucket_names"] = ["<= 0", "0-1", "1-5", "5-10", "10-15", "15-20", "20+"]
    data["bucket_values"] =[0, 0.005, 0.03, 0.075, 0.15, 0.25, 0.35]
    
    json_data = json.dumps(data, indent=2)
    with open(f"../../../ProspectRankingsSite2/public/assets/player_data/{id}.json", "w") as file:
        file.write(json_data)