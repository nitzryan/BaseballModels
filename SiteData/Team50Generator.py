import sqlite3
import json
from tqdm import tqdm

def _War_Function(prob0, prob1, prob2, prob3, prob4, prob5, prob6):
    return 0.5 * prob1 + 3 * prob2 + 7.5 * prob3 + 15 * prob4 + 25 * prob5 + 35 * prob6

def Generate_Team_50(db : sqlite3.Connection, year : int, month : int) -> None:
    cursor = db.cursor()
    db.create_function("warCalculation", 7, _War_Function)
    teams = cursor.execute("SELECT id, name FROM Team_Parents WHERE id>11").fetchall()
    
    for team_id, team_name in tqdm(teams, desc="Team Top 50s", leave=False):
        data = {"year":year, "month":month, "team":team_name, "models":[]}
        for model_name, hitter_idx, pitcher_idx in (("RNN", 1,2), ("LSTM", 3,4)):
            players = cursor.execute('''
                                    SELECT opw.mlbId, warCalculation(prob0, prob1, prob2, prob3, prob4, prob5, prob6)
                                    FROM Output_PlayerWar AS opw
                                    INNER JOIN Player_OrgMap AS pom
                                    ON pom.mlbId=opw.mlbId
                                    AND pom.year=opw.year
                                    AND pom.month=opw.month
                                    WHERE (modelIdx=? OR modelIdx=?)
                                    AND opw.year=? AND opw.month=?
                                    AND pom.parentOrgId=?
                                    ORDER BY warCalculation(prob0, prob1, prob2, prob3, prob4, prob5, prob6) DESC
                                    LIMIT 50
                                    ''', (hitter_idx, pitcher_idx, year, month, team_id)).fetchall()
            
            if len(players) == 0:
                return
            
            model_map = {"name":model_name, "players":[]}
            for id, war in players:
                firstName, lastName = cursor.execute("SELECT DISTINCT useFirstName, useLastName FROM Player WHERE mlbId=?", (id,)).fetchone()
                this_player = {"first":firstName, "last":lastName, "id":id, "war":round(war, 1)}
                model_map["players"].append(this_player)
            
            data["models"].append(model_map)
        
        json_maps = json.dumps(data, indent=2)
        with open(f"../../../ProspectRankingsSite2/public/assets/rankings_team/{team_id}-{year}-{month}.json", "w") as file:
            file.write(json_maps)
        
if __name__ == "__main__":
    db = sqlite3.connect("../BaseballStats.db")
    for year in tqdm(range(2021, 2026), desc="Years", leave=False):
        for month in tqdm(range(4, 10), desc="Months", leave=False):
            Generate_Team_50(db, year, month)