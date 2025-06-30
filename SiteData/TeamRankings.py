import sqlite3
import json
import sys

def GenerateTeamRankings(db : sqlite3.Connection, year: int, month: int, model_names: list[str]):
    cursor = db.cursor()
    teams = cursor.execute("SELECT id FROM Team_Parents WHERE id>11").fetchall()
    
    model_sums = {"models":[]}
    for model_name in model_names:
        team_sums = []
        for team_id, in teams:
            s = f"../../../ProspectRankingsSite2/public/assets/rankings_team/{team_id}-{year}-{month}.json"
            with open(s, "r") as file:
                data = file.read().strip()
                team_dict = json.loads(data)
                for model in team_dict["models"]:
                    if model["name"] != model_name:
                        continue
                    
                    team_war = 0
                    for player in model["players"]:
                        team_war += player["war"]
                        
                    team_sums.append({"id":team_id, "war":round(team_war, 1)})
                
        def _sortFunction(item):
            return item["war"]
        team_sums = sorted(team_sums, key=_sortFunction, reverse=True)
        sums_dict = {"name":model_name, "teams":team_sums}
        model_sums["models"].append(sums_dict)
        
    sums_json = json.dumps(model_sums, indent=2)
    with open(f"../../../ProspectRankingsSite2/public/assets/rankings_team_comparison/{year}-{month}.json", "w") as file:
        file.write(sums_json)
    
if __name__ == '__main__':
    db = sqlite3.connect('../BaseballStats.db')
    for year in range(2021, 2026):
        for month in range(4, 10):
            if year == 2025 and month >= 6:
                continue
            GenerateTeamRankings(db, year, month, ["RNN","LSTM"])