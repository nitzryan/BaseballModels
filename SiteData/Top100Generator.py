import sqlite3
import json
from tqdm import tqdm

def _War_Function(prob0, prob1, prob2, prob3, prob4, prob5, prob6):
    return 0.5 * prob1 + 3 * prob2 + 7.5 * prob3 + 15 * prob4 + 25 * prob5 + 35 * prob6

def Generate_Top_100(db : sqlite3.Connection, year : int, month : int) -> None:
    cursor = db.cursor()
    db.create_function("warCalculation", 7, _War_Function)
    data = {"year":year, "month":month, "models":[]}
    
    for model_name, hitter_idx, pitcher_idx in (("RNN", 1,2), ("LSTM", 3,4)):
        players = cursor.execute('''
                                 SELECT mlbId, warCalculation(prob0, prob1, prob2, prob3, prob4, prob5, prob6)
                                 FROM Output_PlayerWar
                                 WHERE (modelIdx=? OR modelIdx=?)
                                 AND year=? AND month=?
                                 ORDER BY warCalculation(prob0, prob1, prob2, prob3, prob4, prob5, prob6) DESC
                                 LIMIT 100
                                 ''', (hitter_idx, pitcher_idx, year, month)).fetchall()
        model_map = {"name":model_name, "players":[]}
        for id, war in players:
            firstName, lastName = cursor.execute("SELECT DISTINCT useFirstName, useLastName FROM Player WHERE mlbId=?", (id,)).fetchone()
            try:
                org_id = cursor.execute('''
                                        SELECT parentOrgId 
                                        FROM Player_OrgMap 
                                        WHERE mlbId=? 
                                        AND (year=? AND month<=?)
                                        OR (year<?)
                                        ORDER BY Year DESC, Month DESC''', (id, year, month, year)).fetchone()[0]
            except:
                print(f"No Org Id Found for {firstName} {lastName} {year} {month}")
                org_id = 11
            this_player = {"first":firstName, "last":lastName, "id":id, "war":round(war, 1), "orgId":org_id}
            model_map["players"].append(this_player)
        
        data["models"].append(model_map)
    
    json_maps = json.dumps(data, indent=2)
    with open(f"../../../ProspectRankingsSite2/public/assets/rankings_100/{year}-{month}.json", "w") as file:
        file.write(json_maps)
        
if __name__ == "__main__":
    db = sqlite3.connect("../BaseballStats.db")
    for year in tqdm(range(2021, 2025), desc="Years", leave=False):
        for month in tqdm(range(4, 10), desc="Months", leave=False):
            Generate_Top_100(db, year, month)