import sqlite3
from tqdm import tqdm
import requests
import json
from Constants import SPORT_IDS

def Update_Parent_Map(db : sqlite3.Connection, year : int) -> None:
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Team_OrganizationMap WHERE year=?", (year,))
    db.commit()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    
    for sport_id in tqdm(SPORT_IDS, desc="Updating Parent Map", leave=False):
        if sport_id == 1 or sport_id == 17:
            continue
        
        response = requests.get(f"https://statsapi.mlb.com/api/v1/teams?sportIds={sport_id}&season={year}")
        if response.status_code != 200:
            #print(f"Error reading Parent Org Map For {year}-{sport_id}: {response.text}")
            continue
        
        json_data = response.json()
        
        try:
            teams = json_data["teams"]
        except: # sport id not found
            continue
        
        for team in teams:
            try :
                id = team["id"]
                parent_id = team["parentOrgId"]
                cursor.execute("INSERT INTO Team_OrganizationMap VALUES(?,?,?)", (id, year, parent_id))
            except: # Team doesn't have a parent org
                continue
            
    cursor.execute("END TRANSACTION")
    db.commit()
        
def Update_Parents(db : sqlite3.Connection) -> None:
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Team_Parents")
    db.commit()
    cursor = db.cursor()
    
    response = requests.get(f"https://statsapi.mlb.com/api/v1/teams?sportIds=1")
    json_data = response.json()
    teams = json_data["teams"]
    for team in teams:
        id = team["id"]
        name = team["name"]
        abbr = team["abbreviation"]
        cursor.execute("INSERT INTO Team_Parents VALUES(?,?,?)", (id, abbr, name))
        
    # Team 11 respresents the default team
    cursor.execute("INSERT INTO Team_Parents VALUES(?,?,?)", (11, "_", "None"))
    db.commit()