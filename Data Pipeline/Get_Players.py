import sqlite3
import requests
from tqdm import tqdm
import threading

from Constants import SPORT_IDS, MEXICAN_LEAGUE_ID

# Used for multithreading, shouldn't use outside of this file
NUM_THREADS = 16
threadOutputs = [[]] * NUM_THREADS
threadCompleteCounts = [0] * NUM_THREADS

def _ReadPlayer(mlbId, threadIdx):
    response = requests.get(f"https://statsapi.mlb.com/api/v1/people/{mlbId}?hydrate=currentTeam,team,stats(type=[yearByYear](team(league)),leagueListId=mlb_milb)&site=en")
    if response.status_code != 200:
        print(f"Status code {response.status_code} for {mlbId}")
        return
    
    try:
        response = response.json()
        person = response["people"][0]
        useFirstName = person["useName"]
        useLastName = person["useLastName"]
        bats = person["batSide"]["code"]
        throws = person["pitchHand"]["code"]
        birthdateFormatted = person["birthDate"]
        birthYear, birthMonth, birthDate = birthdateFormatted.split("-")
        global threadOutputs
        threadOutputs[threadIdx].append((mlbId, useFirstName, useLastName, bats, throws, birthYear, birthMonth, birthDate))
        
    except Exception as e:
        print(f"Exception {e} for {mlbId}")
        return
    
def _ReadPlayers(threadIdx, unsetPlayers):
    global threadCompleteCounts
    for mlbId, in unsetPlayers[threadIdx * len(unsetPlayers) // NUM_THREADS : (threadIdx + 1) * len(unsetPlayers) // NUM_THREADS]:
        _ReadPlayer(mlbId, threadIdx)
        threadCompleteCounts[threadIdx] += 1

def _Get_Players_Through_Stats(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()
    
    for sportId in tqdm(SPORT_IDS, desc="Updating Players based on Stats for Levels", leave=False):
        playersToInsert = []
        for position in ["hitting","pitching"]:
            #print(f"Getting players for Year={year} SportId={sportId} Position={position}")
            response = requests.get(f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={year}&sportId={sportId}&stats=season&group={position}&gameType=R&limit=5000&offset=0&sortStat=homeRuns&order=desc")
            if response.status_code != 200:
                print(f"Code {response.status_code} for Year={year} and sportId={sportId} and position={position}")
                continue
            
            responseJson = response.json()
            jsonPlayers = responseJson['stats']
            for player in tqdm(jsonPlayers, desc="Updating Players from JSON", leave=False):
                try:
                    if player['leagueId'] == MEXICAN_LEAGUE_ID:
                        continue
                    if cursor.execute(f"SELECT COUNT(*) FROM Player WHERE mlbId='{player['playerId']}' AND position='{position}'").fetchone()[0] > 0:
                        continue
                    playersToInsert.append((player["playerId"], position))
                except:
                    pass
                
        cursor.execute("BEGIN TRANSACTION")
        cursor.executemany("INSERT INTO Player('mlbId','position') VALUES(?,?)", playersToInsert)
        cursor.execute("END TRANSACTION")
        db.commit()
        cursor = db.cursor()

def _Get_Player_Bios(db : sqlite3.Connection):
    cursor = db.cursor()
    unsetPlayers = cursor.execute("SELECT DISTINCT mlbId FROM Player WHERE birthYear IS NULL").fetchall()
    threads = []
    for i in range(NUM_THREADS):
        thread = threading.Thread(target=_ReadPlayers, args=[i, unsetPlayers])
        threads.append(thread)
        thread.start()
        
    progressBar = tqdm(total=len(unsetPlayers), desc="Getting Player Bio Info", leave=False)

    # Start progress bar
    keepTimerRunning = True
    def UpdateTimer():
        if keepTimerRunning:
            threading.Timer(5.0, UpdateTimer).start()
        
        count = 0
        global threadCompleteCounts
        for i in range(NUM_THREADS):
            count += threadCompleteCounts[i]
        
        progressBar
        progressBar.n = count
        progressBar.last_print_n = progressBar.n
        progressBar.refresh()
        
    UpdateTimer()

    for thread in threads:
        thread.join()
        
    keepTimerRunning = False
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    for threadOutput in threadOutputs:
        for mlbId, useFirstName, useLastName, bats, throws, birthYear, birthMonth, birthDate in threadOutput:
        # for data in threadOutput:
            # cursor.execute(f"UPDATE Player SET birthYear='{birthYear}', birthMonth='{birthMonth}', birthDate='{birthDate}', bats='{bats}', throws='{throws}', useFirstName='{useFirstName}', useLastName='{useLastName}' WHERE mlbId='{mlbId}' AND position='{position}'")
            cursor.execute("UPDATE Player SET birthYear=?, birthMonth=?, birthDate=?, bats=?, throws=?, useFirstName=?,  useLastName=? WHERE mlbId=?", (birthYear, birthMonth, birthDate, bats, throws, useFirstName, useLastName, mlbId))
    cursor.execute("END TRANSACTION")
    db.commit()

def _Get_DraftData(db : sqlite3.Connection, year : int):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    response = requests.get(f"https://statsapi.mlb.com/api/v1/draft/{year}")
    json = response.json()
    for rounds in json["drafts"]["rounds"]: # This will be empty if the draft hasn't happened
        for pick in rounds["picks"]:
            try:
                draftPick = pick["pickNumber"]
                mlbId = pick["person"]["id"]
                player_exists = cursor.execute("SELECT COUNT(*) FROM Player WHERE mlbId=?", (mlbId,)).fetchone()[0] > 0
                if not player_exists:
                    position_code = pick["person"]["primaryPosition"]["code"]
                    if position_code == 1:
                        cursor.execute("INSERT INTO Player('mlbId','position') VALUES(?,?)", (mlbId, 'pitching'))
                    else:
                        cursor.execute("INSERT INTO Player('mlbId','position') VALUES(?,?)", (mlbId, 'hitting'))
                cursor.execute("UPDATE Player SET draftPick=?, signingYear=?, signingMonth='7', signingDate='1' WHERE mlbId=?", (draftPick, year, mlbId))
            except:
                continue
        
    cursor.execute("END TRANSACTION")
    db.commit()

def Get_Players(db : sqlite3.Connection, year : int, draft_only : bool = False ) -> None:
    _Get_DraftData(db, year)
    if not draft_only:
        _Get_Players_Through_Stats(db, year)
        _Get_Player_Bios(db)
    