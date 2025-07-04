import sqlite3
import requests
from tqdm import tqdm
import threading
import traceback
import time

from Constants import SPORT_IDS, MEXICAN_LEAGUE_ID, DSL_LEAGUE_ID

NUM_THREADS = 16
threadCompleteCounts = [0] * NUM_THREADS
threadData = [[] for _ in range(NUM_THREADS)]

def _GenerateHitterYearGameLogs(db : sqlite3.Connection, threadIdx, mlbId, year, startMonth=0, endMonth=13):
    global threadData
    cursor = db.cursor()
    # Check if data already exists
    if cursor.execute(f"SELECT COUNT(*) FROM Player_Hitter_GameLog WHERE mlbId='{mlbId}' AND Year='{year}' AND (Month ='{startMonth}' OR Month='{endMonth}')").fetchone()[0] > 0:
        return
    
    response = requests.get(f"https://statsapi.mlb.com/api/v1/people/{mlbId}/stats?stats=gameLog&leagueListId=mlb_milb&group=hitting&gameType=R&sitCodes=1,2,3,4,5,6,7,8,9,10,11,12&hydrate=team&language=en&season={year}")
    if response.status_code != 200:
        time.sleep(2)
        response = requests.get(f"https://statsapi.mlb.com/api/v1/people/{mlbId}/stats?stats=gameLog&leagueListId=mlb_milb&group=hitting&gameType=R&sitCodes=1,2,3,4,5,6,7,8,9,10,11,12&hydrate=team&language=en&season={year}")
        if response.status_code != 200:
            print(f"Status code {response.status_code} for id={mlbId} year={year}")
            return
    try:
        # Make sure any data exists
        try:
            games = response.json()["stats"][0]["splits"]
        except:
            return
        
        # Add each game
        for game in games:
            if game["team"]["league"]["id"] == MEXICAN_LEAGUE_ID:
                continue
            
            _, month, day = game["date"].split("-")
            if int(month) < int(startMonth) or int(month) > int(endMonth):
                continue
            
            gameId = int(game["game"]["gamePk"])
            if len(game["positionsPlayed"]) > 0:
                pos = int(game["positionsPlayed"][0]["code"])
            else:
                pos = 10
            isHomeGame = int(game["isHome"])
            if isHomeGame:
                homeTeamId = int(game["team"]["id"])
            else:
                homeTeamId = int(game["opponent"]["id"])
            level = int(game["sport"]["id"])
            stats = game["stat"]
            ab = int(stats["atBats"])
            h = int(stats["hits"])
            double = int(stats["doubles"])
            triple = int(stats["triples"])
            hr = int(stats["homeRuns"])
            k = int(stats["strikeOuts"])
            bb = int(stats["baseOnBalls"])
            hbp = int(stats["hitByPitch"])
            sb = int(stats["stolenBases"])
            cs = int(stats["caughtStealing"])
            teamId = game["team"]["id"]
            leagueId = game["league"]["id"]
            threadData[threadIdx].append((gameId, int(mlbId), int(day), int(month), int(year), ab, h, double, triple, hr, k, bb, sb, cs, hbp, pos, level, homeTeamId, teamId, leagueId))
            
    except Exception as e:
        print(f"Exception On Id={mlbId} year={year}: {e}")
        traceback.print_exc()
        return 
  
def _HitterGameLogThreadFunction(data, threadIdx, year, month):
    db : sqlite3.Connection = sqlite3.connect("../BaseballStats.db")
    global threadCompleteCounts
    for d in data:
        try:
            mlbId = int(d)
            if month == 4:
                _GenerateHitterYearGameLogs(db, threadIdx, mlbId, year, 3, 4)
            elif month == 9:
                _GenerateHitterYearGameLogs(db, threadIdx, mlbId, year, 9, 10)
            else:
                _GenerateHitterYearGameLogs(db, threadIdx, mlbId, year, month, month)
        except Exception as e:
            print(f"Error for id={mlbId} year={year}: {e}")
        finally:
            threadCompleteCounts[threadIdx] += 1
    
def _Get_All_HitterLogs(db : sqlite3.Connection, playerYearDict, year, month):
    ids = list(playerYearDict.keys())

    threads = []
    for i in range(NUM_THREADS):
        data = ids[len(ids) * i // NUM_THREADS : len(ids) * (i + 1) // NUM_THREADS]
        thread = threading.Thread(target=_HitterGameLogThreadFunction, args=[data, i, year, month])
        threads.append(thread)
        thread.start()
        
    progressBar = tqdm(total=len(ids), desc="Hitter Game Logs", leave=False)

    # Start progress bar
    keepTimerRunning = True
    def UpdateTimer():
        if keepTimerRunning:
            threading.Timer(5.0, UpdateTimer).start()
        
        count = 0
        global threadCompleteCounts
        for i in range(NUM_THREADS):
            count += threadCompleteCounts[i]
        
        progressBar.n = count
        progressBar.last_print_n = progressBar.n
        progressBar.refresh()
        
    UpdateTimer()

    for thread in threads:
        thread.join()
        
    keepTimerRunning = False
    
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    for data in threadData:
        cursor.executemany("INSERT INTO Player_Hitter_GameLog('gameId', 'mlbId', 'Day', 'Month','Year','AB','H','2B','3B','HR','K','BB','SB','CS','HBP','Position','Level','HomeTeamId', 'TeamId', 'LeagueId') VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
    cursor.execute("END TRANSACTION")
    db.commit()
    progressBar.close()

def Update_Hitter_GameLogs(db : sqlite3.Connection, year : int, month : int) -> None:
    # Create dictionary of player ids-year combination
    # Don't need any data, just a lookup table
    playerYearDict = {}
    
    for sportId in tqdm(SPORT_IDS, desc="Getting Hitter Ids SportID", leave=False):
        response = requests.get(f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={year}&sportId={sportId}&stats=season&group=hitting&gameType=R&limit=5000&offset=0&sortStat=homeRuns&order=desc")
        if response.status_code != 200:
            time.sleep(2)
            response = requests.get(f"https://bdfed.stitch.mlbinfra.com/bdfed/stats/player?stitch_env=prod&season={year}&sportId={sportId}&stats=season&group=hitting&gameType=R&limit=5000&offset=0&sortStat=homeRuns&order=desc")
            if response.status_code != 200:
                print(f"Code {response.status_code} for Year={year} and sportId={sportId}")
                continue

        responseJson = response.json()
        jsonPlayers = responseJson['stats']
        for player in jsonPlayers:
            playerString = str(player["playerId"])
            playerYearDict[playerString] = None
            
    # Go through each player in a multithreaded fashion
    _Get_All_HitterLogs(db, playerYearDict, year, month)
    
    # Update DSL to be 17 instead of 16 which it is returned as
    # This is because DSL is below Complex ball, but both are reported as 16
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("UPDATE Player_Hitter_GameLog SET Level=? WHERE LeagueId=?", (17, DSL_LEAGUE_ID))
    cursor.execute("DELETE FROM Player_Hitter_GameLog WHERE Level>?", (17,))
    cursor.execute("END TRANSACTION")
    db.commit()