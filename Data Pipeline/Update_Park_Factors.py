import sqlite3
from tqdm import tqdm

_gameData = {}
ROLLING_PERIOD = 3
INNING_CUTOFF = 100

def _CheckForKeysAndAddIfNecessary(t, l, level, teamRunCounts):
    if not l in teamRunCounts.keys():
        teamRunCounts[l] = {}
    if not t in teamRunCounts[l].keys():
        teamRunCounts[l][t] = {"home": {"outs":0,"pa":0,"runs":0,"hrs":0}, "away": {"outs":0,"pa":0,"runs":0,"hrs":0}, "level": level, "league": l, "team":t}

def _UpdateGameData(gameId, pa, outs, r, hr, homeTeamId, teamId, leagueId, level):
    global _gameData
    if not gameId in _gameData.keys():
        _gameData[gameId] = {"homeTeamId":homeTeamId, "awayTeamId":0, "r":0,"hr":0,"pa":0,"outs":0, "league":leagueId, "level":level}
    
    gd = _gameData[gameId]
    if homeTeamId != teamId:
        gd["awayTeamId"] = teamId
        
    gd["pa"] += pa
    gd["outs"] += outs
    gd["hr"] += hr
    gd["r"] += r

def _Insert_Park_ScoringData(db : sqlite3.Connection, year : int, teamRunCounts):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    insertionData = []
    for leagueId, teamData in tqdm(teamRunCounts.items(), desc="Inserting Park Scoring Data", leave=False):
        for teamid, parkData in teamData.items():
            valueExists = cursor.execute(f"SELECT COUNT(*) FROM Park_ScoringData WHERE TeamId='{teamid}' AND Year='{year}'").fetchone()[0] > 0
            if valueExists:
                continue
                
            homePA = parkData["home"]["pa"]
            homeOuts = parkData["home"]["outs"]
            homeRuns = parkData["home"]["runs"]
            homeHRs = parkData["home"]["hrs"]
            awayPA = parkData["away"]["pa"]
            awayOuts = parkData["away"]["outs"]
            awayRuns = parkData["away"]["runs"]
            awayHRs = parkData["away"]["hrs"]
            levelId = parkData["level"]
            insertionData.append([teamid, year, levelId, leagueId, homePA, homeOuts, homeRuns, homeHRs, awayPA, awayOuts, awayRuns, awayHRs])
    cursor.executemany("INSERT INTO Park_ScoringData('TeamId','Year','LevelId','LeagueId','HomePA','HomeOuts','HomeRuns','HomeHRs','AwayPA','AwayOuts','AwayRuns','AwayHRs') VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", insertionData)
    cursor.execute("END TRANSACTION")
    db.commit()

def _Calculate_Park_Factors(db : sqlite3.Connection, year : int):
    
    cursor = db.cursor()
    scoringData = cursor.execute("SELECT DISTINCT TeamId FROM Park_ScoringData").fetchall()

    cursor.execute("BEGIN TRANSACTION")

    for team, in tqdm(scoringData, desc="Calculating Park Factors", leave=False):
        parkRunData = cursor.execute(f"SELECT * FROM Park_ScoringData WHERE TeamId='{team}' AND year > '{year - ROLLING_PERIOD}' AND year <= '{year}' ORDER BY Year DESC, HomeOuts DESC").fetchall()
        # Check if data exists
        if len(parkRunData) == 0:
            continue
        
        # Make sure there is some data from the current year
        if parkRunData[0][1] != year:
            continue
        
        # Get current league from first entry
        leagueId = parkRunData[0][2]
        levelId = parkRunData[0][3]
        
        awayOuts = 0
        awayPa = 0
        awayRuns = 0
        awayHRs = 0
        homeOuts = 0
        homePa = 0
        homeRuns = 0
        homeHRs = 0
        
        for _,_,_,_,hpa, ho,hr,hhr,apa,ao,ar,ahr in parkRunData:
            awayOuts += ao
            awayPa += apa
            awayRuns += ar
            awayHRs += ahr
            homeOuts += ho
            homePa += hpa
            homeRuns += hr
            homeHRs += hhr
            
        # Ensure enough data to actually calculate park factors
        if awayOuts * 3 < INNING_CUTOFF or homeOuts * 3 < INNING_CUTOFF:
            continue
        
        runFactor = (homeRuns / homeOuts) / (awayRuns / awayOuts)
        hrFactor = (homeHRs / homePa) / (awayHRs / awayPa)
        params = [(team, leagueId, levelId, year, runFactor, hrFactor)]
        cursor.executemany("INSERT INTO Park_Factors('TeamId','LeagueId','LevelId','Year','RunFactor','HRFactor') VALUES(?,?,?,?,?,?)", params)
        
        
    # There is an outlier year where the first year of a DSL team has a HR factor of 6
    # Due to only hitting 5 away HRs
    # This has a large effect because it is the first year, so it only has 1 value to use
    cursor.execute("UPDATE Park_Factors SET RunFactor='1.2', HRFactor='2' WHERE TeamId='5086' AND Year='2016'")
    cursor.execute("UPDATE Park_Factors SET HRFactor='2' WHERE TeamId='5086' AND Year='2017'")
    cursor.execute("END TRANSACTION")
    db.commit()
   
def _Calculate_Level_Factors(db : sqlite3.Connection, year : int):
    cursor = db.cursor()
    data = []
    totalOuts = 0
    totalPa = 0
    totalRuns = 0
    totalHrs = 0
    for level in cursor.execute(f"SELECT DISTINCT LevelId FROM Park_Factors WHERE Year='{year}'").fetchall():
        level = level[0]
        # Covid check
        if year == 2020 and level != 1:
            continue
        if year > 2020 and level == 15:
            continue
        pa, outs, runs, hrs = cursor.execute(f"SELECT SUM(HomePa), SUM(HomeOuts), SUM(HomeRuns), SUM(HomeHRs) FROM Park_ScoringData WHERE Year='{year}' AND LevelId='{level}'").fetchone()
        
        data.append((level, runs/outs, hrs/pa))
        totalOuts += outs
        totalPa += pa
        totalRuns += runs
        totalHrs += hrs
        
    baseRunFactor = totalRuns / totalOuts
    baseHRFactor = totalHrs / totalPa
    cursor.execute("BEGIN TRANSACTION")
    for d in data:
        if cursor.execute(f"SELECT COUNT(*) FROM Level_Factors WHERE LevelId='{d[0]}' AND Year='{year}'").fetchone()[0] == 0:
            cursor.execute("INSERT INTO Level_Factors('LevelId','Year','RunFactor','HRFactor') VALUES(?,?,?,?)", [d[0], year, d[1] / baseRunFactor, d[2] / baseHRFactor])
    cursor.execute("END TRANSACTION")
    db.commit()

def _Calculate_League_Factors(db : sqlite3.Connection, year : int):
    cursor = db.cursor()
    leagues = cursor.execute("SELECT DISTINCT LeagueId FROM Park_Factors").fetchall()
    yearlyLeagueData = []
    for league in leagues:
        league = league[0]
        
        data = cursor.execute(f"SELECT HomePa, HomeOuts, HomeRuns, HomeHRs, LevelId FROM Park_ScoringData WHERE LeagueId='{league}' AND Year='{year}'").fetchall()
        homePa = 0
        homeOuts = 0
        homeRuns = 0
        homeHRs = 0
        for pa, outs, runs, hrs, _ in data:
            homePa += pa
            homeOuts += outs
            homeRuns += runs
            homeHRs += hrs
            
        # if league == 134:
        #     print(f"Year={year} HomeInnings={homeInnings}")
            
        if len(data) > 0:
            yearlyLeagueData.append((league, homePa, homeOuts, homeRuns, homeHRs, data[0][4]))
        
    # Get Average of all leagues
    totalOuts = 0
    totalPa = 0
    totalRuns = 0
    totalHr = 0
    for _, pa, outs, r, hr, _ in yearlyLeagueData:
        totalPa += pa
        totalOuts += outs
        totalRuns += r
        totalHr += hr
        
    # Normalize each league to the average
    cursor.execute("BEGIN TRANSACTION")
    dbData = []
    for leagueId, leaguePa, leagueOuts, leagueRuns, leagueHRs, levelId in yearlyLeagueData:
        levelRunFactor, levelHRFactor = cursor.execute(f"SELECT RunFactor, HRFactor FROM Level_Factors WHERE LevelId='{levelId}' AND Year='{year}'").fetchone()
        if leagueOuts * 3 > INNING_CUTOFF:
            dbData.append((leagueId, year, (leagueRuns / leagueOuts)/(totalRuns / totalOuts)/levelRunFactor, (leagueHRs / leaguePa)/(totalHr / totalPa)/levelHRFactor))
        else:
            dbData.append((leagueId, year, 1, 1))
    
    cursor.executemany("INSERT INTO League_Factors('LeagueId','Year','RunFactor','HRFactor') VALUES(?,?,?,?)", dbData)
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Clear_Data(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Park_Factors WHERE year=?", (year,))
    cursor.execute("DELETE FROM Park_ScoringData WHERE year=?", (year,))
    cursor.execute("DELETE FROM League_Factors WHERE year=?", (year,))
    cursor.execute("DELETE FROM Level_Factors WHERE year=?", (year,))
    db.commit()
    cursor = db.cursor()

def _Get_TeamRunCounts():
    teamRunCounts = {}
    for val in _gameData.values():
        # Get values
        leagueId = val["league"]
        homeTeamId = val["homeTeamId"]
        awayTeamId = val["awayTeamId"]
        runs = val["r"]
        hrs = val["hr"]
        pa = val["pa"]
        outs = val["outs"]
        level = val["level"]
        
        if awayTeamId == 0:
            continue
        
        # Update for each time
        _CheckForKeysAndAddIfNecessary(homeTeamId, leagueId, level, teamRunCounts)
        teamRunCounts[leagueId][homeTeamId]["home"]["outs"] += outs
        teamRunCounts[leagueId][homeTeamId]["home"]["pa"] += pa
        teamRunCounts[leagueId][homeTeamId]["home"]["runs"] += runs
        teamRunCounts[leagueId][homeTeamId]["home"]["hrs"] += hrs
        
        _CheckForKeysAndAddIfNecessary(awayTeamId, leagueId, level, teamRunCounts)
        teamRunCounts[leagueId][awayTeamId]["away"]["outs"] += outs
        teamRunCounts[leagueId][awayTeamId]["away"]["pa"] += pa
        teamRunCounts[leagueId][awayTeamId]["away"]["runs"] += runs
        teamRunCounts[leagueId][awayTeamId]["away"]["hrs"] += hrs
        
    return teamRunCounts

def _Update_GamesData(db : sqlite3.Connection, year : int):
    cursor = db.cursor()
    gameLogs = cursor.execute('''SELECT gameId, level, homeTeamId, TeamId, LeagueId, battersFaced, outs, r, hr 
                              FROM Player_Pitcher_GameLog
                              WHERE year=?''', (year,)).fetchall()
    for gameId, level, homeTeamId, teamId, leagueId, battersFaced, outs, r, hr in tqdm(gameLogs, desc="Game Logs for Park Factors", leave=False):
        _UpdateGameData(gameId, battersFaced, outs, r, hr, homeTeamId, teamId, leagueId, level)

def Update_Park_Factors(db : sqlite3.Connection, year : int) -> None:    
    global _gameData
    _gameData = {}
    _Clear_Data(db, year)
    _Update_GamesData(db, year)
    teamRunCounts = _Get_TeamRunCounts()
    _Insert_Park_ScoringData(db, year, teamRunCounts)
    _Calculate_Park_Factors(db, year)
    _Calculate_Level_Factors(db, year)
    _Calculate_League_Factors(db, year)