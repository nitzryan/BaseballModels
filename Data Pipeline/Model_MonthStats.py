import sqlite3
from tqdm import tqdm
import numpy as np
     
levelMap = {1:0,11:1,12:2,13:3,14:4,15:5,16:6,17:7}
START_MONTH = 4
LAST_MONTH = 9
        
def _Generate_HitterStats(db : sqlite3.Connection):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("DELETE FROM Model_HitterStats")
    playerData = cursor.execute('''
                                SELECT DISTINCT(m.mlbId), m.lastProspectYear, m.lastProspectMonth, p.birthYear, p.birthMonth, p.birthDate
                                FROM Model_Players AS m
                                INNER JOIN Player AS p on m.mlbId = p.mlbId
                                WHERE m.isHitter='1'
                                ''').fetchall()

    for id, lastYear, lastMonth, birthYear, birthMonth, birthDate in tqdm(playerData, desc="Model Hitter Data", leave=False):
        hittingData = cursor.execute('''
                                    SELECT stats.Year, stats.Month, stats.AB+stats.BB+stats.HBP, stats.LevelId, stats.ParkRunFactor, stats.ParkHRFactor,
                                    r.avgRatio, r.obpRatio, r.isoRatio, r.wOBARatio, r.sbRateRatio, r.sbPercRatio, r.hrPercRatio, r.bbPercRatio, r.kPercRatio,
                                    r.PercC, r.Perc1B, r.Perc2B, r.Perc3B, r.PercSS, r.PercLF, r.PercCF, r.PercRF, r.PercDH
                                    FROM Player_Hitter_MonthStats AS stats
                                    INNER JOIN Player_hitter_MonthlyRatios AS r ON stats.mlbId = r.mlbId AND stats.Year = r.Year AND stats.Month = r.Month AND stats.LevelId = r.Level
                                    WHERE stats.mlbId=?
                                    AND (
                                        stats.Year<?
                                        OR (stats.Year=? AND stats.Month<=?)
                                    )
                                    ORDER BY r.Year ASC, r.Month ASC, r.Level ASC
                                    ''', (id, lastYear, lastYear, lastMonth)).fetchall()
        
        prevYear = 0
        prevMonth = 0
        totalPa = 0
        currentLog = None
        for log in hittingData:
            year = log[0]
            month = log[1]
            # Get data for this month
            thisPa = log[2]
            
            log = np.array(log[3:])
            log[0] = levelMap[log[0]]
            
            if year == prevYear and month == prevMonth: # Take weighted average
                try:
                    currentLog = (thisPa / (thisPa + totalPa)) * log + (totalPa / (thisPa + totalPa)) * currentLog
                except: # A few players that only stole bases
                    currentLog = 0.5 * log + 0.5 * currentLog
                totalPa += thisPa
            
            else: # new month
                if currentLog is not None: # Log all but first for a player
                    cursor.execute("INSERT INTO Model_HitterStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (id, prevYear, prevMonth, (prevYear + prevMonth/12) - (birthYear + birthMonth/12 + birthDate/365), totalPa,
                                    currentLog[0],currentLog[1],currentLog[2],currentLog[3],currentLog[4],currentLog[5],currentLog[6],
                                    currentLog[7],currentLog[8],currentLog[9],currentLog[10],currentLog[11],currentLog[12],currentLog[13],
                                    currentLog[14],currentLog[15],currentLog[16],currentLog[17],currentLog[18],currentLog[19],currentLog[20])
                                )
                totalPa = thisPa
                currentLog = log
                prevYear = year
                prevMonth = month
                
        # Have 1 last entry to do
        if currentLog is not None:
            cursor.execute("INSERT INTO Model_HitterStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (id, prevYear, prevMonth, (prevYear + prevMonth/12) - (birthYear + birthMonth/12 + birthDate/365), totalPa,
                                        currentLog[0],currentLog[1],currentLog[2],currentLog[3],currentLog[4],currentLog[5],currentLog[6],
                                        currentLog[7],currentLog[8],currentLog[9],currentLog[10],currentLog[11],currentLog[12],currentLog[13],
                                        currentLog[14],currentLog[15],currentLog[16],currentLog[17],currentLog[18],currentLog[19],currentLog[20])
                                )

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Fill_HitterGaps(db : sqlite3.Connection):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    
    ids = cursor.execute("SELECT DISTINCT mlbId FROM Model_HitterStats").fetchall()
    for (id,) in tqdm(ids, desc="Model Hitter Gaps", leave=False):
        statDates = cursor.execute("SELECT Year, Month FROM Model_HitterStats WHERE mlbId=? ORDER BY Year ASC, Month ASC", (id,)).fetchall()
        birthYear, birthMonth, birthDate = cursor.execute("SELECT BirthYear, BirthMonth, BirthDate FROM Player WHERE mlbId=?", (id,)).fetchone()
        
        currentStatIdx = 0
        currentMonth = statDates[0][1] - 1
        currentYear = statDates[0][0]
        for year, month in statDates:
            currentMonth += 1
            if currentMonth > LAST_MONTH:
                currentMonth = START_MONTH
                currentYear += 1
            
            if currentMonth != month or currentYear != year:
                # Get last level
                level = levelMap[cursor.execute("SELECT Level FROM Player_Hitter_GameLog WHERE mlbId=? AND ((Year<=? AND Month<=?) OR Year<?) ORDER BY Year DESC, Month DESC, Day DESC LIMIT 1", (id, currentYear, currentMonth, currentYear)).fetchone()[0]]
                while currentMonth != month or currentYear != year:
                    rookie_ball_check = (level < 5) or (currentMonth > 5 and currentMonth < 9)
                    minors_check = (level == 0) or (currentMonth > 4 and currentMonth < 9)
                    if currentYear != 2020 and rookie_ball_check and minors_check: # Covid Check
                        # Add entry
                        currentAge = (currentYear + currentMonth/12) - (birthYear + birthMonth/12 + birthDate/365)
                        cursor.execute("INSERT INTO Model_HitterStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (id, currentYear, currentMonth, currentAge, 0, level,
                                        1,1,1,1,1,1,1,1,1,1,1,
                                        0,0,0,0,0,0,0,0,0))
                    
                    # Update Month
                    currentMonth += 1
                    if currentMonth > LAST_MONTH:
                        currentMonth = START_MONTH
                        currentYear += 1
                        
            currentMonth = month
            currentYear = year

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Generate_PitcherStats(db : sqlite3.Connection):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("DELETE FROM Model_PitcherStats")
    playerData = cursor.execute('''
                                SELECT DISTINCT(m.mlbId), m.lastProspectYear, m.lastProspectMonth, p.birthYear, p.birthMonth, p.birthDate
                                FROM Model_Players AS m
                                INNER JOIN Player AS p on m.mlbId = p.mlbId
                                WHERE m.isPitcher='1'
                                ''').fetchall()

    for id, lastYear, lastMonth, birthYear, birthMonth, birthDate in tqdm(playerData, desc="Model Pitcher Data", leave=False):
        hittingData = cursor.execute('''
                                    SELECT stats.Year, stats.Month, stats.battersFaced, stats.level, stats.RunFactor, stats.HRFactor,
                                    r.gbPercRatio, r.eraRatio, r.fipRatio, r.wobaRatio, r.hrPercRatio, r.bbPercRatio, r.kPercRatio
                                    FROM Player_Pitcher_MonthStats AS stats
                                    INNER JOIN Player_Pitcher_MonthlyRatios AS r ON stats.mlbId = r.mlbId AND stats.Year = r.Year AND stats.Month = r.Month AND stats.Level = r.Level
                                    WHERE stats.mlbId=?
                                    AND (
                                        stats.Year<?
                                        OR (stats.Year=? AND stats.Month<=?)
                                    )
                                    ORDER BY r.Year ASC, r.Month ASC, r.Level ASC
                                    ''', (id, lastYear, lastYear, lastMonth)).fetchall()
        
        prevYear = 0
        prevMonth = 0
        totalPa = 0
        currentLog = None
        # if len(hittingData) == 0:
        #     print(f"I didn't get any data for id={id}")
        for log in hittingData:
            year = log[0]
            month = log[1]
            # Get data for this month
            thisPa = log[2]
            
            log = np.array(log[3:])
            log[0] = levelMap[log[0]]
            
            if year == prevYear and month == prevMonth: # Take weighted average
                # try:
                currentLog = (thisPa / (thisPa + totalPa)) * log + (totalPa / (thisPa + totalPa)) * currentLog
                # except: # A few players that only stole bases
                #     currentLog = 0.5 * log + 0.5 * currentLog
                totalPa += thisPa
            
            else: # new month
                if currentLog is not None: # Log all but first for a player
                    cursor.execute("INSERT INTO Model_PitcherStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (id, prevYear, prevMonth, (year + month/12) - (birthYear + birthMonth/12 + birthDate/365), totalPa,
                                    currentLog[0],currentLog[1],currentLog[2],currentLog[3],currentLog[4],currentLog[5],currentLog[6],
                                    currentLog[7],currentLog[8],currentLog[9])
                                )
                totalPa = thisPa
                currentLog = log
                prevYear = year
                prevMonth = month
                
        # Have 1 last entry to do
        if currentLog is not None:
            cursor.execute("INSERT INTO Model_PitcherStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (id, prevYear, prevMonth, (year + month/12) - (birthYear + birthMonth/12 + birthDate/365), totalPa,
                                        currentLog[0],currentLog[1],currentLog[2],currentLog[3],currentLog[4],currentLog[5],currentLog[6],
                                        currentLog[7],currentLog[8],currentLog[9]))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Fill_PitcherGaps(db : sqlite3.Connection):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    ids = cursor.execute("SELECT DISTINCT mlbId FROM Model_PitcherStats").fetchall()
    for (id,) in tqdm(ids, desc="Model Pitcher Gaps", leave=False):
        statDates = cursor.execute("SELECT Year, Month FROM Model_PitcherStats WHERE mlbId=? ORDER BY Year ASC, Month ASC", (id,)).fetchall()
        birthYear, birthMonth, birthDate = cursor.execute("SELECT BirthYear, BirthMonth, BirthDate FROM Player WHERE mlbId=?", (id,)).fetchone()
        
        currentStatIdx = 0
        currentMonth = statDates[0][1] - 1
        currentYear = statDates[0][0]
        for year, month in statDates:
            currentMonth += 1
            if currentMonth > LAST_MONTH:
                currentMonth = START_MONTH
                currentYear += 1
            
            if currentMonth != month or currentYear != year:
                # Get last level
                
                level = levelMap[cursor.execute("SELECT Level FROM Player_Pitcher_GameLog WHERE mlbId=? AND ((Year<=? AND Month<=?) OR Year<?) ORDER BY Year DESC, Month DESC, Day DESC LIMIT 1", (id, currentYear, currentMonth, currentYear)).fetchone()[0]]
                while currentMonth != month or currentYear != year:
                    rookie_ball_check = (level < 5) or (currentMonth > 5 and currentMonth < 9)
                    minors_check = (level == 0) or (currentMonth > 4 and currentMonth < 9)
                    if currentYear != 2020 and rookie_ball_check and minors_check: # Covid Check
                        # Add entry
                        currentAge = (currentYear + currentMonth/12) - (birthYear + birthMonth/12 + birthDate/365)
                        cursor.execute("INSERT INTO Model_PitcherStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (id, currentYear, currentMonth, currentAge, 0, level,
                                        1,1,1,1,1,1,1,1,1)
                                    )
                    
                    # Update Month
                    currentMonth += 1
                    if currentMonth > LAST_MONTH:
                        currentMonth = START_MONTH
                        currentYear += 1
                        
            currentMonth = month
            currentYear = year

    cursor.execute("END TRANSACTION")
    db.commit()
    
def Model_MonthStats(db : sqlite3.Connection) -> None :
    _Generate_HitterStats(db)
    _Fill_HitterGaps(db)
    _Generate_PitcherStats(db)
    _Fill_PitcherGaps(db)