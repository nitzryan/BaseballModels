import requests
import sqlite3
import pandas as pd
import io
from tqdm import tqdm
import pybaseball
import warnings
from Constants import MEXICAN_LEAGUE_ID

def _Apply_Chadwick_Register(db : sqlite3.Connection):
    FILE_NAME = "https://github.com/chadwickbureau/register/raw/master/data/people-"
    FILE_EXT = ".csv"
    FILE_NUM = [0,1,2,3,4,5,6,7,8,9,"a","b","c","d","e","f"]
    
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', UserWarning)
        for num in tqdm(FILE_NUM, desc="Reading Chadwick Files", leave=False):
            file = FILE_NAME + str(num) + FILE_EXT
            response = requests.get(file)
            if response.status_code != 200:
                print(f"Failed to Get {file} : {response.status_code}")
                continue
            
            reqData = response.content
            df = pd.read_csv(io.StringIO(reqData.decode('utf-8')), on_bad_lines="skip", low_memory=False)
            df = df[df['key_fangraphs'].notna()][df['key_mlbam'].notna()]
            df['key_mlbam'] = df['key_mlbam'].astype('Int64')
            df['key_fangraphs'] = df['key_fangraphs'].astype('Int64')
            for row in df.itertuples():
                cursor.execute(f"UPDATE Player Set fangraphsId='{row.key_fangraphs}' WHERE mlbId='{row.key_mlbam}'")
            
        cursor.execute("END TRANSACTION")
        db.commit()

def _Update_Fangraphs_War(db : sqlite3.Connection, year):
    cursor = db.cursor()
    cursor.execute("DELETE FROM Player_YearlyWar WHERE year=?", (year,))
    db.commit()
    cursor = db.cursor()
    
    cursor.execute("BEGIN TRANSACTION")
    hittingStats = pybaseball.batting_stats(year, qual=0, stat_columns=["PA", "BsR", "Off", "Def", "WAR", "OPS"])
    for row in hittingStats.itertuples():
        try:
            mlbId = cursor.execute("SELECT mlbId FROM Player WHERE fangraphsId=?", (row.IDfg,)).fetchone()[0]
            if mlbId == None or cursor.execute("SELECT COUNT(*) FROM Player_YearlyWar WHERE mlbId=? AND year=? AND position=?", (mlbId, year, "hitting")).fetchone()[0] > 0:
                continue
            
            cursor.execute("INSERT INTO Player_YearlyWar VALUES(?,?,?,?,?,?,?,?)", (mlbId, year, "hitting", row.PA, row.WAR, row.Off, row.Def, row.BsR))
        except: # Player doesn't exist in table
            pass
    
    
    # Similar to above, W is needed
    pitchingStats = pybaseball.pitching_stats(year, qual=0, stat_columns = ["IP", "WAR", "W"])
    for row in pitchingStats.itertuples():
        try:
            mlbId = cursor.execute("SELECT mlbId FROM Player WHERE fangraphsId=?", (row.IDfg,)).fetchone()[0]
            if mlbId == None or cursor.execute("SELECT COUNT(*) FROM Player_YearlyWar WHERE mlbId=? AND year=? and position=?", (mlbId, year, "pitching")).fetchone()[0] > 0:
                continue
            
            innings, subinnings = str(row.IP).split('.')
            outs = 3 * int(innings) + int(subinnings)
            
            cursor.execute("INSERT INTO Player_YearlyWar VALUES(?,?,?,?,?,?,?,?)", (mlbId, year, "pitching", outs, row.WAR, 0, 0, 0))
        except:
            pass
        
    cursor.execute("END TRANSACTION")
    
# Constants to determine when a player can be used for model training
NUM_EMPTY_YEARS = 2 # Players who have not played in last N years are considered retired
SERVICE_TIME_CUTOFF = 6 # Players who have reached this service time have reached FA and no longer accrue prospect value
AGED_OUT_AGE = 27 # If a player has not made the MLB by this age, the prospect value becomes 
AGED_OUT_MONTH = 4
ROOKIE_CUTTOFF_DAYS = 60
    
def _Insert_Empty_Players(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    ids = cursor.execute('''SELECT DISTINCT p.mlbId 
                         FROM Player AS p
                         LEFT JOIN Player_CareerStatus AS pcs ON p.mlbId=pcs.mlbId
                         WHERE pcs.mlbId IS NULL''').fetchall()
    for (id,) in tqdm(ids, desc="Creating Empty PCS Entries", leave=False):
        cursor.execute("INSERT INTO Player_CareerStatus VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (id,None,None,None,None,None,None,None,None,None,None,None,None,None))
    
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Update_IsActive(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    ids = cursor.execute("SELECT mlbId, isHitter, isPitcher FROM Player_CareerStatus").fetchall()
    for id, isHitter, isPitcher in tqdm(ids, desc="Updating IsActive in PCS", leave=False):
        if isHitter:
            table = "Player_Hitter_MonthStats"
        else:
            table = "Player_Pitcher_MonthStats"
            
        try:
            lastYear = cursor.execute(f"SELECT Year FROM {table} WHERE mlbId=? ORDER BY Year DESC LIMIT 1", (id,)).fetchone()[0]
            if (year - lastYear) >= 2:
                isActive = 0
            else:
                isActive = 1
        except: # No stats, likely means drafted but not played
            isActive = 1
            
        cursor.execute("UPDATE Player_CareerStatus SET isActive=? WHERE mlbId=?", (isActive, id))
        
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Update_Positions(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    
    ids = cursor.execute("SELECT mlbId FROM Player_CareerStatus WHERE isPitcher IS NULL AND isHitter IS NULL").fetchall()
    for (id,) in tqdm(ids, desc="Updating Player Positions in PCS", leave=False):
        sumBF = cursor.execute("SELECT SUM(battersFaced) FROM Player_Pitcher_MonthStats WHERE mlbId=?", (id,)).fetchone()[0]
        sumPA = cursor.execute("SELECT SUM(AB) FROM Player_Hitter_MonthStats WHERE mlbId=?", (id,)).fetchone()[0]
        
        # If not found at all, need to set to zero
        if sumBF == None:
            sumBF = 0
        if sumPA == None:
            sumPA = 0

        # Set to whichever position has more
        # Only player this doesn't work for is Ohtani, who will be excluded for being a NPB signee
        if sumBF == sumPA:
            continue
        isHitter = sumPA > sumBF
        if isHitter:
            cursor.execute("UPDATE Player_CareerStatus SET isHitter=? WHERE mlbId=?", (1, id))
        else:
            cursor.execute("UPDATE Player_CareerStatus SET isPitcher=? WHERE mlbId=?", (1, id))
    
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Get_MLB_StartYear(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    ids = cursor.execute("SELECT mlbId FROM Player_CareerStatus WHERE mlbStartYear IS NULL").fetchall()
    for id, in tqdm(ids, desc="MLB Start Year Check", leave=False):
        pitchingYear = cursor.execute("SELECT year FROM Player_Pitcher_MonthStats WHERE mlbId=? AND level=? ORDER BY year ASC LIMIT 1", (id, 1)).fetchone()
        hittingYear = cursor.execute("SELECT year FROM Player_Hitter_MonthStats WHERE mlbId=? AND levelId=? ORDER BY year ASC LIMIT 1", (id, 1)).fetchone()
        if pitchingYear == None:
            if hittingYear == None: #Never made mlb
                continue
            
            # only made as hitter
            startYear = hittingYear[0]
        else:
            if hittingYear == None: # only made as pitcher
                startYear = pitchingYear[0]
            else: # made as both
                if hittingYear[0] < pitchingYear[0]:
                    startYear = hittingYear[0]
                else:
                    startYear = pitchingYear[0]
                
        
        #startYear = cursor.execute("SELECT year FROM Player_YearlyWar WHERE mlbId=? ORDER BY year ASC", (id,)).fetchone()
        if startYear is not None:
            cursor.execute("UPDATE Player_CareerStatus SET mlbStartYear=? WHERE mlbId=?", (startYear, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Check_Service_Time(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    ids = cursor.execute("SELECT mlbId FROM Player_CareerStatus WHERE serviceReached IS NULL AND mlbRookieYear IS NOT NULL").fetchall()
    for id, in tqdm(ids, desc="Checking Service Time in PCS", leave=False):
        serviceYears = cursor.execute("SELECT serviceYear FROM Player_ServiceTime WHERE mlbId=? ORDER BY serviceYear DESC", (id,)).fetchone()
        if serviceYears == None:
            serviceReached = 0
        elif serviceYears[0] < SERVICE_TIME_CUTOFF:
            serviceReached = 0
        else:
            serviceReached = 1
            
        cursor.execute("UPDATE Player_CareerStatus SET serviceReached=? WHERE mlbId=?", (serviceReached, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Set_Service_EndYear(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    ids = cursor.execute("SELECT mlbId FROM Player_CareerStatus WHERE serviceReached IS NOT NULL AND serviceEndYear IS NULL").fetchall()
    cursor.execute("BEGIN TRANSACTION")
    for id, in tqdm(ids, desc="Setting Service End Year", leave=False):
        serviceData = cursor.execute("SELECT year, serviceYear, serviceDays FROM Player_ServiceTime WHERE mlbId=? ORDER BY year ASC", (id,)).fetchall()
        if serviceData == None or len(serviceData) == 0:
            continue
        
        for year, sy, sd in serviceData:
            if sy >= SERVICE_TIME_CUTOFF:
                cursor.execute("UPDATE Player_CareerStatus SET serviceEndYear=? WHERE mlbId=?", (year, id))
                break    
            
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Set_CareerStartYear(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()
    
    # First Insert from Pre05 Players
    pre05_data = cursor.execute("SELECT mlbId, careerStartYear FROM Pre05_Players").fetchall()
    cursor.execute("BEGIN TRANSACTION")
    for id, start_year in pre05_data:
        cursor.execute("UPDATE Player_CareerStatus SET careerStartYear=? WHERE mlbId=?", (start_year, id))
    cursor.execute("END TRANSACTION")
    db.commit()
    
    # Get data from first year that player was found
    cursor = db.cursor()
    ids = cursor.execute("SELECT mlbId, isHitter, isPitcher FROM Player_CareerStatus WHERE careerStartYear IS NULL").fetchall()
    cursor.execute("BEGIN TRANSACTION")
    for id, isHitter, isPitcher in tqdm(ids, desc="Setting Career Start Year", leave=False):
        if isHitter:
            table = "Player_Hitter_MonthStats"
        else:
            table = "Player_Pitcher_MonthStats"
        
        try:
            first_year = cursor.execute(f"SELECT Year FROM {table} WHERE mlbId=? ORDER BY Year ASC LIMIT 1", (id,)).fetchone()[0]
        except: # Empty data, so no stats
            first_year = year
        cursor.execute("UPDATE Player_CareerStatus SET careerStartYear=? WHERE mlbId=?", (first_year, id))
            
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Check_Aged_Out_NoMLB(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()
    ids = cursor.execute("SELECT mlbId FROM Player_CareerStatus WHERE mlbStartYear IS NULL AND agedOut IS NULL").fetchall()
    cursor.execute("BEGIN TRANSACTION")
    cutoffYear = year + 1 - AGED_OUT_AGE
    for id, in tqdm(ids, desc="Check Out Aged no MLB", leave=False):
        birthYear, birthMonth = cursor.execute("SELECT birthYear, birthMonth FROM Player WHERE mlbId=?", (id,)).fetchone()
        
        # Need to eliminate any players who don't have a listed birth date
        if birthYear == None or birthMonth == None:
            cursor.execute("UPDATE Player_CareerStatus SET agedOut=?, ignorePlayer=? WHERE mlbId=?", (1, 1, id))
            continue
        
        if birthMonth >= 4:
            birthYear += 1
        if birthYear < cutoffYear:
            agedOut = birthYear + AGED_OUT_AGE
        else:
            agedOut = None
            
        cursor.execute("UPDATE Player_CareerStatus SET agedOut=? WHERE mlbId=?", (agedOut, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Check_Aged_Out_LateMLB(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()

    noMlbPlayers = cursor.execute("SELECT mlbId, mlbStartYear FROM Player_CareerStatus WHERE mlbStartYear IS NOT NULL AND agedOut IS NULL").fetchall()
    cursor.execute("BEGIN TRANSACTION")
    for id, startYear in tqdm(noMlbPlayers, desc="Check Aged Out MLB", leave=False):
        birthYear, birthMonth = cursor.execute("SELECT birthYear, birthMonth FROM Player WHERE mlbId=?", (id,)).fetchone()
        if birthMonth >= 4:
            birthYear += 1
        
        if startYear - birthYear > AGED_OUT_AGE:
            agedOut = birthYear + AGED_OUT_AGE
        else:
            agedOut = None
            
        cursor.execute("UPDATE Player_CareerStatus SET agedOut=? WHERE mlbId=?", (agedOut, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Ignore_Players(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("UPDATE Player_CareerStatus SET ignorePlayer=NULL")

    file = open("ignorePlayers.txt", "r")
    for id in file:
        cursor.execute("UPDATE Player_CareerStatus SET ignorePlayer=? WHERE mlbId=?", (1, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Get_Highest_Level(db: sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    idDatas = cursor.execute("SELECT mlbId FROM Player_CareerStatus WHERE highestLevel IS NULL").fetchall()
    for id, in tqdm(idDatas, desc="Getting Highest Level", leave=False):
        pitchingLevel = cursor.execute("SELECT Level FROM Player_Pitcher_MonthStats WHERE mlbId=? ORDER BY Level ASC LIMIT 1", (id,)).fetchone()
        hittingLevel = cursor.execute("SELECT LevelId FROM Player_Hitter_MonthStats WHERE mlbId=? ORDER BY LevelId ASC LIMIT 1", (id,)).fetchone()
        
        if pitchingLevel == None:
            if hittingLevel == None: 
                continue
            
            # Only hitting stats
            highestLevel = hittingLevel[0]
        else:
            if hittingLevel == None: # Only pitching stats
                highestLevel = pitchingLevel[0]
            else: # both stats, choose highest level
                if pitchingLevel[0] < hittingLevel[0]:
                    highestLevel = pitchingLevel[0]
                else:
                    highestLevel = hittingLevel[0]
                
        
        #startYear = cursor.execute("SELECT year FROM Player_YearlyWar WHERE mlbId=? ORDER BY year ASC", (id,)).fetchone()
        if highestLevel is not None:
            cursor.execute("UPDATE Player_CareerStatus SET highestLevel=? WHERE mlbId=?", (highestLevel, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Set_Rookie_Pitchers(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    # First pass is players that have 150 career outs.  Determine what year they reached that milestone
    ids = cursor.execute('''
                        SELECT pcs.mlbId, SUM(pms.outs) FROM Player_CareerStatus AS pcs
                        INNER JOIN Player_Pitcher_MonthStats AS pms ON pms.mlbId = pcs.mlbId
                        WHERE pcs.highestLevel='1'
                        AND pms.level='1'
                        AND pcs.mlbRookieYear IS NULL
                        AND pcs.careerStartYear>='2005'
                        GROUP BY pcs.mlbId
                        ''').fetchall()

    for id, outs in tqdm(ids, desc="Setting Rookie Year for Pitchers", leave=False):
        if outs < 150:
            continue
        
        yearlyOuts = cursor.execute("SELECT outs, year FROM Player_Pitcher_MonthStats WHERE mlbId=? AND level=? ORDER BY year ASC", (id,1)).fetchall()
        cumOuts = 0
        for outs, year in yearlyOuts:
            if cumOuts < 150:
                cumOuts += outs
                if cumOuts >= 150:
                    cursor.execute("UPDATE Player_CareerStatus SET mlbRookieYear=? WHERE mlbId=?", (year, id))
                    break
                
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Set_Rookie_Hitters(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    # First pass is players that have 150 career outs.  Determine what year they reached that milestone
    ids = cursor.execute('''
                        SELECT pcs.mlbId, SUM(pms.AB) FROM Player_CareerStatus AS pcs
                        INNER JOIN Player_Hitter_MonthStats AS pms ON pms.mlbId = pcs.mlbId
                        WHERE pcs.highestLevel='1'
                        AND pms.levelId='1'
                        AND pcs.mlbRookieYear IS NULL
                        AND pcs.careerStartYear>='2005'
                        GROUP BY pcs.mlbId
                        ''').fetchall()

    for id, ab in tqdm(ids, desc="Setting Rookie Year for Hitters", leave=False):
        if ab < 130:
            continue
        
        yearlyOuts = cursor.execute("SELECT ab, year FROM Player_Hitter_MonthStats WHERE mlbId=? AND levelId=? ORDER BY year ASC", (id,1)).fetchall()
        cumAbs = 0
        for ab, year in yearlyOuts:
            if ab < 130:
                cumAbs += ab
                if cumAbs >= 130:
                    cursor.execute("UPDATE Player_CareerStatus SET mlbRookieYear=? WHERE mlbId=?", (year, id))
                    break
                
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Set_Rookie_EndMonth(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    idData = cursor.execute('''SELECT mlbId, isHitter, mlbRookieYear 
                            FROM Player_CareerStatus 
                            WHERE mlbRookieYear IS NOT NULL
                            AND mlbRookieMonth IS NULL''').fetchall()

    for id, isHitter, rookieYear in tqdm(idData, desc="Updating Rookie End Month", leave=False):
        if isHitter:
            monthData = cursor.execute("SELECT year, month, AB FROM Player_Hitter_MonthStats WHERE mlbId=? AND LevelId=? ORDER BY Year ASC, month ASC", (id, 1)).fetchall()
            cumAb = 0
            rYear = 0
            rMonth = 0
            for year, month, ab in monthData:
                cumAb += ab
                if cumAb > 130:
                    rYear = year
                    rMonth = month
                    break
                
        else:
            monthData = cursor.execute("SELECT year, month, outs FROM Player_Pitcher_MonthStats WHERE mlbId=? AND Level=? ORDER BY Year ASC, month ASC", (id, 1)).fetchall()
            cumPa = 0
            rYear = 0
            rMonth = 0
            for year, month, pa in monthData:
                cumPa += pa
                if cumPa > 150:
                    rYear = year
                    rMonth = month
                    break
                
        # Check if the year is the same
        if rYear == rookieYear:
            cursor.execute("UPDATE Player_CareerStatus SET mlbRookieMonth=? WHERE mlbId=?", (rMonth, id))
        else: # The years don't match, so set the eligibility to the end of the year that Player_CareerStatus has their rookie year
            cursor.execute("UPDATE Player_CareerStatus SET mlbRookieMonth=? WHERE mlbId=?", (13, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Set_Service_Lapse(db : sqlite3.Connection, year : int):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("DELETE FROM Player_ServiceLapse")

    ids = cursor.execute('''SELECT DISTINCT(pcs.mlbId), isHitter, p.birthYear, p.birthMonth
                        FROM Player_CareerStatus as pcs
                        INNER JOIN Player AS p ON pcs.mlbId = p.mlbID
                        WHERE pcs.mlbStartYear IS NOT NULL
                        AND pcs.serviceReached IS NULL
                        AND pcs.serviceLapseYear IS NULL
                        AND pcs.ignorePlayer IS NULL''').fetchall()
    for id, isHitter, birthYear, birthMonth in tqdm(ids, desc="Updating Service Lapse", leave=False):
        if isHitter:
            years = cursor.execute("SELECT DISTINCT Year FROM Player_Hitter_MonthStats WHERE mlbId=? AND LevelId=? ORDER BY Year ASC", (id, 1)).fetchall()
        else:
            years = cursor.execute("SELECT DISTINCT Year FROM Player_Pitcher_MonthStats WHERE mlbId=? AND Level=? ORDER BY Year ASC", (id, 1)).fetchall()
        
        if len(years) == 0:
            continue
            
        # Calculate age at which player should stop accruing value
        if birthMonth < 4:
            birthYear -= 1
        stopYear = 34 + birthYear
            
        startYear = years[0][0]
        prevYear = startYear
        logged = False
        for (y,) in years:
            if (y - startYear >= 9) or (y - prevYear > 2) or y > stopYear:
                cursor.execute("UPDATE Player_CareerStatus SET serviceLapseYear=? WHERE mlbId=?", (y - 1, id))
                logged = True
                break
            prevYear = y
            
        # Check if the last year is not recent enough for the 2 year rule
        lastYear = years[-1][0]
        if not logged:
            if lastYear < year - 1:
                cursor.execute("UPDATE Player_CareerStatus SET serviceLapseYear=? WHERE mlbId=?", (lastYear + 2, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Update_Signing_Year(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    startYearData = cursor.execute('''
                                SELECT p.mlbId, pcs.careerStartYear
                                FROM Player AS p
                                INNER JOIN Player_CareerStatus as pcs ON p.mlbId = pcs.mlbId
                                AND p.signingYear IS NULL
                                ''').fetchall()

    for id, year in tqdm(startYearData, desc="Updating Signing Years in PCS", leave=False):
        cursor.execute("UPDATE Player SET signingYear=?, signingMonth='1', signingDate='1' WHERE mlbId=?", (year, id))

    cursor.execute("END TRANSACTION")
    db.commit()
    
def Update_Careers(db : sqlite3.Connection, year : int, month : int) -> None:
    cursor = db.cursor()
    cursor.execute("DELETE FROM Player_CareerStatus")
    db.commit()
    
    _Apply_Chadwick_Register(db)
    _Update_Fangraphs_War(db, year)
    _Insert_Empty_Players(db)
    _Update_Positions(db)
    _Update_IsActive(db, year)
    _Set_CareerStartYear(db, year)
    _Get_MLB_StartYear(db)
    _Get_Highest_Level(db)
    _Set_Rookie_Pitchers(db)
    _Set_Rookie_Hitters(db)
    _Set_Rookie_EndMonth(db)
    _Check_Service_Time(db)
    _Set_Service_EndYear(db)
    
    if month >= 9:
        _Check_Aged_Out_NoMLB(db, year)
        _Check_Aged_Out_LateMLB(db, year)
    else:
        _Check_Aged_Out_NoMLB(db, year - 1)
        _Check_Aged_Out_LateMLB(db, year - 1)
    _Ignore_Players(db)
    
    if month >= 9:
        _Set_Service_Lapse(db, year)
    else:
        _Set_Service_Lapse(db, year - 1)
    _Update_Signing_Year(db)