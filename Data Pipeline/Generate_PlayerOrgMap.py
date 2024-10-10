import sqlite3
from tqdm import tqdm

def Generate_PlayerOrgMap(db : sqlite3.Connection) -> None:
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Player_OrgMap WHERE year>'0'")
    db.commit()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    
    ids = cursor.execute("SELECT DISTINCT mlbId FROM Player_CareerStatus WHERE careerStartYear>='2005'").fetchall()
    for id, in tqdm(ids, desc="Player Org Map Generation", leave=False):
        try:
            isHitter, isPitcher = cursor.execute("SELECT isHitter, isPitcher FROM Player_CareerStatus WHERE mlbId=?", (id,)).fetchone()
        except:
            continue
        
        if isHitter is not None:
            dates = cursor.execute('''
                                   SELECT DISTINCT phg.Year, phg.Month, tom.parentOrgId
                                    FROM Player_Hitter_GameLog AS phg
                                    INNER JOIN Team_OrganizationMap AS tom
                                    ON phg.teamId=tom.teamId AND phg.year=tom.year
                                    WHERE Day = (
                                        SELECT MAX(Day)
                                        FROM Player_Hitter_GameLog
                                        WHERE phg.mlbId = mlbId
                                        AND phg.Year = Year
                                        AND phg.Month = Month
                                    )
                                        AND gameLogId = (
                                        SELECT MIN(gameLogId)
                                        FROM Player_Hitter_GameLog
                                        WHERE phg.mlbId = mlbId
                                        AND phg.Year = Year
                                        AND phg.Month = Month
                                        AND phg.Day = Day
                                    )
                                    AND mlbId=?
                                    ORDER BY phg.Year ASC, phg.Month ASC;
                                   ''', (id,)).fetchall()
        else:
            dates = cursor.execute('''
                                   SELECT DISTINCT phg.Year, phg.Month, tom.parentOrgId
                                    FROM Player_Pitcher_GameLog AS phg
                                    INNER JOIN Team_OrganizationMap AS tom
                                    ON phg.teamId=tom.teamId AND phg.year=tom.year
                                    WHERE Day = (
                                        SELECT MAX(Day)
                                        FROM Player_Pitcher_GameLog
                                        WHERE phg.mlbId = mlbId
                                        AND phg.Year = Year
                                        AND phg.Month = Month
                                    )
                                    AND gameLogId = (
                                        SELECT MIN(gameLogId)
                                        FROM Player_Pitcher_GameLog
                                        WHERE phg.mlbId = mlbId
                                        AND phg.Year = Year
                                        AND phg.Month = Month
                                        AND phg.Day = Day
                                    )
                                    AND mlbId=?
                                    ORDER BY phg.Year ASC, phg.Month ASC;
                                   ''', (id,)).fetchall()
        
        last_month = 0
        last_year = 0
        last_team = 0
        append_data = []
        #print(id)
        for year, month, teamId in dates:
            if month < 4 or month > 9:
                continue
            if last_year == 0:
                last_year = year
                last_month = month
                last_team = teamId
                
            while last_year != year or last_month != month:
                last_month += 1
                if last_month > 9:
                    last_month = 4
                    last_year += 1
                
                if last_year != year or last_month != month:
                    append_data.append((id, last_year, last_month, last_team))
                
            last_year = year
            last_month = month
            last_team = teamId
            append_data.append((id, last_year, last_month, last_team))
            
        #print(append_data)
        cursor.executemany("INSERT INTO Player_OrgMap VALUES(?,?,?,?)", append_data)
                
    cursor.execute("END TRANSACTION")
    db.commit()