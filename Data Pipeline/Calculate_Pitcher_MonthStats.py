import sqlite3
from tqdm import tqdm

def Calculate_Pitcher_MonthStats(db : sqlite3.Connection, year : int, month : int):
    # Clear out old data
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Player_Pitcher_MonthStats WHERE Year=? AND Month=?", (year, month))
    cursor.execute("DELETE FROM Player_Pitcher_MonthAdvanced WHERE Year=? AND Month=?", (year, month))
    db.commit()
    cursor = db.cursor()
    
    # Get League Factors
    LeagueFactors = {}
    lfData = cursor.execute("SELECT LeagueId, Year, RunFactor, HRFactor FROM League_Factors").fetchall()
    for league, year, rFac, hrFac in lfData:
        if not league in LeagueFactors.keys():
            LeagueFactors[league] = {}
        LeagueFactors[league][year] = {"RunFactor" : rFac, "HRFactor" : hrFac}
        
    # Get Park Factors, adjusted by league factors
    ParkFactors = {}
    pfData = cursor.execute("SELECT TeamId, LeagueId, Year, RunFactor, HRFactor FROM Park_Factors").fetchall()
    for team, league, year, rFac, hrFac in pfData:
        if not team in ParkFactors:
            ParkFactors[team] = {}
        ParkFactors[team][year] = {"RunFactor" : rFac * LeagueFactors[league][year]["RunFactor"], "HRFactor" : hrFac * LeagueFactors[league][year]["HRFactor"]}
        
    # Update Pitcher Stats
    playerLevels = cursor.execute(f"SELECT DISTINCT mlbId, Level FROM Player_Pitcher_GameLog WHERE Year='{year}'").fetchall()
    dbData = []
    dbAdvancedData = []
    for (mlbId, level) in tqdm(playerLevels, desc="Pitcher Monthly Stats", leave=False):
        if month == 4:
            gameLogs = cursor.execute(f'SELECT battersFaced,outs,go,ao,r,er,h,k,bb,hbp,"2B","3B",HR,HomeTeamId FROM Player_Pitcher_GameLog WHERE mlbId=? AND Year=? AND Month<=? AND Level=?', (mlbId, year, 4, level)).fetchall()
        elif month == 8 and level == 16:
            gameLogs = cursor.execute(f'SELECT battersFaced,outs,go,ao,r,er,h,k,bb,hbp,"2B","3B",HR,HomeTeamId FROM Player_Pitcher_GameLog WHERE mlbId=? AND Year=? AND Month>=? AND Level=?', (mlbId, year, 8, level)).fetchall()
        elif month > 8 and level == 16: # Rookie ball has few games after this month, roll september into august
            continue
        elif month == 9:
            gameLogs = cursor.execute(f'SELECT battersFaced,outs,go,ao,r,er,h,k,bb,hbp,"2B","3B",HR,HomeTeamId FROM Player_Pitcher_GameLog WHERE mlbId=? AND Year=? AND Month>=? AND Level=?', (mlbId, year, 9, level)).fetchall()
        else:
            gameLogs = cursor.execute(f'SELECT battersFaced,outs,go,ao,r,er,h,k,bb,hbp,"2B","3B",HR,HomeTeamId FROM Player_Pitcher_GameLog WHERE mlbId=? AND Year=? AND Month=? AND Level=?', (mlbId, year, month, level)).fetchall()
        
        if len(gameLogs) == 0:
            continue
        totalH = 0
        total2B = 0
        total3B = 0
        totalHR = 0
        totalK = 0
        totalBB = 0
        totalHBP = 0
        totalBF = 0
        totalOuts = 0
        totalGO = 0
        totalAO = 0
        totalR = 0
        totalER = 0
        totalRunFactor = 0
        totalHRFactor = 0
        
        for battersFaced, outs, go, ao, r, er, h, k, bb, hbp, doubles, triples, hr, homeTeamId in gameLogs:
            totalH += h
            total2B += doubles
            total3B += triples
            totalHR += hr
            totalK += k
            totalBB += bb
            totalHBP += hbp
            totalBF += battersFaced
            totalOuts += outs
            totalGO += go
            totalAO += ao
            totalR += r
            totalER += er
                
            try:
                totalRunFactor += battersFaced * ParkFactors[homeTeamId][year]["RunFactor"]
                totalHRFactor += battersFaced * ParkFactors[homeTeamId][year]["HRFactor"]
            except: # Not enough data on this park
                totalRunFactor += battersFaced
                totalHRFactor += battersFaced
        
        if totalBF > 0:
            totalRunFactor /= totalBF
            totalHRFactor /= totalBF
        else:
            totalRunFactor = 1
            totalHRFactor = 1
        dbData.append((mlbId, year, month, level, totalBF, totalOuts, totalGO, totalAO, totalR, totalER, totalH, totalK, totalBB, totalHBP, total2B, total3B, totalHR, totalRunFactor, totalHRFactor))

        # Generate Advanced Statistics
        columns = 'SUM(battersFaced),SUM(outs),SUM(H),SUM("2B"),SUM("3B"),SUM(HR),SUM(K),SUM(BB),SUM(HBP),SUM(er),SUM(go),SUM(ao),Level,TeamId,LeagueId FROM Player_Pitcher_GameLog'
        if month == 4:
            conditions = 'mlbId=? AND Year=? AND Month<=? AND Level=?'
        elif month == 8 and level >= 16 or month == 9:
            conditions = 'mlbId=? AND Year=? AND Month>=? AND Level=?'
        # Month > 8 and level >= 16 should be caught by above
        else:
            conditions = 'mlbId=? AND Year=? AND Month=? AND Level=?'
        statsByTeam = cursor.execute(f'SELECT {columns} WHERE {conditions} GROUP BY Level, TeamId, LeagueId', (mlbId, year, month, level)).fetchall()
        
        for bf, outs, h, doubles, triples, hr, k, bb, hbp, er, go, ao, levelId, teamId, leagueId in statsByTeam:
            singles = h - doubles - triples - hr
            if bf > 0:
                hrPerc = hr / bf
                bbPerc = bb / bf
                kPerc = k / bf
                # https://library.fangraphs.com/offense/woba/
                wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr) / (bf)
            else:
                hrPerc = 0
                bbPerc = 0
                kPerc = 0
                wOBA = 0
    
            fipConstant = cursor.execute("SELECT FipConstant FROM Level_PitcherStats WHERE level=? AND year=? AND month=?", (level, year, month)).fetchone()[0]
    
            if outs > 0:
                era = er / outs * 27
                fip = (13 * hr + 3 * (bb + hbp) - 2 * k) / (outs / 3) + fipConstant
            else:
                era = 27    
                fip = 27
            if (go + ao) > 0:
                gbRatio = go / (go + ao)
            else:
                gbRatio = 0
            
            dbAdvancedData.append((mlbId, levelId, year, month, teamId, leagueId, bf, outs, gbRatio, era, fip, kPerc, bbPerc, hrPerc, wOBA))
    
    cursor.execute("BEGIN TRANSACTION")
    cursor.executemany("INSERT INTO Player_Pitcher_MonthStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbData)
    cursor.executemany("INSERT INTO Player_Pitcher_MonthAdvanced VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbAdvancedData)
    cursor.execute("END TRANSACTION")
    db.commit()