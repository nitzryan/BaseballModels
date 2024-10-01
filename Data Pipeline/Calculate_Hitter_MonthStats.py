import sqlite3
from tqdm import tqdm

def Calculate_Hitter_MonthStats(db : sqlite3.Connection, year : int, month : int):
    # Clear out old data
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Player_Hitter_MonthStats WHERE Year=? AND Month=?", (year, month))
    cursor.execute("DELETE FROM Player_Hitter_MonthAdvanced WHERE Year=? AND Month=?", (year, month))
    db.commit()
    cursor = db.cursor()
    
    # Get league factors
    LeagueFactors = {}
    lfData = cursor.execute("SELECT LeagueId, Year, RunFactor, HRFactor FROM League_Factors").fetchall()
    for league, year, rFac, hrFac in lfData:
        if not league in LeagueFactors.keys():
            LeagueFactors[league] = {}
        LeagueFactors[league][year] = {"RunFactor" : rFac, "HRFactor" : hrFac}
        
    # Get Park Factors, adjucted by league factor
    ParkFactors = {}
    pfData = cursor.execute("SELECT TeamId, LeagueId, Year, RunFactor, HRFactor FROM Park_Factors").fetchall()
    for team, league, year, rFac, hrFac in pfData:
        if not team in ParkFactors:
            ParkFactors[team] = {}
        ParkFactors[team][year] = {"RunFactor" : rFac * LeagueFactors[league][year]["RunFactor"], "HRFactor" : hrFac * LeagueFactors[league][year]["HRFactor"]}
        
    # Update Hitter Stats
    playerLevels = cursor.execute(f"SELECT DISTINCT mlbId, Level FROM Player_Hitter_GameLog WHERE Year='{year}'").fetchall()
    dbData = []
    dbAdvancedData = []
    for (mlbId, level) in tqdm(playerLevels, desc="Hitter Monthly Stats", leave=False):
        if month == 4:
            gameLogs = cursor.execute(f'SELECT AB,H,"2B","3B",HR,K,BB,SB,CS,HBP,Position,HomeTeamId FROM Player_Hitter_GameLog WHERE mlbId=? AND Year=? AND Month<=? AND Level=?', (mlbId, year, 4, level)).fetchall()
        elif month == 8 and level >= 16:
            gameLogs = cursor.execute(f'SELECT AB,H,"2B","3B",HR,K,BB,SB,CS,HBP,Position,HomeTeamId FROM Player_Hitter_GameLog WHERE mlbId=? AND Year=? AND Month>=? AND Level=?', (mlbId, year, 8, level)).fetchall()
        elif month > 8 and level >= 16: # Rookie ball has few games after this month, roll september into august
            continue
        elif month == 9:
            gameLogs = cursor.execute(f'SELECT AB,H,"2B","3B",HR,K,BB,SB,CS,HBP,Position,HomeTeamId FROM Player_Hitter_GameLog WHERE mlbId=? AND Year=? AND Month>=? AND Level=?', (mlbId, year, 9, level)).fetchall()
        else:
            gameLogs = cursor.execute(f'SELECT AB,H,"2B","3B",HR,K,BB,SB,CS,HBP,Position,HomeTeamId FROM Player_Hitter_GameLog WHERE mlbId=? AND Year=? AND Month=? AND Level=?', (mlbId, year, month, level)).fetchall()
        
        if len(gameLogs) == 0:
            continue
        totalAb = 0
        totalH = 0
        total2B = 0
        total3B = 0
        totalHR = 0
        totalK = 0
        totalBB = 0
        totalHBP = 0
        totalSB = 0
        totalCS = 0
        totalPositions = [0] * 9
        totalRunFactor = 0
        totalHRFactor = 0
        
        for ab, h, doubles, triples, hr, k, bb, sb, cs, hbp, position, homeTeamId in gameLogs:
            totalAb += ab
            totalH += h
            total2B += doubles
            total3B += triples
            totalHR += hr
            totalK += k
            totalBB += bb
            totalHBP += hbp
            totalSB += sb
            totalCS += cs
            if position > 1 and position <= 9:
                totalPositions[position - 2] += 1
            else:
                totalPositions[-1] += 1
                
            try:
                totalRunFactor += ab * ParkFactors[homeTeamId][year]["RunFactor"]
                totalHRFactor += ab * ParkFactors[homeTeamId][year]["HRFactor"]
            except: # Not enough data on this park
                totalRunFactor += ab
                totalHRFactor += ab
        
        if totalAb > 0:
            totalRunFactor /= totalAb
            totalHRFactor /= totalAb
        else:
            totalRunFactor = 1
            totalHRFactor = 1
        dbData.append((mlbId, year, month, level, totalAb, totalH, total2B, total3B, totalHR, totalK, totalBB, totalSB, totalCS, totalHBP, totalRunFactor, totalHRFactor, totalPositions[0], totalPositions[1], totalPositions[2], totalPositions[3], totalPositions[4], totalPositions[5], totalPositions[6], totalPositions[7], totalPositions[8]))

        # Generate Advanced Statistics
        columns = 'SUM(AB),SUM(H),SUM("2B"),SUM("3B"),SUM(HR),SUM(K),SUM(BB),SUM(SB),SUM(CS),SUM(HBP),Level,TeamId,LeagueId FROM Player_Hitter_GameLog'
        if month == 4:
            conditions = 'mlbId=? AND Year=? AND Month<=? AND Level=?'
        elif month == 8 and level >= 16 or month == 9:
            conditions = 'mlbId=? AND Year=? AND Month>=? AND Level=?'
        # Month > 8 and level >= 16 should be caught by above
        else:
            conditions = 'mlbId=? AND Year=? AND Month=? AND Level=?'
        statsByTeam = cursor.execute(f'SELECT {columns} WHERE {conditions} GROUP BY Level, TeamId, LeagueId', (mlbId, year, month, level)).fetchall()
        
        for ab, h, doubles, triples, hr, k, bb, sb, cs, hbp, levelId, teamId, leagueId in statsByTeam:
            if ab > 0:
                avg = h / ab
                iso = (doubles + 2 * triples + 3 * hr) / ab
                
            else:
                avg = 0
                iso = 0
                
            slg = avg + iso
            wRC = None # Need to adjust for league stats, so can't do at this time
            
            pa = ab + bb + hbp
            singles = h - doubles - triples - hr
            if pa > 0:
                obp = (h + bb + hbp) / pa
                hrPerc = hr / pa
                bbPerc = bb / pa
                kPerc = k / pa
                sbRate = sb / pa
                # https://library.fangraphs.com/offense/woba/
                wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr) / (pa)
            else:
                obp = 0
                hrPerc = 0
                bbPerc = 0
                kPerc = 0
                sbRate = 0
                wOBA = 0
                
            if (sb + cs) > 0:
                sbPerc = sb / (sb + cs)
            else:
                sbPerc = 0
    
            
            dbAdvancedData.append((mlbId, levelId, year, month, teamId, leagueId, pa, avg, obp, slg, iso, wOBA, wRC, hrPerc, bbPerc, kPerc, sbRate, sbPerc))
    
    cursor.execute("BEGIN TRANSACTION")
    cursor.executemany("INSERT INTO Player_Hitter_MonthStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbData)
    cursor.executemany("INSERT INTO Player_Hitter_MonthAdvanced VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbAdvancedData)
    cursor.execute("END TRANSACTION")
    db.commit()