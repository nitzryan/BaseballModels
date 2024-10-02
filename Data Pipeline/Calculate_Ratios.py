from tqdm import tqdm
import sqlite3

def _Clear_Data(db : sqlite3.Connection, year : int, month : int):
    cursor = db.cursor()
    cursor.execute("DELETE FROM Player_Hitter_MonthlyRatios WHERE Year=? AND Month=?", (year, month))
    cursor.execute("DELETE FROM Player_Pitcher_MonthlyRatios WHERE Year=? AND Month=?", (year, month))
    db.commit()
    
def _Calculate_HitterRatios(db : sqlite3.Connection, year : int, month : int):
    cursor = db.cursor()
    playerData = cursor.execute('''SELECT * 
                                FROM Player_Hitter_MonthStats
                                WHERE Year=?
                                AND Month=?
                                ORDER BY mlbId DESC, Year DESC, Month DESC, LevelId DESC''',
                                (year, month)).fetchall()
    
    cursor.execute("BEGIN TRANSACTION")
    for mlbId, year, month, level, ab, h, double, triple, hr, k, bb, sb, cs, hbp, parkFunFactor, ParkHRFacttor, GamesC, Games1B, Games2B, Games3B, GamesSS, GamesLF, GamesCF, GamesRF, GamesDH in tqdm(playerData, desc="Hitter Ratios", leave=False):
        try:
            _, _, _, levelAvg, levelOBP, levelSLG, levelISO, levelWOBA, levelHRPerc, levelBBPerc, levelKPerc, levelSBRate, levelSBPerc = cursor.execute("SELECT * FROM Level_HitterStats WHERE LevelId=? AND Year=? AND Month=?", (level, year, month)).fetchone()
        except: # No Data
            continue
        
        pa = ab + bb + hbp
        singles = h - double - triple - hr
        if ab == 0:
            iso = levelISO
            avg = levelAvg
        else:
            iso = (double + 2 * triple + 3 * hr) / ab
            avg = h / ab
        if pa == 0:
            obp = levelOBP
            hrPerc = levelHRPerc
            bbPerc = levelBBPerc
            kPerc = levelKPerc
            sbRate = levelSBRate
            wOBA = levelWOBA
        else:
            # https://library.fangraphs.com/offense/woba/
            wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * double + 1.62 * triple + 2.10 * hr) / (pa)
            obp = (h + bb + hbp) / pa
            hrPerc = hr / pa
            bbPerc = bb / pa
            kPerc = k / pa
            sbRate = sb / pa
        
        if (sb + cs) != 0:
            sbPerc = sb / (sb + cs)
        else:
            sbPerc = levelSBPerc
        
        
        avgRatio = avg / levelAvg
        obpRatio = obp / levelOBP
        isoRatio = iso / levelISO
        wobaRatio = wOBA / levelWOBA
        sbRateRatio = sbRate / levelSBRate
        sbPercRatio = sbPerc / levelSBPerc
        hrPercRatio = hrPerc / levelHRPerc
        bbPercRatio = bbPerc / levelBBPerc
        kPercRatio = kPerc / levelKPerc
        totalGames = GamesC + Games1B + Games2B + Games3B + GamesSS + GamesLF + GamesCF + GamesRF + GamesDH
        PercC = GamesC / totalGames
        Perc1B = Games1B / totalGames
        Perc2B = Games2B / totalGames
        Perc3B = Games3B / totalGames
        PercSS = GamesSS / totalGames
        PercLF = GamesLF / totalGames
        PercCF = GamesCF / totalGames
        PercRF = GamesRF / totalGames
        PercDH = GamesDH / totalGames
        
        cursor.execute("INSERT INTO Player_Hitter_MonthlyRatios VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (mlbId, year, month, level, avgRatio, obpRatio, isoRatio, wobaRatio, sbRateRatio, sbPercRatio, hrPercRatio, bbPercRatio, kPercRatio, PercC, Perc1B, Perc2B, Perc3B, PercSS, PercLF, PercCF, PercRF, PercDH))
        
    cursor.execute("END TRANSACTION")
    db.commit()
    
def _Calculate_PitcherRatios(db : sqlite3.Connection, year : int, month : int):
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")

    playerData = cursor.execute('''SELECT * 
                                FROM Player_Pitcher_MonthStats 
                                WHERE Year=? AND Month=?
                                ORDER BY mlbId DESC, Year DESC, Month DESC, Level DESC''',
                                (year, month)).fetchall()

    for mlbId, year, month, level, pa, outs, go, ao, r, er, h, k, bb, hbp, double, triple, hr, _, _ in tqdm(playerData, desc="Pitcher Ratios", leave=False):
        try:
            _, _, _, levelEra, levelRA, levelFipConstant, levelWOBA, levelHRPerc, levelBBPerc, levelKPerc, levelGOPerc, levelAvg, levelIso = cursor.execute("SELECT * FROM Level_PitcherStats WHERE Level=? AND Year=? AND Month=?", (level, year, month)).fetchone()
        except: # No Data
            continue
        
        singles = h - double - triple - hr
        if pa == 0:
            hrPerc = levelHRPerc
            bbPerc = levelBBPerc
            kPerc = levelKPerc
            wOBA = levelWOBA
        else:
            # https://library.fangraphs.com/offense/woba/
            wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * double + 1.62 * triple + 2.10 * hr) / (pa)
            hrPerc = hr / pa
            bbPerc = bb / pa
            kPerc = k / pa
        
        if outs == 0:
            goPerc = levelGOPerc
            if er == 0:
                era = levelEra
            else:
                era = er * 27
            fip = era
        else:
            if (go + ao) == 0:
                goPerc = levelGOPerc
            else:
                goPerc = go / (go + ao)
            era = (er / outs) * 27
            fip = (13 * hr + 3 * (bb + hbp) - 2 * k) / (outs / 3) + levelFipConstant
        
        wobaRatio = wOBA / levelWOBA
        hrPercRatio = hrPerc / levelHRPerc
        bbPercRatio = bbPerc / levelBBPerc
        kPercRatio = kPerc / levelKPerc
        gbPercRatio = goPerc / levelGOPerc
        eraRatio = era / levelEra
        fipRatio = fip / levelEra
        
        cursor.execute("INSERT INTO Player_Pitcher_MonthlyRatios VALUES(?,?,?,?,?,?,?,?,?,?,?)", (mlbId, year, month, level, gbPercRatio, eraRatio, fipRatio, wobaRatio, hrPercRatio, bbPercRatio, kPercRatio))
        
    cursor.execute("END TRANSACTION")
    db.commit()
    
def Calculate_Ratios(db : sqlite3.Connection, year : int, month : int):
    _Clear_Data(db, year, month)
    _Calculate_HitterRatios(db, year, month)
    _Calculate_PitcherRatios(db, year, month)