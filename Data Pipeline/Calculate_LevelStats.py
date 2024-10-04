import sqlite3
from tqdm import tqdm
from Constants import SPORT_IDS

def _Clear_Data(db: sqlite3.Connection, year : int, month : int):
    cursor = db.cursor()
    cursor.execute("DELETE FROM Level_HitterStats WHERE Year=? AND Month=?", (year, month))
    cursor.execute("DELETE FROM Level_PitcherStats WHERE Year=? AND Month=?", (year, month))
    db.commit()
    
def _Calculate_HitterStats(db: sqlite3.Connection, year : int, month : int):
    cursor = db.cursor()
    for level in tqdm(SPORT_IDS, desc="Hitter Level Stats", leave=False):
        ab, h, doubles, triples, hr, k, bb, sb, cs, hbp = cursor.execute('SELECT SUM(AB), SUM(H), SUM("2B"), SUM("3B"), SUM(HR), SUM(K), SUM(BB), SUM(SB), SUM(CS), SUM(HBP) FROM Player_Hitter_GameLog WHERE Year=? AND Month=? AND Level=?', (year, month, level)).fetchone()
        
        # Some year/level combos do not have any games
        if ab == 0 or ab == None:
            continue
        
        # Calculate stats
        pa = ab + bb + hbp
        singles = h - doubles - triples - hr
        iso = (doubles + 2 * triples + 3 * hr) / ab
        avg = h / ab
        obp = (h + bb + hbp) / pa
        slg = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab
        hrPerc = hr / pa
        bbPerc = bb / pa
        kPerc = k / pa
        sbRate = sb / pa
        sbPerc = sb / (sb + cs)
        # https://library.fangraphs.com/offense/woba/
        wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr) / (pa)
        
        # Insert data
        cursor.execute("INSERT INTO Level_HitterStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (level, year, month, avg, obp, slg, iso, wOBA, hrPerc, bbPerc, kPerc, sbRate, sbPerc))
        db.commit()
        cursor = db.cursor()

def _Calculate_PitcherStats(db: sqlite3.Connection, year : int, month : int):        
    cursor = db.cursor()
    for level in tqdm(SPORT_IDS, desc="Pitcher Level Stats", leave=False):
        bf, outs, go, ao, r, er, h, k, bb, hbp, doubles, triples, hr = cursor.execute('SELECT SUM(battersFaced), SUM(outs), SUM(go), SUM(ao), SUM(r), SUM(er), SUM(h), SUM(k), SUM(bb), SUM(hbp), SUM("2B"), SUM("3B"), SUM(HR) FROM Player_Pitcher_GameLog WHERE Year=? AND Month=? AND Level=?', (year, month, level)).fetchone()
        
        # Some year/level combos do not have any games
        if bf == 0 or bf == None:
            continue
        
        # Calculate stats
        ab = bf - bb - hbp
        singles = h - doubles - triples - hr
        iso = (doubles + 2 * triples + 3 * hr) / ab
        avg = h / ab
        obp = (h + bb + hbp) / bf
        slg = (singles + 2 * doubles + 3 * triples + 4 * hr) / ab
        hrPerc = hr / bf
        bbPerc = bb / bf
        kPerc = k / bf
        goPerc = go / (go + ao)
        # https://library.fangraphs.com/offense/woba/
        wOBA = (0.69 * bb + 0.72 * hbp + 0.89 * singles + 1.27 * doubles + 1.62 * triples + 2.10 * hr) / (bf)
        era = er / (outs / 27)
        ra = r / (outs / 27)
        fipNoConstant = (13 * hr + 3 * (bb + hbp) - 2 * k) / (outs / 3)
        fipConstant = era - fipNoConstant
        
        # Insert data
        cursor.execute("INSERT INTO Level_PitcherStats VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (level, year, month, era, ra, fipConstant, wOBA, hrPerc, bbPerc, kPerc, goPerc, avg, iso))
        db.commit()
        cursor = db.cursor()
    
def Calculate_LevelStats(db: sqlite3.Connection, year : int, month : int) -> None:
    db.rollback()
    _Clear_Data(db, year, month)
    _Calculate_HitterStats(db, year, month)
    _Calculate_PitcherStats(db, year, month)