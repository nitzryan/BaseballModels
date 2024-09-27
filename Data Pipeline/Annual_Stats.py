import sqlite3
from tqdm import tqdm
import sys

from Get_Advanced_Stats import Hitting_Stats_To_Advanced, Pitching_Stats_To_Advanced

db = sqlite3.connect("../BaseballStats.db")
db.rollback()

cursor = db.cursor()
cursor.execute("DELETE FROM Player_Hitter_YearAdvanced")
cursor.execute("DELETE FROM Player_Pitcher_YearAdvanced")
db.commit()
cursor = db.cursor()

hitter_season_data = cursor.execute("SELECT DISTINCT mlbId, year, level, teamId, leagueId FROM Player_Hitter_GameLog").fetchall()
db_data = []

for mlbId, year, level, teamId, leagueId in tqdm(hitter_season_data, desc="Hitter Season Data"):
    ab, h, doubles, triples, hr, k, bb, sb, cs, hbp = cursor.execute('''SELECT SUM(AB), SUM(H), SUM("2B"), SUM("3B"), SUM(HR), SUM(K), SUM(BB), SUM(SB), SUM(CS), SUM(HBP) 
                                                                     FROM Player_Hitter_GameLog
                                                                     WHERE mlbId=? AND year=? AND level=? AND teamId=? AND leagueId=?''', (mlbId, year, level, teamId, leagueId)).fetchone()
    
    pa, avg, obp, slg, iso, wOBA, _, bbPerc, kPerc, _, _ = Hitting_Stats_To_Advanced(ab, h, doubles, triples, hr, k, bb, sb, cs, hbp)
    db_data.append((mlbId, level, year, teamId, leagueId, pa, avg, obp, slg, iso, wOBA, None, hr, bbPerc, kPerc, sb, cs))
    
cursor.execute("BEGIN TRANSACTION")
cursor.executemany("INSERT INTO Player_Hitter_YearAdvanced VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", db_data)
cursor.execute("END TRANSACTION")
db.commit()
cursor = db.cursor()

pitcher_season_data = cursor.execute("SELECT DISTINCT mlbId, year, level, teamId, leagueId FROM Player_Pitcher_GameLog").fetchall()
db_data = []

for mlbId, year, level, teamId, leagueId in tqdm(pitcher_season_data, desc="Pitcher Season Data"):
    bf, outs, go, ao, er, h, k, bb, hbp, doubles, triples, hr = cursor.execute('''SELECT SUM(battersFaced), SUM(outs), SUM(go), SUM(ao), SUM(er), SUM(H), SUM(K), SUM(bb), SUM(hbp), SUM("2B"), SUM("3B"), SUM(HR) 
                                                                     FROM Player_Pitcher_GameLog
                                                                     WHERE mlbId=? AND year=? AND level=? AND teamId=? AND leagueId=?''', (mlbId, year, level, teamId, leagueId)).fetchone()
    
    fipConstant = cursor.execute("SELECT AVG(FipConstant) FROM Level_PitcherStats WHERE level=? AND year=?", (level, year)).fetchone()[0]
    gbRatio, era, fip, kPerc, bbPerc, _, wOBA = Pitching_Stats_To_Advanced(bf, outs, go, ao, er, h, k, bb, hbp, doubles, triples, hr, fipConstant)
    db_data.append((mlbId, level, year, teamId, leagueId, bf, outs, gbRatio, era, fip, kPerc, bbPerc, hr, wOBA))
    
cursor.execute("BEGIN TRANSACTION")
cursor.executemany("INSERT INTO Player_Pitcher_YearAdvanced VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", db_data)
cursor.execute("END TRANSACTION")
db.commit()

sys.exit(0)