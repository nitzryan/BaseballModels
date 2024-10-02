import sqlite3
from tqdm import tqdm

def Model_PlayerWar(db : sqlite3.Connection):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("BEGIN TRANSACTION")
    cursor.execute("DELETE FROM Model_PlayerWar")

    ids = cursor.execute('''SELECT mlbId, lastMLBSeason, isHitter 
                        FROM Model_Players
                        WHERE lastMLBSeason IS NOT NULL''').fetchall()

    for id, lastSeason, isHitter in tqdm(ids):
        warData = cursor.execute('''SELECT position, year, pa, war, off, def, bsr 
                                FROM Player_YearlyWar
                                WHERE mlbId=?
                                AND year<=?
                                AND position=?
                                ORDER BY Year ASC''', (id, lastSeason, "hitting" if isHitter == 1 else "pitching")).fetchall()
        
        for position, year, pa, war, off, df, bsr in warData:
            if (position == "hitting" and isHitter == 1) or (position == "pitching" and isHitter == 0):
                cursor.execute("INSERT INTO Model_PlayerWar VALUES(?,?,?,?,?,?,?,?)",
                            (id, year, isHitter, pa, war, off, df, bsr))

    cursor.execute("END TRANSACTION")
    db.commit()