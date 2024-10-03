import sqlite3
from tqdm import tqdm
    
def Model_Players(db : sqlite3.Connection, year : int, month : int):
    db.rollback()
    cursor = db.cursor()
    cursor.execute("DELETE FROM Model_Players")
    db.commit()
    cursor = db.cursor()
    data = cursor.execute(f'''
                      SELECT DISTINCT(pcs.mlbId), pcs.isHitter, pcs.isPitcher, pcs.agedOut, pcs.serviceEndYear, pcs.mlbRookieYear, pcs.mlbRookieMonth, psl.year, p.birthYear, p.birthMonth, p.birthDate, p.signingYear
                      FROM Player_CareerStatus AS pcs
                      LEFT JOIN Player_ServiceLapse AS psl ON pcs.mlbId = psl.mlbId
                      INNER JOIN Player AS p ON pcs.mlbId = p.mlbId
                      WHERE pcs.careerStartYear>=?
                      AND pcs.ignorePlayer IS NULL
                      AND p.birthYear IS NOT NULL
                      AND p.birthMonth IS NOT NULL
                      ''', (2005,)).fetchall()

    for id, isHitter, isPitcher, agedOut, serviceEndYear, rookieYear, rookieMonth, serviceLapseYear, birthYear, birthMonth, birthDate, signingYear in tqdm(data, desc="Model Players", leave=False):
        if cursor.execute("SELECT COUNT(*) FROM Model_Players WHERE mlbId=?", (id,)).fetchone()[0] > 0:
            continue # Player was validly added, so don't add invalid player
        
        # Determine last MLB Season
        if agedOut is not None:
            lastMLBSeason = agedOut
        elif serviceLapseYear is not None:
            lastMLBSeason = serviceLapseYear
        elif serviceEndYear is not None:
            lastMLBSeason = serviceEndYear
        else:
            lastMLBSeason = year
        
        # Last prospect year/month
        if agedOut is not None and agedOut != 0:
            lastProspectYear = agedOut
            lastProspectMonth = 13
        elif rookieYear is not None and rookieMonth is not None:
            lastProspectYear = rookieYear
            lastProspectMonth = rookieMonth
        elif serviceLapseYear is not None:
            lastProspectYear = serviceLapseYear
            lastProspectMonth = 13
        else:
            lastProspectYear = year
            lastProspectMonth = month
        
        # Age at signing
        # Use 07/01/SigningYear for signing date.  Should try to get better data for this
        signingYear += 0.5
        signingAge = signingYear - birthYear - (birthMonth - 1) / 12 - (birthDate - 1) / 365
        if signingAge >= 27: # Player will immediately be ineligible, so discard
            continue
        
        cursor.execute("INSERT INTO Model_Players VALUES(?,?,?,?,?,?,?)", (id, isHitter, isPitcher, lastProspectYear, lastProspectMonth, lastMLBSeason, signingAge))

    cursor.execute("END TRANSACTION")
    db.commit()