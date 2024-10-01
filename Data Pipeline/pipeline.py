import sqlite3
import sys
from Get_Players import Get_Players
from Update_Careers import Update_Careers
from Update_Hitter_GameLogs import Update_Hitter_GameLogs
from Update_Pitcher_GameLogs import Update_Pitcher_GameLogs
from Update_Park_Factors import Update_Park_Factors

if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    db = sqlite3.connect("../BaseballStats.db")
    #Get_Players(db, year)
    #Update_Hitter_GameLogs(db, year, month)
    #Update_Pitcher_GameLogs(db, year, month)
    Update_Park_Factors(db, year)
    
    if month == 9:
        Update_Careers(db, year)