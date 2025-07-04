import sqlite3
import sys
from Get_Players import Get_Players
from Update_Careers import Update_Careers
from Update_Hitter_GameLogs import Update_Hitter_GameLogs
from Update_Pitcher_GameLogs import Update_Pitcher_GameLogs
from Update_Park_Factors import Update_Park_Factors
from Calculate_Hitter_MonthStats import Calculate_Hitter_MonthStats
from Calculate_Pitcher_MonthStats import Calculate_Pitcher_MonthStats
from Calculate_LevelStats import Calculate_LevelStats
from Calculate_Ratios import Calculate_Ratios
from Update_ServiceTime import Update_ServiceTime
from Model_Players import Model_Players
from Model_MonthStats import Model_MonthStats
from Model_PlayerWar import Model_PlayerWar
from Annual_Stats import Annual_Stats
from Update_ParentMap import Update_Parent_Map, Update_Parents
from Generate_PlayerOrgMap import Generate_PlayerOrgMap

if __name__ == '__main__':
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    db = sqlite3.connect("../BaseballStats.db")
    if month == 0:
        Update_ServiceTime(db, year)
        Update_Careers(db, year, month)
        exit(0)
    
    # Get_Players(db, year)
    # Update_Hitter_GameLogs(db, year, month)
    # Update_Pitcher_GameLogs(db, year, month)
    # Update_Park_Factors(db, year)
    # Calculate_LevelStats(db, year, month)
    # Calculate_Hitter_MonthStats(db, year, month)
    # Calculate_Pitcher_MonthStats(db, year, month)
    # Calculate_Ratios(db, year, month)
    # Annual_Stats(db, year)
    # Update_Parent_Map(db, year)
    # Update_Parents(db)
    # Generate_PlayerOrgMap(db)
    
    # Update_Careers(db, year, month)
    # Model_Players(db, year, month)
    # Model_PlayerWar(db)
    #Model_MonthStats(db)