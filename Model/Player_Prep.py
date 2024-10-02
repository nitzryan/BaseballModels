import torch
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split # type: ignore
from sklearn.decomposition import PCA # type: ignore
from sklearn.preprocessing import StandardScaler # type: ignore
import math
import random
from tqdm import tqdm
from Constants import db, D_TYPE
# Common Variables

NOT_DRAFTED_VALUE = 1000
NOT_INIT_DRAFT_VALUE = 100
CUTOFF_YEAR_DIF = 10
levelMap = {1:0,11:1,12:2,13:3,14:4,15:5,16:6,17:7}
np.set_printoptions(precision=2, linewidth=300, floatmode='fixed')

cursor = db.cursor()
hitters = None
_cutoff_year = None

def Init_Hitters(year : int):
    global hitters
    global _cutoff_year
    _cutoff_year = year - CUTOFF_YEAR_DIF
    
    cursor = db.cursor()
    hitters = cursor.execute('''
                         SELECT mp.mlbId, mp.ageAtSigningYear, p.draftPick
                         FROM Model_Players AS mp
                         INNER JOIN Player as p ON mp.mlbId = p.mlbId
                         INNER JOIN Player_CareerStatus AS pcs ON mp.mlbId = pcs.mlbId
                         WHERE mp.isHitter='1'
                         AND pcs.careerStartYear<=?
                         AND p.position='hitting'
                         ORDER BY mp.mlbId
                         ''', (_cutoff_year,)).fetchall()
    
    global age_mean
    global age_std
    global log_picks_mean
    global log_picks_std
    age_mean, age_std, log_picks_mean, log_picks_std = Get_Age_Draft_Data()

pitchers = None
def Init_Pitchers(year : int):
    global pitchers
    cutoff_year = year - CUTOFF_YEAR_DIF
    pitchers = cursor.execute('''
                            SELECT mp.mlbId, mp.ageAtSigningYear, p.draftPick
                            FROM Model_Players AS mp
                            INNER JOIN Player as p ON mp.mlbId = p.mlbId
                            WHERE mp.isPitcher='1'
                            AND p.position='pitching'
                            AND pcs.careerStartYear<=?
                            ORDER BY mp.mlbId
                            ''', (cutoff_year,)).fetchall()

# Get Age and draft pick, which are done differently than other stats
SIGNING_COMPONENTS = 3
def Get_Age_Draft_Data():
    ages = []
    picks = []

    for _, signingAge, draftPick in hitters:
        ages.append(signingAge)
        if draftPick == None:
            picks.append(NOT_DRAFTED_VALUE)
        else:
            picks.append(draftPick)
        
    age_mean = torch.tensor(ages).float().mean()
    age_std = torch.tensor(ages).float().std()

    log_picks = torch.log10(torch.tensor(picks))
    log_picks_mean = log_picks.float().mean()
    log_picks_std = log_picks.float().std()
    return age_mean, age_std, log_picks_mean, log_picks_std

age_mean, age_std, log_picks_mean, log_picks_std = (None, None, None, None)
#

# Input Normalization
def Generate_Normalized_Stats(db, stats, table):
    df = pd.read_sql_query(f'''SELECT {stats} 
                           FROM {table} AS t 
                           INNER JOIN Model_Players AS mp ON mp.mlbId = t.mlbId 
                           INNER JOIN Player_CareerStatus AS pcs ON mp.mlbId = pcs.mlbId
                           WHERE pcs.careerStartYear<="{_cutoff_year}"''', db)
    std_scaler = StandardScaler()
    scaled_stats = std_scaler.fit_transform(df)
    return scaled_stats, std_scaler

def Plot_Explained_Var(stat_type, explained_var, num_stats):
    plt.plot(range(1,num_stats + 1), explained_var, marker='o')
    plt.xlabel("Number of Principal Components")
    plt.ylabel("Explained Variance")
    plt.title(F"Explained Variance Ratio for Player {stat_type}")
    
def Generate_StdDev_Ratio(variance_ratio):
    for i in range(len(variance_ratio)):
        variance_ratio[i] = math.sqrt(variance_ratio[i])
        
    sum = 0
    for x in variance_ratio:
        sum += x
    
    for i in range(len(variance_ratio)):
        variance_ratio[i] /= sum
        
    return variance_ratio

# Queries
fielding_stats_query = "PercC, Perc1B, Perc2B, Perc3B, PercSS, PercLF, PercCF, PercRF, PercDH"
hitting_stats_query = "avgRatio, obpRatio, isoRatio, wOBARatio, hrPercRatio, bbPercRatio, kPercRatio"
stealing_stats_query = "sbRateRatio, sbPercRatio"
park_factors_query = "ParkRunFactor, ParkHRFactor"
hitter_person_query = "Age, PA, Level"

pitcher_person_query = "Age, BF, Level"
pitching_stats_query = "GBPercRatio, ERARatio, FIPRatio, wOBARatio, hrPercRatio, bbPercRatio, kPercRatio"

hitter_table = "Model_HitterStats"
pitcher_table = "Model_PitcherStats"

HITTER_OUTPUT_COLS = 3
PITCHER_OUTPUT_COLS = 3

def Generate_Pca(query, table, num_components):
    global db
    data, scaler = Generate_Normalized_Stats(db, query, table)
    pca_model = PCA(num_components)
    pca_model.fit(data)
    pca_std_devs = Generate_StdDev_Ratio(pca_model.explained_variance_ratio_)
    return scaler, pca_model, pca_std_devs


# _pca_fielding = None
# _pca_hitting = None
# _pca_stealing = None
# _pca_parkfactors = None
# _pca_person = None
# _scaler_fielding = None
# _scaler_hitting = None
# _scaler_stealing = None
# _scaler_parkfactors = None
# _scaler_person = None
# _hitter_cols = 0

def Transform_Hitter(hitter_data):
    id, signingAge, draftPick = hitter_data
    fielding_stats_df = pd.read_sql_query(f"SELECT {fielding_stats_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    hitting_stats_df = pd.read_sql_query(f"SELECT {hitting_stats_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    stealing_stats_df = pd.read_sql_query(f"SELECT {stealing_stats_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    parkfactor_stats_df = pd.read_sql_query(f"SELECT {park_factors_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    player_stats_df = pd.read_sql_query(f"SELECT {hitter_person_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    
    hitter_input = torch.zeros(fielding_stats_df.shape[0] + 1, _hitter_cols)
    initVal = torch.zeros(_hitter_cols, dtype=D_TYPE)
    initVal[0] = (signingAge - age_mean) / age_std
    if draftPick is not None:
        initVal[1] = draftPick
    else:
        initVal[1] = NOT_DRAFTED_VALUE
    initVal[1] = (math.log10(initVal[1]) - log_picks_mean) / log_picks_std
    initVal[2] = 1
    hitter_input[0] = initVal
        
    if fielding_stats_df.shape[0] != 0:
        fielding_stats_pca = _pca_fielding.transform(_scaler_fielding.transform(fielding_stats_df))
        hitting_stats_pca = _pca_hitting.transform(_scaler_hitting.transform(hitting_stats_df))
        stealing_stats_pca = _pca_stealing.transform(_scaler_stealing.transform(stealing_stats_df))
        parkfactor_stats_pca = _pca_parkfactors.transform(_scaler_parkfactors.transform(parkfactor_stats_df))
        player_stats_pca = _pca_person.transform(_scaler_person.transform(player_stats_df))

        for i in range(fielding_stats_pca.shape[0]):
            hitter_input[i + 1] = torch.tensor([initVal[0], initVal[1], 0]
                                            + list(fielding_stats_pca[i])
                                            + list(hitting_stats_pca[i])
                                            + list(stealing_stats_pca[i])
                                            + list(parkfactor_stats_pca[i])
                                            + list(player_stats_pca[i]), 
                                            dtype=D_TYPE)
    
    highestLevel, pa, war, off, df, bsr = cursor.execute('''
                            SELECT pcs.highestLevel, SUM(mpw.pa), SUM(mpw.war), SUM(mpw.off), SUM(mpw.def), SUM(mpw.bsr)
                            FROM Player_CareerStatus AS pcs
                            LEFT JOIN Model_PlayerWar as mpw ON pcs.mlbId = mpw.mlbId
                            WHERE pcs.mlbId=?
                            AND (mpw.isHitter='1' or mpw.isHitter IS NULL)
                            AND pcs.isHitter IS NOT NULL
                            ''', (id,)).fetchone()
            
    
    hitter_output = torch.zeros(hitter_input.size(0), HITTER_OUTPUT_COLS)
    # out = torch.tensor([levelMap[highestLevel]], dtype=D_TYPE)
    if pa is None:
        # out = (torch.tensor([levelMap[highestLevel], 0, 0, 0, 0, 0], dtype=D_TYPE))
        out = torch.tensor([0, levelMap[highestLevel], 0], dtype=D_TYPE)
    else:
        out = (torch.tensor([war, levelMap[highestLevel], pa], dtype=D_TYPE))
        
    for i in range(hitter_output.size(0)):
        hitter_output[i] = out

    return hitter_input, hitter_output

def Generate_Hitters(
    fielding_components,
    hitting_components, 
    stealing_components,
    park_components,
    person_components
):
    global _hitter_cols
    _hitter_cols = fielding_components + hitting_components + stealing_components + park_components + person_components + SIGNING_COMPONENTS
    
    global _pca_fielding
    global _pca_hitting
    global _pca_stealing
    global _pca_parkfactors
    global _pca_person
    global _scaler_fielding
    global _scaler_hitting
    global _scaler_stealing
    global _scaler_parkfactors
    global _scaler_person
    
    _scaler_fielding, _pca_fielding, fielding_stddev = Generate_Pca(fielding_stats_query, hitter_table, fielding_components)
    _scaler_hitting, _pca_hitting, hitting_stddev, = Generate_Pca(hitting_stats_query, hitter_table, hitting_components)
    _scaler_stealing, _pca_stealing, stealing_stddev = Generate_Pca(stealing_stats_query, hitter_table, stealing_components)
    _scaler_parkfactors, _pca_parkfactors, parkfactor_stddev = Generate_Pca(park_factors_query, hitter_table, park_components)
    _scaler_person, _pca_person, person_stddev = Generate_Pca(hitter_person_query, hitter_table, person_components)
    
    hitterInput = []
    hitterOutput = []
    hitter_ids = []

    for id, signingAge, draftPick in tqdm(hitters, leave=False, desc="Hitter Data"):
        hitter_input, hitter_output = Transform_Hitter((id, signingAge, draftPick))
        hitterInput.append(hitter_input)
        hitterOutput.append(hitter_output)
        hitter_ids.append(id)
        
        
    return hitterInput, hitterOutput, (fielding_stddev, 
                                       hitting_stddev, 
                                       stealing_stddev,
                                       parkfactor_stddev,
                                       person_stddev), hitter_ids
    
def Generate_Hitter_Mutators(batch_size, max_input_size,
                             fielding_components, fielding_scale, fielding_stddev,
                                hitting_components, hitting_scale, hitting_stddev,
                                stealing_components, stealing_scale, stealing_stddev,
                                park_components, park_scale, park_stddev,
                                person_components, player_scale, player_stddev,
                                draft_scale, signing_age_scale):
    hitter_cols = fielding_components + hitting_components + stealing_components + park_components + person_components + 3
    hitting_mutators = torch.zeros(size=(batch_size, max_input_size,hitter_cols), dtype=D_TYPE)
    for n in tqdm(range(batch_size), leave=False, desc="Mutators"):
        for m in range(max_input_size):
            player_header = [0,0,0]
        
            fielding_mutator = [0] * fielding_components
            for i in range(fielding_components):
                fielding_mutator[i] = fielding_scale * random.gauss(0, fielding_stddev[i])
            
            hitting_mutator = [0] * hitting_components
            for i in range(hitting_components):
                hitting_mutator[i] = hitting_scale * random.gauss(0, hitting_stddev[i])
            
            stealing_mutator = [0] * stealing_components
            for i in range(stealing_components):
                stealing_mutator[i] = stealing_scale * random.gauss(0, stealing_stddev[i])
            
            parkfactor_mutator = [0] * park_components
            for i in range(park_components):
                parkfactor_mutator[i] = park_scale * random.gauss(0, park_stddev[i])
            
            player_mutator = [0] * person_components
            for i in range(person_components):
                player_mutator[i] = player_scale * random.gauss(0, player_stddev[i])
            
            hitting_mutators[n, m] = torch.tensor(player_header
                                            + fielding_mutator
                                            + hitting_mutator
                                            + stealing_mutator
                                            + parkfactor_mutator
                                            + player_mutator,
                                            dtype=float)
        
        signing_delta = signing_age_scale * random.gauss(0, 1)
        draft_delta = draft_scale * random.gauss(0, 1)
        hitting_mutators[0,:] = signing_delta
        hitting_mutators[1,:] = draft_delta
    
    return hitting_mutators
   
####### PITCHER VARIANTS ############

def Transform_Pitcher(pitcher_data):
    id, signingAge, draftPick = pitcher_data
    pitching_stats_df = pd.read_sql_query(f"SELECT {pitching_stats_query} FROM Model_PitcherStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    parkfactor_stats_df = pd.read_sql_query(f"SELECT {park_factors_query} FROM Model_PitcherStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    player_stats_df = pd.read_sql_query(f"SELECT {pitcher_person_query} FROM Model_PitcherStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
    
    pitcher_input = torch.zeros(pitching_stats_df.shape[0] + 1, _pitcher_cols)
    initVal = torch.zeros(_pitcher_cols, dtype=D_TYPE)
    initVal[0] = (signingAge - age_mean) / age_std
    if draftPick is not None:
        initVal[1] = draftPick
    else:
        initVal[1] = NOT_DRAFTED_VALUE
    initVal[1] = (math.log10(initVal[1]) - log_picks_mean) / log_picks_std
    initVal[2] = 1
    pitcher_input[0] = initVal
    
    if pitching_stats_df.shape[0] != 0:
        pitching_stats_pca = _pca_pitching.transform(_scaler_pitching.transform(pitching_stats_df))
        parkfactor_stats_pca = _pca_parkfactors.transform(_scaler_parkfactors.transform(parkfactor_stats_df))
        player_stats_pca = _pca_person.transform(_scaler_person.transform(player_stats_df))

        for i in range(pitching_stats_pca.shape[0]):
            pitcher_input[i + 1] = torch.tensor([initVal[0], initVal[1], 0]
                                            + list(pitching_stats_pca[i])
                                            + list(parkfactor_stats_pca[i])
                                            + list(player_stats_pca[i]), 
                                            dtype=D_TYPE)
    
    highestLevel, pa, war = cursor.execute('''
                            SELECT pcs.highestLevel, SUM(mpw.pa), SUM(mpw.war)
                            FROM Player_CareerStatus AS pcs
                            LEFT JOIN Model_PlayerWar as mpw ON pcs.mlbId = mpw.mlbId
                            WHERE pcs.mlbId=?
                            AND (mpw.isHitter='0' or mpw.isHitter IS NULL)
                            AND pcs.isPitcher IS NOT NULL
                            ''', (id,)).fetchone()
            
    
    pitcher_output = torch.zeros(pitcher_input.size(0), PITCHER_OUTPUT_COLS)
    # out = torch.tensor([levelMap[highestLevel]], dtype=D_TYPE)
    if highestLevel is None:
        out = torch.tensor([0, 6, 0], dtype=D_TYPE)
    elif pa is None:
        # out = (torch.tensor([levelMap[highestLevel], 0, 0, 0, 0, 0], dtype=D_TYPE))
        out = torch.tensor([0, levelMap[highestLevel], 0], dtype=D_TYPE)
    else:
        out = (torch.tensor([war, levelMap[highestLevel], pa], dtype=D_TYPE))
        
    for i in range(pitcher_output.size(0)):
        pitcher_output[i] = out

    return pitcher_input, pitcher_output

def Generate_Pitchers(
    pitching_components,
    park_components,
    person_components
):
    global _pitcher_cols
    _pitcher_cols = pitching_components + park_components + person_components + SIGNING_COMPONENTS
    
    global _pca_pitching
    global _pca_person
    global _scaler_pitching
    global _scaler_person
    global _pca_parkfactors
    global _scaler_parkfactors
    
    _scaler_pitching, _pca_pitching, pitching_stddev = Generate_Pca(pitching_stats_query, pitcher_table, pitching_components)
    _scaler_parkfactors, _pca_parkfactors, parkfactor_stddev = Generate_Pca(park_factors_query, hitter_table, park_components)
    _scaler_person, _pca_person, person_stddev = Generate_Pca(pitcher_person_query, pitcher_table, person_components)
    
    pitcherInput = []
    pitcherOutput = []
    pitcher_ids = []

    for id, signingAge, draftPick in tqdm(pitchers, leave=False, desc="Pitcher Data"):
        pitcher_input, pitcher_output = Transform_Pitcher((id, signingAge, draftPick))
        pitcherInput.append(pitcher_input)
        pitcherOutput.append(pitcher_output)
        pitcher_ids.append(id)
        
        
    return pitcherInput, pitcherOutput, (pitching_stddev, parkfactor_stddev,
                                       person_stddev), pitcher_ids


def Generate_Pitcher_Mutators(batch_size, max_input_size,
                             pitching_components, pitching_scale, pitching_stddev,
                             park_components, park_scale, park_stddev,
                                person_components, player_scale, player_stddev,
                                draft_scale, signing_age_scale):
    pitcher_cols = pitching_components + park_components + person_components + 3
    pitching_mutators = torch.zeros(size=(batch_size, max_input_size,pitcher_cols), dtype=D_TYPE)
    for n in tqdm(range(batch_size), leave=False, desc="Mutators"):
        for m in range(max_input_size):
            player_header = [0,0,0]
        
            pitching_mutator = [0] * pitching_components
            for i in range(pitching_components):
                pitching_mutator[i] = pitching_scale * random.gauss(0, pitching_stddev[i])
            
            parkfactor_mutator = [0] * park_components
            for i in range(park_components):
                parkfactor_mutator[i] = park_scale * random.gauss(0, park_stddev[i])
            
            player_mutator = [0] * person_components
            for i in range(person_components):
                player_mutator[i] = player_scale * random.gauss(0, player_stddev[i])
            
            pitching_mutators[n, m] = torch.tensor(player_header
                                                   + pitching_mutator
                                            + parkfactor_mutator
                                            + player_mutator,
                                            dtype=float)
        
        signing_delta = signing_age_scale * random.gauss(0, 1)
        draft_delta = draft_scale * random.gauss(0, 1)
        pitching_mutators[0,:] = signing_delta
        pitching_mutators[1,:] = draft_delta
    
    return pitching_mutators

#####################################
    
def Generate_Test_Train(input, output, test_size, random_state):
    x_train, x_test, y_train, y_test = train_test_split(input, output, test_size=test_size, random_state=random_state)

    train_lengths = torch.tensor([len(seq) for seq in x_train])
    test_lengths = torch.tensor([len(seq) for seq in x_test])

    x_train_padded = torch.nn.utils.rnn.pad_sequence(x_train)
    x_test_padded = torch.nn.utils.rnn.pad_sequence(x_test)
    y_train_padded = torch.nn.utils.rnn.pad_sequence(y_train)
    y_test_padded = torch.nn.utils.rnn.pad_sequence(y_test)
    
    return x_train_padded, x_test_padded, y_train_padded, y_test_padded, train_lengths, test_lengths