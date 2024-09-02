import torch
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import math
import random
from tqdm import tqdm
from Constants import db, D_TYPE
# Common Variables

NOT_DRAFTED_VALUE = 1000
NOT_INIT_DRAFT_VALUE = 100
levelMap = {1:0,11:1,12:2,13:3,14:4,15:5,16:6,17:7}
np.set_printoptions(precision=2, linewidth=300, floatmode='fixed')

cursor = db.cursor()
hitters = cursor.execute('''
                         SELECT mp.mlbId, mp.ageAtSigningYear, p.draftPick
                         FROM Model_Players AS mp
                         INNER JOIN Player as p ON mp.mlbId = p.mlbId
                         WHERE mp.isHitter='1'
                         ORDER BY mp.mlbId
                         ''').fetchall()

pitchers = cursor.execute('''
                         SELECT mp.mlbId, mp.ageAtSigningYear, p.draftPick
                         FROM Model_Players AS mp
                         INNER JOIN Player as p ON mp.mlbId = p.mlbId
                         WHERE mp.isPitcher='1'
                         ''').fetchall()

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

age_mean, age_std, log_picks_mean, log_picks_std = Get_Age_Draft_Data()

# Input Normalization
def Generate_Normalized_Stats(db, stats, table):
    df = pd.read_sql_query(f"SELECT {stats} FROM {table}", db)
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
hitter_person_query = "AGE, PA, Level"

hitter_table = "Model_HitterStats"
pitcher_table = "Model_PitcherStats"

HITTER_OUTPUT_COLS = 1

def Generate_Pca(query, table, num_components):
    global db
    data, scaler = Generate_Normalized_Stats(db, query, table)
    pca_model = PCA(num_components)
    pca_model.fit(data)
    pca_std_devs = Generate_StdDev_Ratio(pca_model.explained_variance_ratio_)
    return scaler, pca_model, pca_std_devs

def Generate_Hitters(
    fielding_components,
    hitting_components, 
    stealing_components,
    park_components,
    person_components
):
    hitter_cols = fielding_components + hitting_components + stealing_components + park_components + person_components + SIGNING_COMPONENTS
    
    scaler_fielding, pca_fielding, fielding_stddev = Generate_Pca(fielding_stats_query, hitter_table, fielding_components)
    scaler_hitting, pca_hitting, hitting_stddev, = Generate_Pca(hitting_stats_query, hitter_table, hitting_components)
    scaler_stealing, pca_stealing, stealing_stddev = Generate_Pca(stealing_stats_query, hitter_table, stealing_components)
    scaler_parkfactors, pca_parkfactors, parkfactor_stddev = Generate_Pca(park_factors_query, hitter_table, park_components)
    scaler_person, pca_person, person_stddev = Generate_Pca(hitter_person_query, hitter_table, person_components)
    
    hitterInput = []
    hitterOutput = []

    for id, signingAge, draftPick in tqdm(hitters, leave=False, desc="Hitter Data"):
        fielding_stats_df = pd.read_sql_query(f"SELECT {fielding_stats_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
        hitting_stats_df = pd.read_sql_query(f"SELECT {hitting_stats_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
        stealing_stats_df = pd.read_sql_query(f"SELECT {stealing_stats_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
        parkfactor_stats_df = pd.read_sql_query(f"SELECT {park_factors_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
        player_stats_df = pd.read_sql_query(f"SELECT {hitter_person_query} FROM Model_HitterStats WHERE mlbId='{id}' ORDER BY YEAR ASC, Month ASC", db)
        
        fielding_stats_pca = pca_fielding.transform(scaler_fielding.transform(fielding_stats_df))
        hitting_stats_pca = pca_hitting.transform(scaler_hitting.transform(hitting_stats_df))
        stealing_stats_pca = pca_stealing.transform(scaler_stealing.transform(stealing_stats_df))
        parkfactor_stats_pca = pca_parkfactors.transform(scaler_parkfactors.transform(parkfactor_stats_df))
        player_stats_pca = pca_person.transform(scaler_person.transform(player_stats_df))
        
        thisInputs = torch.zeros(fielding_stats_pca.shape[0] + 1, hitter_cols)
        initVal = torch.zeros(hitter_cols, dtype=D_TYPE)
        initVal[0] = (signingAge - age_mean) / age_std
        if draftPick is not None:
            initVal[1] = draftPick
        else:
            initVal[1] = NOT_DRAFTED_VALUE
        initVal[1] = (math.log10(initVal[1]) - log_picks_mean) / log_picks_std
        initVal[2] = 1
        
        thisInputs[0] = initVal
        for i in range(fielding_stats_pca.shape[0]):
            thisInputs[i + 1] = torch.tensor([initVal[0], initVal[1], 0]
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
                                AND pcs.position='hitting'
                                AND pcs.isPrimaryPosition='1'
                                ''', (id,)).fetchone()
                
        hitterInput.append(thisInputs)
        
        thisOutputs = torch.zeros(thisInputs.size(0), HITTER_OUTPUT_COLS)
        # out = torch.tensor([levelMap[highestLevel]], dtype=D_TYPE)
        if pa is None:
            # out = (torch.tensor([levelMap[highestLevel], 0, 0, 0, 0, 0], dtype=D_TYPE))
            out = torch.tensor([0], dtype=D_TYPE)
        else:
            out = (torch.tensor([war], dtype=D_TYPE))
            
        for i in range(thisOutputs.size(0)):
            thisOutputs[i] = out
        hitterOutput.append(thisOutputs)
        
    return hitterInput, hitterOutput, (fielding_stddev, 
                                       hitting_stddev, 
                                       stealing_stddev,
                                       parkfactor_stddev,
                                       person_stddev)
    
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
    
def Generate_Test_Train(hitter_input, hitter_output, test_size, random_state):
    x_train, x_test, y_train, y_test = train_test_split(hitter_input, hitter_output, test_size=test_size, random_state=random_state)

    train_lengths = torch.tensor([len(seq) for seq in x_train])
    test_lengths = torch.tensor([len(seq) for seq in x_test])

    x_train_padded = torch.nn.utils.rnn.pad_sequence(x_train)
    x_test_padded = torch.nn.utils.rnn.pad_sequence(x_test)
    y_train_padded = torch.nn.utils.rnn.pad_sequence(y_train)
    y_test_padded = torch.nn.utils.rnn.pad_sequence(y_test)
    
    return x_train_padded, x_test_padded, y_train_padded, y_test_padded, train_lengths, test_lengths