import torch
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from torch.optim import lr_scheduler
from tqdm import tqdm
import sys

from Player_Prep import Init_Pitchers, Generate_Pitchers, Generate_Pitcher_Mutators, Generate_Test_Train
from Model import RNN_Model, LSTM_Model, RNN_Classification_Loss
from Constants import device
from Model_Train import trainAndGraph
from Dataset import PitcherDataset

from Output import Delete_Model_Run_Pitcher, Generate_Model_Run_Pitcher, Setup_Pitchers
from Constants import db, p_init_components, p_park_components, p_person_components, p_pitching_components

cursor = db.cursor()
all_pitcher_ids = cursor.execute("SELECT mlbId FROM Model_Players WHERE isHitter='0'").fetchall()
year = int(sys.argv[1])
Init_Pitchers(year)
# Data for savings tests
xs = []
losses = []

# Create Input, Output Data
pitching_components = p_pitching_components
park_components = p_park_components
person_components = p_person_components
init_components = p_init_components

input_size = pitching_components + park_components + person_components + init_components

pitcher_input, pitcher_output, (pitching_stddev,
                                park_stddev,
                            person_stddev), pitcher_ids = Generate_Pitchers(pitching_components,
                        park_components,
                        person_components)

# Create mutations to create synthetic data
pitching_scale = 0.2
person_scale = 0.3
draft_scale = 0.2
park_scale = 0.1
signing_age_scale = 0.5

batch_size = 800
max_input_size = 0
for i in pitcher_input:
    if i.shape[0] > max_input_size:
        max_input_size = i.shape[0]

# for x_var in tqdm(np.arange(0.01, 1.0, 0.01), desc="Mutator Options", leave=True):
    # test_size = x_var
pitching_mutators = Generate_Pitcher_Mutators(batch_size, max_input_size,
                        pitching_components,pitching_scale,pitching_stddev,
                        park_components, park_scale, park_stddev,
                        person_components, person_scale, person_stddev,
                        draft_scale, signing_age_scale)

# Prepare data in form for PyTorch
test_size = 0.2
random_state = 1

x_train_padded, x_test_padded, y_train_padded, y_test_padded, train_lengths, test_lengths = Generate_Test_Train(
    pitcher_input, pitcher_output, test_size, random_state)

train_pitchers_dataset = PitcherDataset(x_train_padded, train_lengths, y_train_padded)
test_pitchers_dataset = PitcherDataset(x_test_padded, test_lengths, y_test_padded)

# Train Model
dropout_perc = 0.0
num_layers = 3
hidden_size = 30

Setup_Pitchers(all_pitcher_ids, pitcher_ids)

model_name = sys.argv[2]

cursor = db.cursor()
cursor.execute(f'''DELETE FROM Model_TrainingHistory 
               WHERE Year=? 
               AND IsHitter="0" 
               AND ModelName LIKE "{model_name}%"''', (year,))
db.commit()
cursor = db.cursor()

# Get the next model idx
last_model_idx = cursor.execute("SELECT DISTINCT ModelIdx FROM Model_TrainingHistory ORDER BY ModelIdx DESC LIMIT 1").fetchone()
if last_model_idx == None:
    model_idx = 1
else:
    model_idx = last_model_idx[0] + 1

training_generator = torch.utils.data.DataLoader(train_pitchers_dataset, batch_size=batch_size, shuffle=True)
testing_generator = torch.utils.data.DataLoader(test_pitchers_dataset, batch_size=batch_size, shuffle=False)

for n in tqdm(range(int(sys.argv[3])), desc="Model Training Iterations"):
    network = LSTM_Model(input_size, num_layers, hidden_size, dropout_perc, pitching_mutators)
    network = network.to(device)

    optimizer = torch.optim.Adam(network.parameters(), lr=0.003)
    scheduler = lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.5, patience=20, cooldown=5, verbose=False)
    loss_function = RNN_Classification_Loss

    num_epochs = 300
    

    this_model_name = model_name + '_' + str(n) + '.pt'
    loss = trainAndGraph(network, 
                         training_generator, 
                         testing_generator, 
                         loss_function, 
                         optimizer, 
                         scheduler, 
                         num_epochs, 
                         10, 
                         early_stopping_cutoff=40, 
                         should_output=False,
                         model_name="Models/" + this_model_name)
    
    cursor = db.cursor()
    cursor.execute("INSERT INTO Model_TrainingHistory VALUES(?,?,?,ROUND(?,3),?,?,?)", (this_model_name, year, False, loss, num_layers, hidden_size, model_idx))
    db.commit()
