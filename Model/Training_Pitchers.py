import torch
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from torch.optim import lr_scheduler
from tqdm import tqdm
import sys

from Player_Prep import Generate_Pitchers, Generate_Pitcher_Mutators, Generate_Test_Train
from Model import RNN_Model, RNN_Classification_Loss
from Constants import device
from Model_Train import trainAndGraph
from Dataset import PitcherDataset

from Output import Delete_Model_Run_Pitcher, Generate_Model_Run_Pitcher, Setup_Pitchers
from Constants import db

cursor = db.cursor()
all_pitcher_ids = cursor.execute("SELECT mlbId FROM Model_Players WHERE isHitter='0'").fetchall()
# Data for savings tests
xs = []
losses = []

# Create Input, Output Data
pitching_components = 5
park_components = 2
person_components = 3
init_components = 3

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
max_input_size = 79

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
num_layers = 5
hidden_size = 30

Setup_Pitchers(all_pitcher_ids, pitcher_ids)

for n in tqdm(range(int(sys.argv[1]))):
    network = RNN_Model(input_size, num_layers, hidden_size, dropout_perc, pitching_mutators)
    network = network.to(device)

    optimizer = torch.optim.Adam(network.parameters(), lr=0.003)
    scheduler = lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.5, patience=20, cooldown=5, verbose=False)
    loss_function = RNN_Classification_Loss

    num_epochs = 300
    training_generator = torch.utils.data.DataLoader(train_pitchers_dataset, batch_size=batch_size, shuffle=True)
    testing_generator = torch.utils.data.DataLoader(test_pitchers_dataset, batch_size=batch_size, shuffle=False)

    loss = trainAndGraph(network, training_generator, testing_generator, loss_function, optimizer, scheduler, num_epochs, 10, early_stopping_cutoff=40, should_output=False)
    model = f"test_run_pitchers_{n}"
    Delete_Model_Run_Pitcher(model)
    Generate_Model_Run_Pitcher(model, network, device, leave_progress=False)
