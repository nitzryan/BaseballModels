import torch
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from torch.optim import lr_scheduler
from tqdm import tqdm
import sys

from Player_Prep import Init_Hitters, Generate_Hitters, Generate_Hitter_Mutators, Generate_Test_Train
from Model import RNN_Model, RNN_Classification_Loss
from Constants import device
from Model_Train import trainAndGraph
from Dataset import HitterDataset

from Output import Delete_Model_Run_Hitter, Generate_Model_Run_Hitter, Setup_Players
from Constants import db

cursor = db.cursor()
all_hitter_ids = cursor.execute("SELECT mlbId FROM Model_Players WHERE isHitter='1'").fetchall()
year = int(sys.argv[1])
Init_Hitters(year)
# Data for savings tests
xs = []
losses = []

# Create Input, Output Data
fielding_components = 4
hitting_components = 5
stealing_components = 1
park_components = 2
person_components = 3
init_components = 3

input_size = fielding_components + hitting_components + stealing_components + park_components + person_components + init_components

hitter_input, hitter_output, (fielding_stddev,
                            hitting_stddev,
                            stealing_stddev,
                            park_stddev,
                            person_stddev), hitter_ids = Generate_Hitters(fielding_components,
                        hitting_components,
                        stealing_components,
                        park_components,
                        person_components)

# Create mutations to create synthetic data
fielding_scale = 0.6
hitting_scale = 0.2
stealing_scale = 0.3
park_scale = 0.1
person_scale = 0.3
draft_scale = 0.2
signing_age_scale = 0.5

batch_size = 800
max_input_size = 0
for i in hitter_input:
    if i.shape[0] > max_input_size:
        max_input_size = i.shape[0]

# for x_var in tqdm(np.arange(0.01, 1.0, 0.01), desc="Mutator Options", leave=True):
    # test_size = x_var
hitting_mutators = Generate_Hitter_Mutators(batch_size, max_input_size,
                    fielding_components,fielding_scale,fielding_stddev,
                    hitting_components, hitting_scale, hitting_stddev,
                    stealing_components, stealing_scale, stealing_stddev,
                    park_components, park_scale, park_stddev,
                    person_components, person_scale, person_stddev,
                    draft_scale, signing_age_scale)

# Prepare data in form for PyTorch
test_size = 0.2
random_state = 1

x_train_padded, x_test_padded, y_train_padded, y_test_padded, train_lengths, test_lengths = Generate_Test_Train(
    hitter_input, hitter_output, test_size, random_state)

train_hitters_dataset = HitterDataset(x_train_padded, train_lengths, y_train_padded)
test_hitters_dataset = HitterDataset(x_test_padded, test_lengths, y_test_padded)

# Train Model
dropout_perc = 0.0
num_layers = 5
hidden_size = 30

Setup_Players(all_hitter_ids, hitter_ids)
model_name = sys.argv[2]

cursor = db.cursor()
cursor.execute(f'''DELETE FROM Model_TrainingHistory 
               WHERE Year=? 
               AND IsHitter="1" 
               AND ModelName LIKE "{model_name}%"''', (year,))

for n in tqdm(range(int(sys.argv[3]))):
    network = RNN_Model(input_size, num_layers, hidden_size, dropout_perc, hitting_mutators)
    network = network.to(device)

    optimizer = torch.optim.Adam(network.parameters(), lr=0.003)
    scheduler = lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.5, patience=20, cooldown=5, verbose=False)
    loss_function = RNN_Classification_Loss

    num_epochs = 300
    training_generator = torch.utils.data.DataLoader(train_hitters_dataset, batch_size=batch_size, shuffle=True)
    testing_generator = torch.utils.data.DataLoader(test_hitters_dataset, batch_size=batch_size, shuffle=False)

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
    cursor.execute("INSERT INTO Model_TrainingHistory VALUES(?,?,?,?)", (this_model_name, year, True, loss))
    db.commit()
    # model = f"test_run_hitters_{n}"
    # Delete_Model_Run_Hitter(model)
    # Generate_Model_Run_Hitter(model, network, device, leave_progress=False)

