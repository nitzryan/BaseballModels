import torch
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from torch.optim import lr_scheduler
from tqdm import tqdm

from Player_Prep import Generate_Pitchers, Generate_Pitcher_Mutators, Generate_Test_Train, Init_Pitchers
from Model import RNN_Model, RNN_Classification_Loss
from Constants import device
from Model_Train import trainAndGraph
from Dataset import PitcherDataset

from Constants import db, p_init_components, p_park_components, p_person_components, p_pitching_components

cursor = db.cursor()
all_pitcher_ids = cursor.execute("SELECT mlbId FROM Model_Players WHERE isPitcher='1'").fetchall()
year = int(2023)
Init_Pitchers(year)
# Data for savings tests
xs = []
ys = []
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

training_generator = torch.utils.data.DataLoader(train_pitchers_dataset, batch_size=batch_size, shuffle=True)
testing_generator = torch.utils.data.DataLoader(test_pitchers_dataset, batch_size=batch_size, shuffle=False)
    # Train Model
for x_var in tqdm(range(1, 10), desc="Num Layers"):
    for y_var in tqdm(range(10, 91, 10), desc="Hidden Size", leave=False):
        lr = 0.003
        dropout_perc = 0.0
        num_layers = 5
        hidden_size = 30

        num_layers = x_var
        hidden_size = y_var

        network = RNN_Model(input_size, num_layers, hidden_size, dropout_perc, pitching_mutators)
        network = network.to(device)

        optimizer = torch.optim.Adam(network.parameters(), lr=lr)
        scheduler = lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.5, patience=20, cooldown=5, verbose=False)
        loss_function = RNN_Classification_Loss

        num_epochs = 300

        loss = trainAndGraph(network, training_generator, testing_generator, loss_function, optimizer, scheduler, num_epochs, 10, early_stopping_cutoff=40, should_output=False)
        xs.append(x_var)
        ys.append(y_var)
        losses.append(loss)
        del network, optimizer
        torch.cuda.empty_cache()
   
# Plot Heatmap
x_unique = np.unique(xs)
y_unique = np.unique(ys)
heatmap_data = np.zeros((len(y_unique), len(x_unique)))
for i in range(len(xs)):
    x_idx = np.where(x_unique == xs[i])[0][0]
    y_idx = np.where(y_unique == ys[i])[0][0]
    heatmap_data[y_idx, x_idx] = losses[i]

plt.subplots(figsize=(0.75 * len(x_unique) + 2.5, 0.75 * len(y_unique) + 2.5))
y_labels = [f"{y:.2f}" for y in y_unique]

sns.heatmap(heatmap_data, xticklabels=x_unique, yticklabels=y_labels, 
            cmap='flare', annot=True, fmt='.3f',
            square=True)

#plt.figure(figsize=(2 * len(x_unique) + 1, 2 * len(y_unique) + 1))
plt.xlabel('Num Layers')
plt.ylabel("Hidden Size")
plt.title("Model Parameters Pitching")
plt.savefig('img/ModelSizeParametersParametersPitching.png')
plt.show()