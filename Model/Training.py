import torch
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from torch.optim import lr_scheduler
from tqdm import tqdm

from Player_Prep import Generate_Hitters, Generate_Hitter_Mutators, Generate_Test_Train
from Model import RNN_Model, RNN_Classification_Loss
from Constants import device
from Model_Train import trainAndGraph
from Dataset import HitterDataset

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
                            person_stddev) = Generate_Hitters(fielding_components,
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
max_input_size = 79

for x_var in tqdm(np.arange(0.01, 1.0, 0.01), desc="Mutator Options", leave=True):
    test_size = x_var
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
    num_layers = 2
    hidden_size = 50
    network = RNN_Model(input_size, num_layers, hidden_size, dropout_perc, hitting_mutators)
    network = network.to(device)

    optimizer = torch.optim.Adam(network.parameters(), lr=0.003)
    scheduler = lr_scheduler.ReduceLROnPlateau(optimizer, factor=0.5, patience=20, cooldown=5, verbose=False)
    loss_function = RNN_Classification_Loss

    num_epochs = 300
    training_generator = torch.utils.data.DataLoader(train_hitters_dataset, batch_size=batch_size, shuffle=True)
    testing_generator = torch.utils.data.DataLoader(test_hitters_dataset, batch_size=batch_size, shuffle=False)

    loss = trainAndGraph(network, training_generator, testing_generator, loss_function, optimizer, scheduler, num_epochs, 10, early_stopping_cutoff=40, should_output=False)
    xs.append(x_var)
    losses.append(loss)
   
# Plot Heatmap
# x_unique = np.unique(xs)
# y_unique = np.unique(ys)
# heatmap_data = np.zeros((len(y_unique), len(x_unique)))
# for i in range(len(xs)):
#     x_idx = np.where(x_unique == xs[i])[0][0]
#     y_idx = np.where(y_unique == ys[i])[0][0]
#     heatmap_data[y_idx, x_idx] = losses[i]

# plt.subplots(figsize=(0.75 * len(x_unique) + 2.5, 0.75 * len(y_unique) + 2.5))
# y_labels = [f"{y:.2f}" for y in y_unique]

# sns.heatmap(heatmap_data, xticklabels=x_unique, yticklabels=y_labels, 
#             cmap='flare', vmin=0.65, vmax=0.72, annot=True, fmt='.3f',
#             square=True)

# #plt.figure(figsize=(2 * len(x_unique) + 1, 2 * len(y_unique) + 1))
# plt.xlabel('Hitting PCA Components')
# plt.ylabel("Hitting Mutator Variance")
# plt.title("Hitting Model Parameters")
# plt.savefig('HittingParameters.png')
# plt.show()

# Plot 2D
plt.plot(xs, losses, 'bo')
plt.xlabel('Test Size')
plt.ylabel('Test Loss')
plt.title('Test Loss vs Test Size')
plt.savefig('TestSize.png')
plt.show()