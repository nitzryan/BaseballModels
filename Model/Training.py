import torch
import matplotlib.pyplot as plt
from torch.optim import lr_scheduler
from tqdm import tqdm

from Player_Prep import Generate_Hitters, Generate_Hitter_Mutators, Generate_Test_Train
from Model import RNN_Model, RNN_Classification_Loss
from Constants import device
from Model_Train import trainAndGraph
from Dataset import HitterDataset

# Data for savings tests
xs = []
ys = []

# Create Input, Output Data
fielding_components = 8
hitting_components = 4
stealing_components = 1
park_components = 1
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
fielding_scale = 0.5
hitting_scale = 0.5
stealing_scale = 0.5
park_scale = 0.5
person_scale = 0.5
draft_scale = 0.2
signing_age_scale = 0.2

batch_size = 800
max_input_size = 79

for x_var in tqdm(range(80), desc="Mutator Options"):
    signing_age_scale = x_var / 80
    hitting_mutators = Generate_Hitter_Mutators(batch_size, max_input_size,
                        fielding_components,fielding_scale,fielding_stddev,
                        hitting_components, hitting_scale, hitting_stddev,
                        stealing_components, stealing_scale, stealing_stddev,
                        park_components, park_scale, park_stddev,
                        person_components, person_scale, person_stddev,
                        draft_scale, signing_age_scale)

    # Prepare data in form for PyTorch
    test_size = 0.25
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

    num_epochs = 301
    training_generator = torch.utils.data.DataLoader(train_hitters_dataset, batch_size=batch_size, shuffle=True)
    testing_generator = torch.utils.data.DataLoader(test_hitters_dataset, batch_size=batch_size, shuffle=False)

    loss = trainAndGraph(network, training_generator, testing_generator, loss_function, optimizer, scheduler, num_epochs, 10, early_stopping_cutoff=50, should_output=False)
    xs.append(x_var)
    ys.append(loss)
    
plt.plot(xs, ys, 'bo')
plt.xlabel('Draft Variance')
plt.ylabel("Test Loss")
plt.savefig('SigningAgeMutator.png')
plt.show()