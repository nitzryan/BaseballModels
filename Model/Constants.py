import torch
import sqlite3

use_cuda = torch.cuda.is_available()
if (use_cuda):
  device = torch.device("cuda")
else:
  device = torch.device("cpu")
  
db = sqlite3.connect("../BaseballStats.db")

D_TYPE = torch.float32

h_fielding_components = 4
h_hitting_components = 5
h_stealing_components = 1
h_park_components = 2
h_person_components = 3
h_init_components = 3

p_pitching_components = 5
p_park_components = 2
p_person_components = 3
p_init_components = 3