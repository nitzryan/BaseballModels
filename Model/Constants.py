import torch
import sqlite3

use_cuda = torch.cuda.is_available()
if (use_cuda):
  device = torch.device("cuda")
else:
  device = torch.device("cpu")
  
db = sqlite3.connect("../BaseballStats.db")

D_TYPE = torch.float32