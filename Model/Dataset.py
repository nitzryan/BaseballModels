import torch
import numpy as np
from Constants import D_TYPE

BUCKET_MAXES = torch.tensor([0,1,5,10,15,20,np.inf], dtype=D_TYPE)

class HitterDataset(torch.utils.data.Dataset):
    def __init__(self, data, lengths, labels):
        self.data = data
        self.lengths = lengths
        self.labels = torch.bucketize(labels, BUCKET_MAXES).squeeze(-1)
        
    def __len__(self):
        return self.data.size(dim=1)
    
    def should_augment_data(self, should_augment):
        self.should_augment = should_augment
    
    def Get_Bincounts(self):
        bincounts = torch.zeros((BUCKET_MAXES.size(0),))
        for n in range(self.labels.size(1)):
            bin = self.labels[0,n].item()
            bincounts[bin] += 1
        return bincounts
    
    def __getitem__(self, idx):
        return self.data[:,idx], self.lengths[idx], self.labels[:,idx]
    
