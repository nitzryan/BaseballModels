import torch
import numpy as np
from Constants import D_TYPE
import warnings

WAR_BUCKETS = torch.tensor([0,1,5,10,15,20,np.inf], dtype=D_TYPE)
LEVEL_BUCKETS = torch.tensor([0,1,2,3,4,5,6, np.inf], dtype=D_TYPE)
PA_BUCKETS = torch.tensor([0, 50, 200, 1000, 2000, np.inf], dtype=D_TYPE)

class HitterDataset(torch.utils.data.Dataset):
    def __init__(self, data, lengths, labels):
        self.data = data
        self.lengths = lengths
        with warnings.catch_warnings(): # Get warning for data copy, which is okay since this is only run once
            warnings.filterwarnings("ignore", category=UserWarning, message='.*non-contiguous.*')
            self.war_buckets = torch.bucketize(labels[:,:,0], WAR_BUCKETS).squeeze(-1)
            self.level_buckets = torch.bucketize(labels[:,:,1], LEVEL_BUCKETS).squeeze(-1)
            self.pa_buckets = torch.bucketize(labels[:,:,2], PA_BUCKETS).squeeze(-1)
        
    def __len__(self):
        return self.data.size(dim=1)
    
    def should_augment_data(self, should_augment):
        self.should_augment = should_augment
    
    # def Get_Bincounts(self):
    #     bincounts = torch.zeros((BUCKET_MAXES.size(0),))
    #     for n in range(self.labels.size(1)):
    #         bin = self.labels[0,n].item()
    #         bincounts[bin] += 1
    #     return bincounts
    
    def __getitem__(self, idx):
        return self.data[:,idx], self.lengths[idx], self.war_buckets[:,idx], self.level_buckets[:,idx], self.pa_buckets[:,idx]
    
