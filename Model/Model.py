import torch
import torch.nn as nn
import torch.nn.functional as F
from Dataset import BUCKET_MAXES

class RNN_Model(nn.Module):
    def __init__(self, input_size, num_layers, hidden_size, dropout_perc, mutators):
        super().__init__()
        
        self.rnn = nn.RNN(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers, batch_first=False, dropout=dropout_perc)
        self.linear1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear2 = nn.Linear(hidden_size // 2, len(BUCKET_MAXES))
        # self.linear = nn.Linear(hidden_size, len(BUCKET_MAXES))
        self.mutators = mutators
        
    def to(self, *args, **kwargs):
        self.mutators = self.mutators.to(*args, **kwargs)
        return super(RNN_Model, self).to(*args, **kwargs)
        
    def forward(self, x, lengths):
        if self.training:
            x += self.mutators[:x.size(0), :x.size(1), :]
        lengths = lengths.to(torch.device("cpu")).long()
        packedInput = nn.utils.rnn.pack_padded_sequence(x, lengths, batch_first=True, enforce_sorted=False)
        packedOutput, (h_n, c_n) = self.rnn(packedInput)
        output, _ = nn.utils.rnn.pad_packed_sequence(packedOutput, batch_first=True)
            
        output = F.leaky_relu(self.linear1(output))
        output = self.linear2(output)
        # output = self.linear(output)
        return output
    
def RNN_Classification_Loss(pred, actual, lengths):
    # Reshape into format required by CrossEntropyLoss
    actual = actual[:,:pred.size(1)]
    batch_size = actual.size(0)
    time_steps = actual.size(1)
    num_classes = pred.size(2)
    actual = actual.reshape((batch_size * time_steps,))
    pred = pred.reshape((batch_size * time_steps, num_classes))
    # Calculate Loss for each time step (regardless of whether it was actually used)
    l = nn.CrossEntropyLoss(reduction='none')
    loss = l(pred, actual)
    
    # Reshape back into format to apply mask for actual valid predictions
    loss = loss.reshape((batch_size, time_steps))
    
    # Mask based off lenghts of actual predictions
    batch_size, max_steps = loss.size()
    mask = torch.arange(max_steps, device=lengths.device).unsqueeze(0) < lengths.unsqueeze(1)
    maskedLoss = loss * mask
    
    # Calculate average loss of each entry (although not sure if this is actually good)
    lossSums = maskedLoss.sum(dim=1)
    lengths = lengths.float()
    lossMeans = lossSums / lengths.unsqueeze(1)
    
    return lossMeans.mean()