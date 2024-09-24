import torch
import torch.nn as nn
import torch.nn.functional as F
from Dataset import WAR_BUCKETS, LEVEL_BUCKETS, PA_BUCKETS

class RNN_Model(nn.Module):
    def __init__(self, input_size, num_layers, hidden_size, dropout_perc, mutators):
        super().__init__()
        
        self.rnn = nn.RNN(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers, batch_first=False, dropout=dropout_perc)
        self.linear_war1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear_war2 = nn.Linear(hidden_size // 2, len(WAR_BUCKETS))
        self.linear_level1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear_level2 = nn.Linear(hidden_size // 2, len(LEVEL_BUCKETS))
        self.linear_pa1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear_pa2 = nn.Linear(hidden_size // 2, len(PA_BUCKETS))
        # self.linear = nn.Linear(hidden_size, len(BUCKET_MAXES))
        self.mutators = mutators
        
    def to(self, *args, **kwargs):
        self.mutators = self.mutators.to(*args, **kwargs)
        return super(RNN_Model, self).to(*args, **kwargs)
        
    def forward(self, x, lengths):
        if self.training:
            x += self.mutators[:x.size(0), :x.size(1), :]
        # Apply lengths since they aren't the same for all entries
        lengths = lengths.to(torch.device("cpu")).long()
        packedInput = nn.utils.rnn.pack_padded_sequence(x, lengths, batch_first=True, enforce_sorted=False)
        
        # Generate Player State
        packedOutput, h_n = self.rnn(packedInput)
        output, _ = nn.utils.rnn.pad_packed_sequence(packedOutput, batch_first=True)
            
        # Generate War predictions
        output_war = F.leaky_relu(self.linear_war1(output))
        output_war = self.linear_war2(output_war)
        
        # Generate Level Predictions
        output_level = F.leaky_relu(self.linear_level1(output))
        output_level = self.linear_level2(output_level)
        
        # Generate PA Predictions
        output_pa = F.leaky_relu(self.linear_pa1(output))
        output_pa = self.linear_pa2(output_pa)
        # output = self.linear(output)
        return output_war, output_level, output_pa
    
class LSTM_Model(nn.Module):
    def __init__(self, input_size, num_layers, hidden_size, dropout_perc, mutators):
        super().__init__()
        
        self.lstm = nn.LSTM(input_size=input_size, hidden_size=hidden_size, num_layers=num_layers, batch_first=False, dropout=dropout_perc)
        self.linear_war1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear_war2 = nn.Linear(hidden_size // 2, len(WAR_BUCKETS))
        self.linear_level1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear_level2 = nn.Linear(hidden_size // 2, len(LEVEL_BUCKETS))
        self.linear_pa1 = nn.Linear(hidden_size, hidden_size // 2)
        self.linear_pa2 = nn.Linear(hidden_size // 2, len(PA_BUCKETS))
        # self.linear = nn.Linear(hidden_size, len(BUCKET_MAXES))
        self.mutators = mutators
        
    def to(self, *args, **kwargs):
        self.mutators = self.mutators.to(*args, **kwargs)
        return super(LSTM_Model, self).to(*args, **kwargs)
        
    def forward(self, x, lengths):
        if self.training:
            x += self.mutators[:x.size(0), :x.size(1), :]
        # Apply lengths since they aren't the same for all entries
        lengths = lengths.to(torch.device("cpu")).long()
        packedInput = nn.utils.rnn.pack_padded_sequence(x, lengths, batch_first=True, enforce_sorted=False)
        
        # Generate Player State
        packedOutput, _ = self.lstm(packedInput)
        output, _ = nn.utils.rnn.pad_packed_sequence(packedOutput, batch_first=True)
            
        # Generate War predictions
        output_war = F.leaky_relu(self.linear_war1(output))
        output_war = self.linear_war2(output_war)
        
        # Generate Level Predictions
        output_level = F.leaky_relu(self.linear_level1(output))
        output_level = self.linear_level2(output_level)
        
        # Generate PA Predictions
        output_pa = F.leaky_relu(self.linear_pa1(output))
        output_pa = self.linear_pa2(output_pa)
        # output = self.linear(output)
        return output_war, output_level, output_pa
    
def RNN_Classification_Loss(pred_war, pred_level, pred_pa, actual_war, actual_level, actual_pa, lengths):
    # Reshape into format required by CrossEntropyLoss
    actual_war = actual_war[:,:pred_war.size(1)]
    actual_level = actual_level[:,:pred_level.size(1)]
    actual_pa = actual_pa[:,:pred_pa.size(1)]
    
    batch_size = actual_war.size(0)
    time_steps = actual_war.size(1)
    
    num_classes_war = pred_war.size(2)
    num_classes_level = pred_level.size(2)
    num_classes_pa = pred_pa.size(2)
    
    actual_war = actual_war.reshape((batch_size * time_steps,))
    actual_level = actual_level.reshape((batch_size * time_steps,))
    actual_pa = actual_pa.reshape((batch_size * time_steps,))
    
    pred_war = pred_war.reshape((batch_size * time_steps, num_classes_war))
    pred_level = pred_level.reshape((batch_size * time_steps, num_classes_level))
    pred_pa = pred_pa.reshape((batch_size * time_steps, num_classes_pa))
    
    # Calculate Loss for each time step (regardless of whether it was actually used)
    l = nn.CrossEntropyLoss(reduction='none')
    loss_war = l(pred_war, actual_war)
    loss_level = l(pred_level, actual_level)
    loss_pa = l(pred_pa, actual_pa)
    
    # Reshape back into format to apply mask for actual valid predictions
    loss_war = loss_war.reshape((batch_size, time_steps))
    loss_level = loss_level.reshape((batch_size, time_steps))
    loss_pa = loss_pa.reshape((batch_size, time_steps))
    
    # Mask based off lenghts of actual predictions
    batch_size, max_steps = loss_war.size()
    mask = torch.arange(max_steps, device=lengths.device).unsqueeze(0) < lengths.unsqueeze(1)
    masked_loss_war = loss_war * mask
    masked_loss_level = loss_level * mask
    masked_loss_pa = loss_pa * mask
    
    # Calculate average loss of each entry (although not sure if this is actually good)
    loss_sums_war = masked_loss_war.sum(dim=1)
    loss_sums_level = masked_loss_level.sum(dim=1)
    loss_sums_pa = masked_loss_pa.sum(dim=1)
    lengths = lengths.float()
    loss_mean_war = loss_sums_war / lengths.unsqueeze(1)
    loss_mean_level = loss_sums_level / lengths.unsqueeze(1)
    loss_mean_pa = loss_sums_pa / lengths.unsqueeze(1)
    
    return loss_mean_war.mean(), loss_mean_level.mean(), loss_mean_pa.mean()