import matplotlib.pyplot as plt
import torch
from tqdm import tqdm
from Constants import device

def train(network,  data_generator, loss_function, optimizer, logging = 200, should_output=True):
  network.train() #updates any network layers that behave differently in training and execution
  avg_loss = [0,0,0]
  num_batches = 0
  for batch, (data, length, target_war, target_level, target_pa) in enumerate(data_generator):
    data, length = data.to(device), length.to(device)
    target_war, target_level, target_pa = target_war.to(device), target_level.to(device), target_pa.to(device)
    optimizer.zero_grad()
    output_war, output_level, output_pa = network(data, length)
    loss_war, loss_level, loss_pa = loss_function(output_war, output_level, output_pa, target_war, target_level, target_pa, length)
    loss_war.backward(retain_graph=True)
    loss_level.backward(retain_graph=True)
    loss_pa.backward()
    optimizer.step()
    avg_loss[0] += loss_war.item()
    avg_loss[1] += loss_level.item()
    avg_loss[2] += loss_pa.item()
    num_batches += 1
    if should_output and ((batch+1)%logging == 0): print('Batch [%d/%d], Train Loss: %.4f' %(batch+1, len(data_generator.dataset)/len(output_war), avg_loss/num_batches))
  
  avg_loss[0] /= num_batches
  avg_loss[1] /= num_batches
  avg_loss[2] /= num_batches
  
  return avg_loss

def test(network, test_loader, loss_function):
  network.eval() #updates any network layers that behave differently in training and execution
  avg_loss = [0,0,0]
  num_batches = 0
  with torch.no_grad():
    for data, length, target_war, target_level, target_pa in test_loader:
      data, length = data.to(device), length.to(device)
      target_war, target_level, target_pa = target_war.to(device), target_level.to(device), target_pa.to(device)
      output_war, output_level, output_pa = network(data, length)
      loss_war, loss_level, loss_pa = loss_function(output_war, output_level, output_pa, target_war, target_level, target_pa, length)
      avg_loss[0] += loss_war.item()
      avg_loss[1] += loss_level.item()
      avg_loss[2] += loss_pa.item()
      num_batches += 1
  
  avg_loss[0] /= num_batches
  avg_loss[1] /= num_batches
  avg_loss[2] /= num_batches
  #print('\nTest set: Avg. loss: {:.4f})\n'.format(test_loss))
  return avg_loss

def count_parameters(model):
    return sum(p.numel() for p in model.parameters() if p.requires_grad)

def logResults(epoch, num_epochs, train_loss, test_loss, print_interval=1000, should_output=True):
  if should_output and (epoch%print_interval == 0):  
    print('Epoch [%d/%d], Train Loss: %.4f, Test Loss: %.4f' %(epoch+1, num_epochs, train_loss, test_loss))

def graphLoss(epoch_counter, train_loss_hist, test_loss_hist, loss_name="Loss", start = 0, graph_y_range=None, title=""):
  fig = plt.figure()
  plt.plot(epoch_counter[start:], train_loss_hist[start:], color='blue')
  plt.plot(epoch_counter[start:], test_loss_hist[start:], color='red')
  plt.title(title)
  if graph_y_range is not None:
    plt.ylim(graph_y_range)
  plt.legend(['Train Loss', 'Test Loss'], loc='upper right')
  plt.xlabel('#Epochs')
  plt.ylabel(loss_name)

def trainAndGraph(network, training_generator, testing_generator, loss_function, optimizer, scheduler, num_epochs, logging_interval=1, early_stopping_cutoff=20, should_output=True, graph_y_range=None, model_name="no_name.pt"):
  #Arrays to store training history
  test_loss_history = [[],[],[]]
  epoch_counter = []
  train_loss_history = [[],[],[]]
  last_loss = 999999
  best_loss = 999999
  best_epoch = 0
  epochsSinceLastImprove = 0
  
  iterable = range(num_epochs)
  if not should_output:
    iterable = tqdm(iterable, leave=False, desc="Training")
  for epoch in iterable:
    avg_loss = train(network, training_generator, loss_function, optimizer, should_output=should_output)
    test_loss = test(network, testing_generator, loss_function)
    scheduler.step(test_loss[0])
    logResults(epoch, num_epochs, avg_loss[0], test_loss[0], logging_interval, should_output)
    
    for n in range(3):
      train_loss_history[n].append(avg_loss[n])
      test_loss_history[n].append(test_loss[n])
    epoch_counter.append(epoch)
    
    if (test_loss[0] < best_loss):
      best_loss = test_loss[0]
      best_epoch = epoch
      torch.save(network.state_dict(), model_name)
      epochsSinceLastImprove = 0
    else:
      epochsSinceLastImprove += 1
      
    if epochsSinceLastImprove >= early_stopping_cutoff:
      if should_output:
        print("Stopped Training Early")
      break

  if should_output:
    print(f"Best result at epoch={best_epoch} with loss={best_loss}")

    graphLoss(epoch_counter, train_loss_history[0], test_loss_history[0], graph_y_range=graph_y_range, title="WAR")
    graphLoss(epoch_counter, train_loss_history[1], test_loss_history[1], title="Level")
    graphLoss(epoch_counter, train_loss_history[2], test_loss_history[2], title="PA")
  return best_loss